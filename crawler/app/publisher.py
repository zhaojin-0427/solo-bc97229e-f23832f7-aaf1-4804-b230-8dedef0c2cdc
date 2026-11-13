"""Background publication engine: immutable ZIP snapshots of a crawl job.

Consistency model
-----------------
Every publication is built from a *single committed snapshot*.  The worker
thread opens its own read-only SQLite connection and keeps ONE read transaction
open across every scan (job row, counts, papers, citations).  In WAL mode that
transaction pins one consistent view: rows committed later by the crawler
(retries, backfills, late fetches) are never mixed into the archive, while the
crawler's write transactions are never blocked -- a read transaction takes no
write lock and the scans run on a separate connection.

Streaming / bounded memory
--------------------------
Both tables are read with server-side cursors in fixed batches and streamed
straight into ZIP entries; nothing accumulates in Python, so peak memory does
not grow with the corpus size.

Crash safety
------------
The archive is written to a unique temp file, fsynced, atomically renamed into
place and only THEN registered as completed in one DB transaction.  Startup
recovery fails every unfinished publication and removes its remnants, while
completed packages (and only verified completed packages) are kept.
"""

import csv
import io
import json
import logging
import os
import sqlite3
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from .db import PUB_COMPLETED, Database

log = logging.getLogger("publisher")

BATCH_SIZE = 1000
MAX_WORKERS = 2
PAPERS_HEADER = ["url", "depth", "title", "status"]
CITATIONS_HEADER = ["src_url", "dst_url"]

# progress budget per stage (percent)
P_SNAPSHOT_DONE = 5
P_PAPERS_END = 55
P_CITATIONS_END = 95
P_META_END = 97


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def package_file_name(pub_id: int) -> str:
    return f"publication_{pub_id}.zip"


class PublicationManager:
    def __init__(self, db: Database, packages_dir: str):
        self.db = db
        self.packages_dir = packages_dir
        self._pool = ThreadPoolExecutor(
            max_workers=MAX_WORKERS, thread_name_prefix="publisher"
        )
        self._fs_lock = threading.Lock()  # serialize same-dir rename / recovery churn

    # ------------------------------------------------------------------ start

    def start(self):
        os.makedirs(self.packages_dir, exist_ok=True)
        self._recover()

    def shutdown(self):
        # In-flight builds are interrupted with the process; startup recovery
        # fails their rows and cleans their files, so shutdown is not blocked.
        self._pool.shutdown(wait=False, cancel_futures=True)

    def submit(self, pub_id: int):
        self._pool.submit(self._run_publication, pub_id)

    # --------------------------------------------------------------- recovery

    def _recover(self):
        """Restart rules:

        * publications that never finished -> failed, all their files removed;
        * completed publications -> kept and verified (a missing file turns the
          row failed rather than silently deleting anything);
        * temp files or unregistered final files left behind are swept, while
          files belonging to verified completed packages are never touched.
        """
        good_files: set[str] = set()
        for pub in self.db.list_completed_publications():
            name = pub.get("file_name") or package_file_name(pub["id"])
            if os.path.isfile(os.path.join(self.packages_dir, name)):
                good_files.add(name)
            else:
                self.db.mark_publication_failed(
                    pub["id"], "package file missing after restart"
                )

        for pub in self.db.list_unfinished_publications():
            pub_id = pub["id"]
            self.db.mark_publication_failed(pub_id, "interrupted by restart")
            self._remove_tmp_files(pub_id)
            final_path = os.path.join(self.packages_dir, package_file_name(pub_id))
            if os.path.isfile(final_path) and package_file_name(pub_id) not in good_files:
                self._unlink_quiet(final_path)

        for name in os.listdir(self.packages_dir):
            is_tmp = name.startswith(".publication_") and ".tmp." in name
            is_orphan_final = (
                name.startswith("publication_")
                and name.endswith(".zip")
                and name not in good_files
            )
            if is_tmp or is_orphan_final:
                self._unlink_quiet(os.path.join(self.packages_dir, name))

    def _remove_tmp_files(self, pub_id: int):
        prefix = f".publication_{pub_id}.tmp."
        for name in os.listdir(self.packages_dir):
            if name.startswith(prefix):
                self._unlink_quiet(os.path.join(self.packages_dir, name))

    @staticmethod
    def _unlink_quiet(path: str):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError:
            log.exception("could not remove %s", path)

    # ---------------------------------------------------------------- worker

    def _open_snapshot_connection(self) -> sqlite3.Connection:
        """Independent read-only connection; query_only makes the read-only
        intent explicit even if the URI flag were ignored."""
        from urllib.parse import quote

        conn = sqlite3.connect(
            f"file:{quote(self.db.path, safe='/')}?mode=ro",
            uri=True,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        # A deferred transaction on a read-only connection is a pure read
        # transaction. The first SELECT acquires (pins) one WAL snapshot that
        # every later scan in this transaction will read, even if the crawler
        # commits in between -- so the pinning read must happen here, before
        # the build does any work.
        conn.execute("BEGIN")
        conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        return conn

    def _run_publication(self, pub_id: int):
        tmp_path: str | None = None
        final_name = package_file_name(pub_id)
        final_path = os.path.join(self.packages_dir, final_name)
        conn: sqlite3.Connection | None = None
        try:
            self.db.mark_publication_running(pub_id)
            job_id = self.db.get_publication(pub_id)["job_id"]

            conn = self._open_snapshot_connection()
            # establish the snapshot before any count/scan uses it
            job_row = conn.execute(
                "SELECT id, seed_url, max_depth, status FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if job_row is None:
                raise LookupError(f"job {job_id} not found in snapshot")
            job = dict(job_row)

            papers_count = conn.execute(
                "SELECT COUNT(*) AS c FROM papers WHERE job_id=?", (job_id,)
            ).fetchone()["c"]
            citations_count = conn.execute(
                "SELECT COUNT(*) AS c FROM citations WHERE job_id=?", (job_id,)
            ).fetchone()["c"]
            self.db.update_publication_progress(pub_id, "snapshot", P_SNAPSHOT_DONE)

            tmp_name = f".publication_{pub_id}.tmp.{os.urandom(8).hex()}"
            tmp_path = os.path.join(self.packages_dir, tmp_name)
            with self._fs_lock:
                fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            try:
                with os.fdopen(fd, "wb") as raw:
                    with zipfile.ZipFile(raw, "w", zipfile.ZIP_DEFLATED) as zf:
                        papers_exported = self._write_papers_csv(
                            zf, conn, job_id, papers_count, pub_id
                        )
                        citations_exported = self._write_citations_csv(
                            zf, conn, job_id, citations_count, pub_id
                        )
                        self._write_meta(
                            zf, job, papers_count, citations_count
                        )
                    raw.flush()
                    os.fsync(raw.fileno())
                # durability of the directory entry itself
                self._fsync_dir()
                os.replace(tmp_path, final_path)  # atomic, same filesystem
                tmp_path = None
                self._fsync_dir()
                file_size = os.path.getsize(final_path)
            except BaseException:
                # make sure no half-written file is ever exposed under final name
                if os.path.exists(tmp_path):
                    self._unlink_quiet(tmp_path)
                    tmp_path = None
                raise

            # register success only after the complete file is durable
            self.db.mark_publication_complete(
                pub_id, final_name, file_size, papers_count, citations_count
            )
            log.info(
                "publication %s ready: %s (%d papers, %d citations, %d bytes)",
                pub_id, final_name, papers_count, citations_count, file_size,
            )
        except Exception as exc:  # publication boundary: report, never raise
            self._fail(pub_id, final_path, exc, conn)

    def _fail(self, pub_id: int, final_path: str, exc: Exception, conn):
        log.exception("publication %s failed", pub_id)
        if conn is not None:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        try:
            self.db.mark_publication_failed(
                pub_id, f"{type(exc).__name__}: {exc}"
            )
        except Exception:
            log.exception("could not mark publication %s failed", pub_id)
        # a registered successful package must never be removed by an error path
        try:
            row = self.db.get_publication(pub_id)
            if row and row["status"] != PUB_COMPLETED and os.path.isfile(final_path):
                self._unlink_quiet(final_path)
        except Exception:
            log.exception("could not clean final file for publication %s", pub_id)

    def _fsync_dir(self):
        try:
            dfd = os.open(self.packages_dir, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            log.debug("directory fsync unsupported", exc_info=True)

    # ------------------------------------------------------------- csv/meta

    def _write_papers_csv(self, zf, conn, job_id, total, pub_id) -> int:
        cursor = conn.execute(
            "SELECT url, depth, title, status FROM papers"
            " WHERE job_id=? ORDER BY depth, id",
            (job_id,),
        )
        exported = 0
        with zf.open("papers.csv", "w", force_zip64=True) as sink:
            writer = csv.writer(_TextToBytes(sink))
            writer.writerow(PAPERS_HEADER)
            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                writer.writerows(
                    (r["url"], r["depth"], r["title"] or "", r["status"])
                    for r in rows
                )
                exported += len(rows)
                if total:
                    progress = P_SNAPSHOT_DONE + int(
                        (P_PAPERS_END - P_SNAPSHOT_DONE) * exported / total
                    )
                else:
                    progress = P_PAPERS_END
                self.db.update_publication_progress(
                    pub_id, "papers", progress, papers_exported=exported
                )
        self.db.update_publication_progress(
            pub_id, "papers", P_PAPERS_END, papers_exported=exported
        )
        return exported

    def _write_citations_csv(self, zf, conn, job_id, total, pub_id) -> int:
        cursor = conn.execute(
            "SELECT src_url, dst_url FROM citations"
            " WHERE job_id=? ORDER BY src_url, dst_url",
            (job_id,),
        )
        exported = 0
        with zf.open("citations.csv", "w", force_zip64=True) as sink:
            writer = csv.writer(_TextToBytes(sink))
            writer.writerow(CITATIONS_HEADER)
            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                writer.writerows((r["src_url"], r["dst_url"]) for r in rows)
                exported += len(rows)
                if total:
                    progress = P_PAPERS_END + int(
                        (P_CITATIONS_END - P_PAPERS_END) * exported / total
                    )
                else:
                    progress = P_CITATIONS_END
                self.db.update_publication_progress(
                    pub_id, "citations", progress, citations_exported=exported
                )
        self.db.update_publication_progress(
            pub_id, "citations", P_CITATIONS_END, citations_exported=exported
        )
        return exported

    def _write_meta(self, zf, job: dict, papers_count: int, citations_count: int):
        meta = {
            "job_id": job["id"],
            "seed_url": job["seed_url"],
            "max_depth": job["max_depth"],
            "job_status": job["status"],
            "papers_rows": papers_count,
            "citations_rows": citations_count,
            "generated_at": _utcnow(),
            "files": {
                "papers.csv": PAPERS_HEADER,
                "citations.csv": CITATIONS_HEADER,
            },
        }
        data = json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True)
        with zf.open("meta.json", "w", force_zip64=True) as sink:
            sink.write(data.encode("utf-8"))


class _TextToBytes(io.TextIOBase):
    """Adapt a binary ZIP sink to the text-based csv writer without buffering
    whole rowsets: each line is encoded and forwarded immediately."""

    def __init__(self, sink):
        self._sink = sink

    def writable(self):
        return True

    def write(self, text):
        self._sink.write(text.encode("utf-8"))
        return len(text)

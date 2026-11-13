"""SQLite persistence for the citation crawler.

All crawl state (jobs, papers, citation edges, pending progress) lives in one
SQLite database.  Every state transition that involves several rows is done in
a single IMMEDIATE transaction, so a crash can never leave pages, citations
and queue progress out of sync.
"""

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import urlparse

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    seed_url    TEXT NOT NULL,
    origin      TEXT NOT NULL,
    max_depth   INTEGER NOT NULL CHECK (max_depth BETWEEN 0 AND 4),
    status      TEXT NOT NULL DEFAULT 'running',   -- running | completed | failed
    created_at  TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS papers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER NOT NULL REFERENCES jobs(id),
    url         TEXT NOT NULL,
    depth       INTEGER NOT NULL,                  -- shortest known distance from seed
    status      TEXT NOT NULL,                     -- pending | fetching | done | failed
                                                   -- | out_of_range | external | non_html
    title       TEXT,
    http_status INTEGER,
    error       TEXT,
    fetched_at  TEXT,
    UNIQUE (job_id, url)                           -- a URL is registered at most once per job
);
CREATE INDEX IF NOT EXISTS idx_papers_claim ON papers(job_id, status, depth, id);

CREATE TABLE IF NOT EXISTS citations (
    job_id  INTEGER NOT NULL,
    src_url TEXT NOT NULL,
    dst_url TEXT NOT NULL,
    PRIMARY KEY (job_id, src_url, dst_url)         -- an edge is stored at most once
);
"""

# paper statuses
PENDING = "pending"
FETCHING = "fetching"
DONE = "done"
FAILED = "failed"
OUT_OF_RANGE = "out_of_range"
EXTERNAL = "external"
NON_HTML = "non_html"

# job statuses
JOB_RUNNING = "running"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def same_origin(url: str, origin: str) -> bool:
    p = urlparse(url)
    return f"{p.scheme.lower()}://{p.netloc.lower()}" == origin


class Database:
    def __init__(self, path: str):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)

    @contextmanager
    def _tx(self):
        """One IMMEDIATE transaction: readers never see a partial crawl step."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # ------------------------------------------------------------------ jobs

    def create_job(self, seed_url: str, origin: str, max_depth: int) -> int:
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT INTO jobs(seed_url, origin, max_depth, status, created_at)"
                " VALUES (?,?,?,?,?)",
                (seed_url, origin, max_depth, JOB_RUNNING, utcnow()),
            )
            job_id = cur.lastrowid
            # the seed is the depth-0 page and the first pending work item
            conn.execute(
                "INSERT INTO papers(job_id, url, depth, status) VALUES (?,?,0,?)",
                (job_id, seed_url, PENDING),
            )
        return job_id

    def get_job(self, job_id: int):
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None

    def list_jobs(self):
        with self._lock:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
            return [dict(r) for r in rows]

    def job_counts(self, job_id: int) -> dict:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS c FROM papers WHERE job_id=? GROUP BY status",
                (job_id,),
            ).fetchall()
            counts = {r["status"]: r["c"] for r in rows}
            depths = self._conn.execute(
                "SELECT depth, COUNT(*) AS c FROM papers WHERE job_id=? GROUP BY depth ORDER BY depth",
                (job_id,),
            ).fetchall()
            return {
                "by_status": counts,
                "by_depth": {str(r["depth"]): r["c"] for r in depths},
            }

    def job_inflight(self, job_id: int) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) AS c FROM papers WHERE job_id=? AND status=?",
                (job_id, FETCHING),
            ).fetchone()["c"]

    # -------------------------------------------------------------- claiming

    def claim_next(self, job_id: int):
        """Atomically move the next pending page (breadth-first order) to fetching."""
        with self._tx() as conn:
            row = conn.execute(
                "SELECT id, url, depth FROM papers WHERE job_id=? AND status=?"
                " ORDER BY depth, id LIMIT 1",
                (job_id, PENDING),
            ).fetchone()
            if row is None:
                return None
            conn.execute("UPDATE papers SET status=? WHERE id=?", (FETCHING, row["id"]))
            return dict(row)

    # ------------------------------------------------------- record outcomes

    def record_success(self, job_id, paper_id, page_url, page_depth, title, links, http_status):
        """Persist page + citations + queue progress in ONE transaction.

        Depths always hold the shortest known distance from the seed.  When a
        late, shorter path reaches an already registered URL we lower its depth
        and propagate the decrease through already-fetched pages, so papers that
        newly fall inside max_depth are queued (backfilled) without refetching
        anything.
        """
        with self._tx() as conn:
            job = conn.execute("SELECT max_depth, origin FROM jobs WHERE id=?", (job_id,)).fetchone()
            max_depth, origin = job["max_depth"], job["origin"]

            conn.execute(
                "UPDATE papers SET status=?, title=?, http_status=?, error=NULL, fetched_at=?"
                " WHERE id=?",
                (DONE, title, http_status, utcnow(), paper_id),
            )

            stack = []  # (url, new_depth) of done pages whose children need re-checking
            for dst in links:
                conn.execute(
                    "INSERT OR IGNORE INTO citations(job_id, src_url, dst_url) VALUES (?,?,?)",
                    (job_id, page_url, dst),
                )
                internal = same_origin(dst, origin)
                row = conn.execute(
                    "SELECT id, url, depth, status FROM papers WHERE job_id=? AND url=?",
                    (job_id, dst),
                ).fetchone()
                new_depth = page_depth + 1
                if row is None:
                    if not internal:
                        status = EXTERNAL
                    elif new_depth <= max_depth:
                        status = PENDING
                    else:
                        status = OUT_OF_RANGE
                    conn.execute(
                        "INSERT INTO papers(job_id, url, depth, status) VALUES (?,?,?,?)",
                        (job_id, dst, new_depth, status),
                    )
                elif internal and new_depth < row["depth"]:
                    # late shorter path -> lower depth, maybe backfill
                    self._lower_depth(conn, row, new_depth, max_depth, stack)

            self._propagate(conn, job_id, max_depth, origin, stack)
            self._finalize_if_idle(conn, job_id)

    def record_failure(self, job_id, paper_id, http_status, error):
        """Keep everything collected so far; only this page is marked failed."""
        with self._tx() as conn:
            conn.execute(
                "UPDATE papers SET status=?, http_status=?, error=?, fetched_at=? WHERE id=?",
                (FAILED, http_status, error, utcnow(), paper_id),
            )
            self._finalize_if_idle(conn, job_id)

    def record_non_html(self, job_id, paper_id, http_status, content_type):
        with self._tx() as conn:
            conn.execute(
                "UPDATE papers SET status=?, http_status=?, error=?, fetched_at=? WHERE id=?",
                (NON_HTML, http_status, f"content-type: {content_type}", utcnow(), paper_id),
            )
            self._finalize_if_idle(conn, job_id)

    # ----------------------------------------------------- depth propagation

    def _lower_depth(self, conn, row, new_depth, max_depth, stack):
        conn.execute("UPDATE papers SET depth=? WHERE id=?", (new_depth, row["id"]))
        if row["status"] == OUT_OF_RANGE and new_depth <= max_depth:
            # newly inside the depth budget -> backfill it into the queue
            conn.execute("UPDATE papers SET status=? WHERE id=?", (PENDING, row["id"]))
        elif row["status"] == DONE:
            # already fetched: do NOT refetch, just re-check its children
            stack.append((row["url"], new_depth))

    def _propagate(self, conn, job_id, max_depth, origin, stack):
        """Push depth decreases through stored citation edges (cycle-safe: every
        accepted decrease is strict and depths are bounded below by 0)."""
        while stack:
            url, depth = stack.pop()
            edges = conn.execute(
                "SELECT dst_url FROM citations WHERE job_id=? AND src_url=?",
                (job_id, url),
            ).fetchall()
            for edge in edges:
                dst = edge["dst_url"]
                if not same_origin(dst, origin):
                    continue
                row = conn.execute(
                    "SELECT id, url, depth, status FROM papers WHERE job_id=? AND url=?",
                    (job_id, dst),
                ).fetchone()
                if row is not None and depth + 1 < row["depth"]:
                    self._lower_depth(conn, row, depth + 1, max_depth, stack)

    # ------------------------------------------------------------ finalizing

    def _finalize_if_idle(self, conn, job_id):
        """A job settles only when no pending/fetching work remains; any failed
        page means the job must NOT report completion."""
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM papers WHERE job_id=? AND status IN (?,?)",
            (job_id, PENDING, FETCHING),
        ).fetchone()
        if row["c"] == 0:
            failed = conn.execute(
                "SELECT COUNT(*) AS c FROM papers WHERE job_id=? AND status=?",
                (job_id, FAILED),
            ).fetchone()["c"]
            status = JOB_FAILED if failed else JOB_COMPLETED
            conn.execute(
                "UPDATE jobs SET status=?, finished_at=? WHERE id=? AND status=?",
                (status, utcnow(), job_id, JOB_RUNNING),
            )

    # ----------------------------------------------------------------- retry

    def retry_job(self, job_id: int):
        """Re-queue failed pages so the crawl can resume where it stopped."""
        with self._tx() as conn:
            job = conn.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                return None
            cur = conn.execute(
                "UPDATE papers SET status=?, error=NULL WHERE job_id=? AND status=?",
                (PENDING, job_id, FAILED),
            )
            requeued = cur.rowcount
            if requeued:
                conn.execute(
                    "UPDATE jobs SET status=?, finished_at=NULL WHERE id=?",
                    (JOB_RUNNING, job_id),
                )
            return {"requeued": requeued, "previous_status": job["status"]}

    def recover(self):
        """After a restart: pages that were in-flight go back to pending, and
        every still-running job gets its workers again (nothing lost, nothing
        duplicated thanks to the UNIQUE constraints)."""
        with self._tx() as conn:
            conn.execute(
                "UPDATE papers SET status=? WHERE status=? AND job_id IN"
                " (SELECT id FROM jobs WHERE status=?)",
                (PENDING, FETCHING, JOB_RUNNING),
            )
            rows = conn.execute(
                "SELECT id FROM jobs WHERE status=?", (JOB_RUNNING,)
            ).fetchall()
            return [r["id"] for r in rows]

    # ---------------------------------------------------------------- queries

    def list_papers(self, job_id: int, status=None, depth=None):
        sql = "SELECT * FROM papers WHERE job_id=?"
        args = [job_id]
        if status:
            sql += " AND status=?"
            args.append(status)
        if depth is not None:
            sql += " AND depth=?"
            args.append(depth)
        sql += " ORDER BY depth, id"
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    def list_citations(self, job_id: int, src=None):
        sql = "SELECT src_url, dst_url FROM citations WHERE job_id=?"
        args = [job_id]
        if src:
            sql += " AND src_url=?"
            args.append(src)
        sql += " ORDER BY src_url, dst_url"
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    def list_failures(self, job_id: int):
        with self._lock:
            return [
                dict(r)
                for r in self._conn.execute(
                    "SELECT url, depth, http_status, error, fetched_at FROM papers"
                    " WHERE job_id=? AND status=? ORDER BY id",
                    (job_id, FAILED),
                ).fetchall()
            ]

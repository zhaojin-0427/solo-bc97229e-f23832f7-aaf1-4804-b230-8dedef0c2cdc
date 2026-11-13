"""Integration tests for the publication API and snapshot engine.

Run with:
    .venv/bin/python -m pytest tests/ -v
(or plain unittest: .venv/bin/python -m unittest discover -s tests -v)

Each test gets its own temp data directory and a freshly imported application,
so DATABASE_PATH / PACKAGES_DIR are isolated per test.
"""

import csv
import importlib
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import zipfile

APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "crawler"))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)


def load_app(tmpdir: str):
    os.environ["DATABASE_PATH"] = os.path.join(tmpdir, "crawler.db")
    os.environ["PACKAGES_DIR"] = os.path.join(tmpdir, "packages")
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    main = importlib.import_module("app.main")
    importlib.reload(main)
    from fastapi.testclient import TestClient

    main.TestClient = TestClient
    return main


def seed_corpus(db, job_id=1, extra=0):
    """Insert a deterministic corpus for the given (already created) job."""
    papers = [
        ("http://site/p0.html", 0, "done", "Paper 0"),
        ("http://site/p1.html", 1, "done", "Paper 1"),
        ("http://site/p2.html", 2, "failed", None),
        ("http://site/ext.html", 1, "external", None),
        ("http://site/deck.pdf", 1, "non_html", None),
    ]
    for url, depth, status, title in papers:
        with db._tx() as conn:
            conn.execute(
                "INSERT INTO papers(job_id, url, depth, status, title)"
                " VALUES (?,?,?,?,?)"
                " ON CONFLICT(job_id, url) DO UPDATE SET"
                " depth=excluded.depth, status=excluded.status,"
                " title=excluded.title",
                (job_id, url, depth, status, title),
            )
    edges = [
        ("http://site/p0.html", "http://site/p1.html"),
        ("http://site/p0.html", "http://site/ext.html"),
        ("http://site/p1.html", "http://site/p2.html"),
        ("http://site/p1.html", "http://site/deck.pdf"),
    ]
    with db._tx() as conn:
        conn.executemany(
            "INSERT INTO citations(job_id, src_url, dst_url) VALUES (?,?,?)",
            [(job_id, s, d) for s, d in edges],
        )
    for i in range(extra):
        with db._tx() as conn:
            conn.execute(
                "INSERT INTO papers(job_id, url, depth, status, title)"
                " VALUES (?,?,?,?,?)",
                (job_id, f"http://site/bulk{i}.html", 3, "done", f"Bulk {i}"),
            )
    return len(papers) + extra, len(edges)


def wait_status(main, pub_id, target, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        pub = main.db.get_publication(pub_id)
        if pub["status"] in (target, "failed"):
            return pub
        time.sleep(0.02)
    raise AssertionError(f"publication {pub_id} did not reach {target}")


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tmpdir = self.tmp.name
        self.main = load_app(self.tmpdir)
        self.client_ctx = self.main.TestClient(self.main.app)
        self.client = self.client_ctx.__enter__()

    def tearDown(self):
        self.client_ctx.__exit__(None, None, None)
        self.tmp.cleanup()

    # ------------------------------------------------------------ 1. contents

    def test_zip_contents_and_meta_match_database(self):
        from app.db import JOB_RUNNING

        job_id = self.main.db.create_job("http://site/p0.html", "http://site", 4)
        n_papers, n_edges = seed_corpus(self.main.db, job_id)

        resp = self.client.post(f"/jobs/{job_id}/publications")
        self.assertEqual(resp.status_code, 201)
        pub_id = resp.json()["id"]
        pub = wait_status(self.main, pub_id, "completed")
        self.assertEqual(pub["status"], "completed")
        self.assertEqual(pub["papers_count"], n_papers)
        self.assertEqual(pub["citations_count"], n_edges)
        self.assertEqual(pub["progress"], 100)
        self.assertTrue(pub["file_name"].endswith(".zip"))
        self.assertGreater(pub["file_size"], 0)

        dl = self.client.get(f"/publications/{pub_id}/download")
        self.assertEqual(dl.status_code, 200)
        self.assertEqual(dl.headers["content-type"], "application/zip")
        zf = zipfile.ZipFile(io.BytesIO(dl.content))
        self.assertEqual(sorted(zf.namelist()),
                         ["citations.csv", "meta.json", "papers.csv"])

        papers = list(csv.DictReader(io.StringIO(
            zf.read("papers.csv").decode())))
        self.assertEqual(len(papers), n_papers)
        self.assertEqual(list(papers[0].keys()),
                         ["url", "depth", "title", "status"])
        p2 = next(r for r in papers if r["url"].endswith("p2.html"))
        self.assertEqual(p2["status"], "failed")
        self.assertEqual(p2["depth"], "2")
        self.assertEqual(p2["title"], "")  # failed page has no title

        cites = list(csv.DictReader(io.StringIO(
            zf.read("citations.csv").decode())))
        self.assertEqual(len(cites), n_edges)
        self.assertEqual(list(cites[0].keys()), ["src_url", "dst_url"])

        meta = json.loads(zf.read("meta.json").decode())
        self.assertEqual(meta["job_id"], job_id)
        self.assertEqual(meta["seed_url"], "http://site/p0.html")
        self.assertEqual(meta["max_depth"], 4)
        self.assertEqual(meta["job_status"], JOB_RUNNING)
        self.assertEqual(meta["papers_rows"], n_papers)
        self.assertEqual(meta["citations_rows"], n_edges)

        # the row counts in meta.json must equal the actual CSV row counts
        self.assertEqual(meta["papers_rows"], len(papers))
        self.assertEqual(meta["citations_rows"], len(cites))

    # ---------------------------------------------------- 2. snapshot isolation

    def test_snapshot_excludes_later_crawl_writes(self):
        job_id = self.main.db.create_job("http://site/p0.html", "http://site", 4)
        seed_corpus(self.main.db, job_id)
        base_papers, base_edges = 5, 4

        snapshot_ready = threading.Event()
        release = threading.Event()
        original_open = self.main.publisher._open_snapshot_connection

        def gated_open():
            conn = original_open()
            # snapshot is now pinned; let the test commit new crawl progress,
            # which the still-open read transaction must NOT see
            snapshot_ready.set()
            self.assertTrue(release.wait(timeout=10))
            return conn

        self.main.publisher._open_snapshot_connection = gated_open
        try:
            resp = self.client.post(f"/jobs/{job_id}/publications")
            pub_id = resp.json()["id"]
            self.assertTrue(snapshot_ready.wait(timeout=10))

            # simulate a retry/backfill committing AFTER the snapshot began
            with self.main.db._tx() as conn:
                conn.execute(
                    "INSERT INTO papers(job_id, url, depth, status, title)"
                    " VALUES (?,?,?,?,?)",
                    (job_id, "http://site/late.html", 2, "done", "Late Paper"),
                )
                conn.execute(
                    "INSERT INTO citations(job_id, src_url, dst_url)"
                    " VALUES (?,?,?)",
                    (job_id, "http://site/p0.html", "http://site/late.html"),
                )
            release.set()
            pub = wait_status(self.main, pub_id, "completed")
        finally:
            self.main.publisher._open_snapshot_connection = original_open

        zf = zipfile.ZipFile(io.BytesIO(
            self.client.get(f"/publications/{pub_id}/download").content))
        papers = list(csv.DictReader(io.StringIO(
            zf.read("papers.csv").decode())))
        cites = list(csv.DictReader(io.StringIO(
            zf.read("citations.csv").decode())))
        urls = {r["url"] for r in papers}
        self.assertNotIn("http://site/late.html", urls)
        self.assertEqual(len(papers), base_papers)
        self.assertEqual(len(cites), base_edges)
        meta = json.loads(zf.read("meta.json").decode())
        self.assertEqual(meta["papers_rows"], base_papers)

        # the live database itself does contain the later rows
        self.assertEqual(
            self.main.db.job_counts(job_id)["by_status"].get("done", 0), 3)

    # ----------------------------------------------------------- 3. downloads

    def test_repeated_downloads_are_identical_and_guards_apply(self):
        job_id = self.main.db.create_job("http://site/p0.html", "http://site", 4)
        seed_corpus(self.main.db, job_id)
        pub_id = self.client.post(
            f"/jobs/{job_id}/publications").json()["id"]
        wait_status(self.main, pub_id, "completed")

        first = self.client.get(f"/publications/{pub_id}/download").content
        second = self.client.get(f"/publications/{pub_id}/download").content
        self.assertEqual(first, second)  # immutable package

        # download of an unknown package -> 404
        self.assertEqual(self.client.get("/publications/999/download").status_code, 404)
        # unknown publication detail -> 404
        self.assertEqual(self.client.get("/publications/999").status_code, 404)
        # publishing for an unknown job -> 404
        self.assertEqual(
            self.client.post("/jobs/999/publications").status_code, 404)

        # failed publication cannot be downloaded and exposes its error
        bad_id = self.main.db.create_publication(job_id)
        self.main.db.mark_publication_failed(bad_id, "boom: disk on fire")
        resp = self.client.get(f"/publications/{bad_id}/download")
        self.assertEqual(resp.status_code, 409)
        self.assertIn("failed", resp.json()["detail"])
        detail = self.client.get(f"/publications/{bad_id}").json()
        self.assertEqual(detail["status"], "failed")
        self.assertIn("disk on fire", detail["error"])
        self.assertNotIn("download_url", detail)

        # listing endpoints
        listing = self.client.get("/publications").json()
        self.assertEqual({p["id"] for p in listing}, {pub_id, bad_id})
        job_listing = self.client.get(
            f"/publications", params={"job_id": job_id}).json()
        self.assertTrue(all(p["job_id"] == job_id for p in job_listing))
        completed = next(p for p in listing if p["id"] == pub_id)
        self.assertEqual(completed["download_url"],
                         f"/publications/{pub_id}/download")

    # ----------------------------------------------------------- 4. streaming

    def test_streaming_export_keeps_memory_flat(self):
        import tracemalloc

        job_id = self.main.db.create_job("http://site/p0.html", "http://site", 4)
        n_papers, n_edges = seed_corpus(self.main.db, job_id, extra=8000)
        # add plenty of edges as well to exercise both cursors
        with self.main.db._tx() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO citations(job_id, src_url, dst_url)"
                " VALUES (?,?,?)",
                [(job_id, f"http://site/bulk{i}.html", "http://site/p0.html")
                 for i in range(8000)],
            )

        tracemalloc.start()
        snap_before = tracemalloc.take_snapshot()
        pub_id = self.client.post(
            f"/jobs/{job_id}/publications").json()["id"]
        pub = wait_status(self.main, pub_id, "completed", timeout=60)
        snap_after = tracemalloc.take_snapshot()
        stats = snap_after.compare_to(snap_before, "lineno")
        growth = sum(s.size_diff for s in stats if s.size_diff > 0)
        tracemalloc.stop()

        self.assertEqual(pub["papers_count"], n_papers)
        self.assertEqual(pub["citations_count"], n_edges + 8000)
        # 16k+ rows streamed: the worker must not retain them (allow generous
        # headroom for interpreter/zip buffers, but far below the corpus size)
        self.assertLess(growth, 4 * 1024 * 1024,
                        f"memory grew {growth} bytes while streaming")

        zf = zipfile.ZipFile(io.BytesIO(
            self.client.get(f"/publications/{pub_id}/download").content))
        papers = list(csv.reader(io.StringIO(
            zf.read("papers.csv").decode())))
        cites = list(csv.reader(io.StringIO(
            zf.read("citations.csv").decode())))
        self.assertEqual(len(papers) - 1, n_papers)
        self.assertEqual(len(cites) - 1, n_edges + 8000)

    # ------------------------------------------------------------ 5. recovery

    def test_restart_recovery_keeps_packages_and_sweeps_remnants(self):
        job_id = self.main.db.create_job("http://site/p0.html", "http://site", 4)
        seed_corpus(self.main.db, job_id)
        good_id = self.client.post(
            f"/jobs/{job_id}/publications").json()["id"]
        wait_status(self.main, good_id, "completed")
        good_file = self.main.db.get_publication(good_id)["file_name"]
        good_path = os.path.join(self.tmpdir, "packages", good_file)
        with open(good_path, "rb") as fh:
            good_bytes = fh.read()
        self.assertTrue(os.path.exists(good_path))

        # an interrupted build: running row + temp file + orphan final file
        stuck_id = self.main.db.create_publication(job_id)
        self.main.db.mark_publication_running(stuck_id)
        tmp = os.path.join(
            self.tmpdir, "packages",
            f".publication_{stuck_id}.tmp.deadbeef")
        with open(tmp, "wb") as fh:
            fh.write(b"partial")
        orphan_final = os.path.join(
            self.tmpdir, "packages", f"publication_{stuck_id}.zip")
        with open(orphan_final, "wb") as fh:
            fh.write(b"unregistered")
        # also an unknown temp file from a row that never made it to the DB
        with open(os.path.join(self.tmpdir, "packages",
                               ".publication_777.tmp.aaaa"), "wb") as fh:
            fh.write(b"x")

        # simulate process restart: shut down and rebuild managers on same dir
        self.client_ctx.__exit__(None, None, None)
        fresh = load_app(self.tmpdir)
        with fresh.TestClient(fresh.app) as client2:
            files = set(os.listdir(os.path.join(self.tmpdir, "packages")))
            # completed package survives byte-for-byte
            self.assertIn(good_file, files)
            with open(good_path, "rb") as fh:
                self.assertEqual(fh.read(), good_bytes)
            # every remnant is gone
            self.assertNotIn(os.path.basename(tmp), files)
            self.assertNotIn(os.path.basename(orphan_final), files)
            self.assertNotIn(".publication_777.tmp.aaaa", files)

            stuck = fresh.db.get_publication(stuck_id)
            self.assertEqual(stuck["status"], "failed")
            self.assertIn("restart", stuck["error"])
            good = fresh.db.get_publication(good_id)
            self.assertEqual(good["status"], "completed")
            # surviving package still downloadable after restart
            dl = client2.get(f"/publications/{good_id}/download")
            self.assertEqual(dl.status_code, 200)
            self.assertEqual(dl.content, good_bytes)

    def test_completed_row_with_missing_file_is_failed_not_deleted_silently(self):
        job_id = self.main.db.create_job("http://site/p0.html", "http://site", 4)
        seed_corpus(self.main.db, job_id)
        good_id = self.client.post(
            f"/jobs/{job_id}/publications").json()["id"]
        wait_status(self.main, good_id, "completed")
        # external storage fault: file vanished
        os.unlink(os.path.join(
            self.tmpdir, "packages",
            self.main.db.get_publication(good_id)["file_name"]))

        self.client_ctx.__exit__(None, None, None)
        fresh = load_app(self.tmpdir)
        with fresh.TestClient(fresh.app):
            row = fresh.db.get_publication(good_id)
            self.assertEqual(row["status"], "failed")
            self.assertIn("missing after restart", row["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

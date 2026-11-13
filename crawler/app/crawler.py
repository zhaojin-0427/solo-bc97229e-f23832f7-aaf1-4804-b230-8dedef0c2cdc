"""Async crawl engine: exactly two fetch slots per job.

Each worker independently claims the next pending page, so a slow page never
blocks its level: as soon as a slot frees and pending work exists, another
request is issued.
"""

import asyncio
import logging
from urllib.parse import urldefrag, urljoin, urlparse

import httpx

from .db import Database
from .parser import parse_html

log = logging.getLogger("crawler")

SLOTS_PER_JOB = 2
IDLE_POLL_SECONDS = 0.05


class CrawlManager:
    def __init__(self, db: Database):
        self.db = db
        self._client: httpx.AsyncClient | None = None
        self._workers: dict[int, list[asyncio.Task]] = {}

    async def start(self):
        # follow_redirects=False: redirected URLs are recorded as failures,
        # only direct 200/text-html pages are collected.
        self._client = httpx.AsyncClient(
            follow_redirects=False,
            timeout=httpx.Timeout(15.0, connect=10.0),
            headers={"User-Agent": "citation-crawler/1.0"},
        )

    async def stop(self):
        tasks = [t for tasks in self._workers.values() for t in tasks]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._client:
            await self._client.aclose()

    def ensure_workers(self, job_id: int):
        """Start the job's two slots unless they are already running."""
        tasks = self._workers.get(job_id)
        if tasks and any(not t.done() for t in tasks):
            return
        self._workers[job_id] = [
            asyncio.create_task(self._worker(job_id, slot)) for slot in range(SLOTS_PER_JOB)
        ]

    async def _worker(self, job_id: int, slot: int):
        while True:
            paper = self.db.claim_next(job_id)
            if paper is None:
                if self.db.job_inflight(job_id) == 0:
                    return  # nothing pending and nothing in flight: job settled
                # siblings are still fetching and may produce new pending pages
                await asyncio.sleep(IDLE_POLL_SECONDS)
                continue
            try:
                await self._fetch_and_record(job_id, paper)
            except Exception:  # never let one page kill the slot
                log.exception("worker error job=%s url=%s", job_id, paper["url"])
                try:
                    self.db.record_failure(job_id, paper["id"], None, "internal crawler error")
                except Exception:
                    log.exception("could not record failure for %s", paper["url"])

    async def _fetch_and_record(self, job_id: int, paper: dict):
        url, pid, depth = paper["url"], paper["id"], paper["depth"]
        try:
            resp = await self._client.get(url)
        except httpx.HTTPError as exc:
            self.db.record_failure(job_id, pid, None, f"request error: {type(exc).__name__}: {exc}")
            return

        status = resp.status_code
        if 300 <= status < 400:
            location = resp.headers.get("location", "")
            self.db.record_failure(job_id, pid, status, f"redirect not followed (Location: {location})")
            return
        if status != 200:
            self.db.record_failure(job_id, pid, status, f"HTTP {status}")
            return
        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type.lower():
            self.db.record_non_html(job_id, pid, status, content_type or "unknown")
            return

        title, hrefs = parse_html(resp.text)
        links, seen = [], set()
        for href in hrefs:
            # resolve against the current page, then drop the fragment
            absolute = urldefrag(urljoin(url, href))[0]
            if urlparse(absolute).scheme.lower() not in ("http", "https"):
                continue  # e.g. mailto: links are not crawlable citations
            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)
        self.db.record_success(job_id, pid, url, depth, title, links, status)

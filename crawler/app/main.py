"""FastAPI surface: submit jobs, watch status, inspect failures, query the
collected papers and citations, retry failed pages.

Interactive API docs (Swagger UI) are served at /docs.
"""

import logging
import os
from contextlib import asynccontextmanager
from urllib.parse import urldefrag, urlparse

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .crawler import CrawlManager
from .db import JOB_RUNNING, Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

DB_PATH = os.environ.get("DATABASE_PATH", "/data/crawler.db")
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

db = Database(DB_PATH)
manager = CrawlManager(db)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.start()
    # auto-resume every job that was interrupted mid-crawl
    for job_id in db.recover():
        logging.getLogger("crawler").info("resuming interrupted job %s", job_id)
        manager.ensure_workers(job_id)
    yield
    await manager.stop()


app = FastAPI(
    title="Citation Corpus Collector",
    version="1.0.0",
    description=(
        "Collects a citation corpus from a static paper site. Submit a seed URL "
        "and a max depth (0-4, seed = depth 0); the crawler fetches same-origin, "
        "redirect-free static HTML pages with two slots per job, persists pages, "
        "citations and pending progress in SQLite, resumes after restarts and "
        "never reports completion while failures remain."
    ),
    lifespan=lifespan,
)


class JobCreate(BaseModel):
    seed_url: str = Field(..., examples=["http://papersite:8080/papers/p0.html"])
    max_depth: int = Field(..., ge=0, le=4, examples=[4])


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.post("/jobs", status_code=201, summary="Create a crawl job")
async def create_job(body: JobCreate):
    seed_url = urldefrag(body.seed_url.strip())[0]
    parsed = urlparse(seed_url)
    if parsed.scheme.lower() not in ("http", "https") or not parsed.netloc:
        raise HTTPException(422, "seed_url must be an absolute http(s) URL")
    origin = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
    job_id = db.create_job(seed_url, origin, body.max_depth)
    manager.ensure_workers(job_id)
    return _job_payload(job_id)


@app.get("/jobs", summary="List all jobs")
async def list_jobs():
    return [_job_payload(j["id"]) for j in db.list_jobs()]


@app.get("/jobs/{job_id}", summary="Job status and progress")
async def job_detail(job_id: int):
    return _job_payload(job_id)


@app.post("/jobs/{job_id}/retry", summary="Re-queue failed pages and resume the job")
async def retry_job(job_id: int):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    if job["status"] == JOB_RUNNING:
        raise HTTPException(409, "job is still running")
    result = db.retry_job(job_id)
    if result["requeued"]:
        manager.ensure_workers(job_id)
    return {"job_id": job_id, **result, "job": _job_payload(job_id)}


@app.get("/jobs/{job_id}/failures", summary="Failed URLs of a job")
async def job_failures(job_id: int):
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    return {"job_id": job_id, "failures": db.list_failures(job_id)}


@app.get("/jobs/{job_id}/papers", summary="Papers registered by a job")
async def job_papers(job_id: int, status: str | None = None, depth: int | None = None):
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    return {"job_id": job_id, "papers": db.list_papers(job_id, status=status, depth=depth)}


@app.get("/jobs/{job_id}/citations", summary="Citation edges collected by a job")
async def job_citations(job_id: int, src: str | None = None):
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    return {"job_id": job_id, "citations": db.list_citations(job_id, src=src)}


def _job_payload(job_id: int) -> dict:
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    counts = db.job_counts(job_id)
    return {**job, "counts": counts["by_status"], "papers_by_depth": counts["by_depth"]}

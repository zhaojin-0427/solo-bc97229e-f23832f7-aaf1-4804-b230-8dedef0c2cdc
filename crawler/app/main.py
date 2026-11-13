"""FastAPI surface: submit jobs, watch status, inspect failures, query the
collected papers and citations, retry failed pages, and publish immutable ZIP
snapshots of a job's corpus for the offline analysis pipeline.

Interactive API docs (Swagger UI) are served at /docs.
"""

import logging
import os
from contextlib import asynccontextmanager
from urllib.parse import urldefrag, urlparse

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field

from .crawler import CrawlManager
from .db import PUB_COMPLETED, Database
from .publisher import PublicationManager, package_file_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

DB_PATH = os.environ.get("DATABASE_PATH", "/data/crawler.db")
PACKAGES_DIR = os.environ.get("PACKAGES_DIR", "/data/packages")
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
os.makedirs(PACKAGES_DIR, exist_ok=True)

db = Database(DB_PATH)
manager = CrawlManager(db)
publisher = PublicationManager(db, PACKAGES_DIR)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # recover publications first: interrupted builds are failed and swept,
    # verified packages kept -- before any new build can touch the directory
    publisher.start()
    await manager.start()
    # auto-resume every job that was interrupted mid-crawl
    for job_id in db.recover():
        logging.getLogger("crawler").info("resuming interrupted job %s", job_id)
        manager.ensure_workers(job_id)
    yield
    publisher.shutdown()
    await manager.stop()


app = FastAPI(
    title="Citation Corpus Collector",
    version="1.1.0",
    description=(
        "Collects a citation corpus from a static paper site. Submit a seed URL "
        "and a max depth (0-4, seed = depth 0); the crawler fetches same-origin, "
        "redirect-free static HTML pages with two slots per job, persists pages, "
        "citations and pending progress in SQLite, resumes after restarts and "
        "never reports completion while failures remain.\n\n"
        "Publish any job (running or finished) to build, in the background, an "
        "immutable ZIP snapshot of one committed database state: papers.csv, "
        "citations.csv and meta.json. Re-downloading a package always returns "
        "the identical bytes."
    ),
    lifespan=lifespan,
)


class JobCreate(BaseModel):
    seed_url: str = Field(..., examples=["http://papersite:8080/papers/p0.html"])
    max_depth: int = Field(..., ge=0, le=4, examples=[4])


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}


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
    if job["status"] == "running":
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


# ------------------------------------------------------------ publications


@app.post(
    "/jobs/{job_id}/publications",
    status_code=201,
    summary="Publish an immutable ZIP snapshot of the job's corpus",
    description=(
        "The package is built in the background from one committed database "
        "state; crawling, retries and queries keep working while it runs. The "
        "ZIP contains papers.csv (url, depth, title, status), citations.csv "
        "(src_url, dst_url) and meta.json (job id, seed, max depth, job status "
        "and both row counts)."
    ),
)
async def create_publication(job_id: int):
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    pub_id = db.create_publication(job_id)
    publisher.submit(pub_id)
    return _publication_payload(pub_id)


@app.get("/publications", summary="List publication packages (optionally by job)")
async def list_publications(job_id: int | None = Query(default=None)):
    if job_id is not None and db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    return [_publication_payload(p["id"]) for p in db.list_publications(job_id)]


@app.get(
    "/publications/{pub_id}",
    summary="Publication progress, row counts and error details",
)
async def publication_detail(pub_id: int):
    return _publication_payload(pub_id)


@app.get(
    "/publications/{pub_id}/download",
    summary="Download the immutable ZIP (repeatable, identical bytes)",
)
async def publication_download(pub_id: int):
    pub = db.get_publication(pub_id)
    if pub is None:
        raise HTTPException(404, "publication not found")
    if pub["status"] != PUB_COMPLETED:
        raise HTTPException(
            409,
            f"publication {pub_id} is {pub['status']} (stage={pub['stage']}, "
            f"progress={pub['progress']}%)"
            + (f"; error: {pub['error']}" if pub["error"] else ""),
        )
    file_name = pub["file_name"] or package_file_name(pub_id)
    path = os.path.join(PACKAGES_DIR, file_name)
    if not os.path.isfile(path):
        # defensive: a completed row without its file is a storage fault
        raise HTTPException(409, "package file is unavailable on this server")
    return FileResponse(
        path,
        media_type="application/zip",
        filename=file_name,
        headers={"Cache-Control": "private, max-age=0, immutable"},
    )


# ----------------------------------------------------------------- payloads


def _job_payload(job_id: int) -> dict:
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    counts = db.job_counts(job_id)
    return {**job, "counts": counts["by_status"], "papers_by_depth": counts["by_depth"]}


def _publication_payload(pub_id: int) -> dict:
    pub = db.get_publication(pub_id)
    if pub is None:
        raise HTTPException(404, "publication not found")
    payload = dict(pub)
    if pub["status"] == PUB_COMPLETED:
        payload["download_url"] = f"/publications/{pub_id}/download"
    return payload

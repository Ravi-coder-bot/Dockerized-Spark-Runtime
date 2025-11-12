import json
import os
import subprocess
import time
from datetime import datetime
from typing import Dict, Union, Optional
from threading import Thread

from fastapi import FastAPI, Request, HTTPException, status, Depends
from fastapi.responses import JSONResponse, FileResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware

# Local imports (direct)
from jobs_db import JOBS_DB, Job, JobStatus, get_job_output_path, get_job_log_path
from utils import (
    API_KEY, JOBS_PATH, OUTPUTS_PATH, LOGS_PATH,
    execute_pyspark_job, check_job_status, read_log_preview, read_full_log
)

# -------------------------------------------------------------------
# FASTAPI APP CONFIG
# -------------------------------------------------------------------
app = FastAPI(title="PySpark Job Orchestrator API")

# Enable CORS (for frontend)
origins = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------------------------
# API Key Security (Now works in Swagger UI)
# -------------------------------------------------------------------
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def verify_api_key(x_api_key: str = Depends(api_key_header)):
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key."
        )

# -------------------------------------------------------------------
# MODELS
# -------------------------------------------------------------------
class SubmitJobRequest(BaseModel):
    job_name: str
    job_type: str  # "prebuilt" or "custom"
    code: Optional[str] = None
    params: Optional[dict] = {}
    input_paths: Dict[str, str]
    output_path: str
    timeout_seconds: int = 600

class SubmitJobResponse(BaseModel):
    job_id: str
    status_url: str

class StatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    exit_code: Optional[int]
    result_url: Optional[str] = None
    log_preview: Optional[str] = None

# -------------------------------------------------------------------
# JOB MONITOR THREAD
# -------------------------------------------------------------------
RUNNING_PROCESSES: Dict[str, subprocess.Popen] = {}

def monitor_jobs():
    """Background thread to check job status every 1s."""
    while True:
        jobs_to_check = list(RUNNING_PROCESSES.keys())
        for job_id in jobs_to_check:
            process = RUNNING_PROCESSES.get(job_id)
            job = JOBS_DB.get(job_id)

            if not process or not job:
                RUNNING_PROCESSES.pop(job_id, None)
                continue

            new_status, exit_code = check_job_status(job_id, process, job.timeout_seconds)
            if new_status != JobStatus.RUNNING:
                job.status = new_status
                job.finished_at = datetime.now()
                job.exit_code = exit_code
                RUNNING_PROCESSES.pop(job_id, None)

        time.sleep(1)

@app.on_event("startup")
def startup_event():
    if API_KEY:
        print("✅ API Key loaded, starting job monitor thread.")
        Thread(target=monitor_jobs, daemon=True).start()
    else:
        print("⚠️  API_KEY not set! Jobs won't be monitored properly.")

# -------------------------------------------------------------------
# ENDPOINTS
# -------------------------------------------------------------------
@app.post("/api/jobs/submit", response_model=SubmitJobResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_job(request: SubmitJobRequest, api_key: str = Depends(verify_api_key)):
    """Submit a PySpark job."""
    print(f"🟢 Received job submission: {request.job_name}")

    if request.job_type != "prebuilt":
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Only prebuilt jobs are supported in this version."
        )

    if request.job_name != "top_customers_revenue":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown prebuilt job: {request.job_name}"
        )

    # Create job object
    job = Job(
        job_name=request.job_name,
        job_type=request.job_type,
        timeout_seconds=request.timeout_seconds,
        inputs=request.input_paths,
        output_path=request.output_path,
        log_file=get_job_log_path(None, LOGS_PATH),
        result_file=get_job_output_path(None, OUTPUTS_PATH)
    )
    JOBS_DB[job.job_id] = job

    try:
        process = execute_pyspark_job(
            job.job_id, job.job_name, job.inputs, job.timeout_seconds
        )
        RUNNING_PROCESSES[job.job_id] = process
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now()
        print(f"🚀 Job {job.job_id} started.")
    except Exception as e:
        job.status = JobStatus.FAILED
        job.exit_code = 1
        job.finished_at = datetime.now()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start job process: {e}"
        )

    return SubmitJobResponse(
        job_id=job.job_id,
        status_url=f"/api/jobs/{job.job_id}"
    )

@app.get("/api/jobs/{job_id}", response_model=StatusResponse)
async def get_job_status(job_id: str, api_key: str = Depends(verify_api_key)):
    """Poll job status."""
    job = JOBS_DB.get(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found.")

    result_url = f"/api/jobs/{job_id}/result" if job.status == JobStatus.SUCCEEDED else None
    log_preview = read_log_preview(job.log_file)

    return StatusResponse(
        job_id=job.job_id,
        status=job.status,
        started_at=job.started_at,
        finished_at=job.finished_at,
        exit_code=job.exit_code,
        result_url=result_url,
        log_preview=log_preview
    )

@app.get("/api/jobs/{job_id}/result")
async def get_job_result(job_id: str, api_key: str = Depends(verify_api_key)):
    """Download job result."""
    job = JOBS_DB.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job.status != JobStatus.SUCCEEDED:
        raise HTTPException(status_code=409, detail=f"Job not completed. Status: {job.status}")

    if os.path.exists(job.result_file):
        return FileResponse(path=job.result_file, filename=f"{job_id}.json")
    else:
        raise HTTPException(status_code=500, detail="Job succeeded but result file not found.")

@app.get("/api/jobs/{job_id}/logs")
async def get_job_logs(job_id: str, api_key: str = Depends(verify_api_key)):
    """Return full job logs."""
    job = JOBS_DB.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")

    log_content = read_full_log(job.log_file)
    return JSONResponse(content={"job_id": job_id, "logs": log_content})

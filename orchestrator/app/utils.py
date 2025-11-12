
import subprocess
import os
import json
import time
import base64 
from typing import Dict, Tuple, Optional
from datetime import datetime
from dotenv import load_dotenv


load_dotenv(override=True)


API_KEY = os.getenv("API_KEY")
JOBS_PATH = os.getenv("JOBS_PATH", "../jobs/")
OUTPUTS_PATH = os.getenv("OUTPUTS_PATH", "../outputs/")
LOGS_PATH = os.getenv("LOGS_PATH", "../logs/")


os.makedirs(OUTPUTS_PATH, exist_ok=True)
os.makedirs(LOGS_PATH, exist_ok=True)

def execute_pyspark_job(job_id: str, job_name: str, inputs: Dict[str, str], timeout: int) -> subprocess.Popen:
    """
    Launches a PySpark job as a non-blocking subprocess.
    Returns the Popen object for tracking.
    """
    job_script_path = os.path.join(JOBS_PATH, f"{job_name}.py")
    log_file_path = os.path.join(LOGS_PATH, f"{job_id}.log")

    if not os.path.exists(job_script_path):
        raise FileNotFoundError(f"Job script not found: {job_script_path}")
        
    
    job_args_json = json.dumps(inputs)
    
    
    job_args_base64 = base64.b64encode(job_args_json.encode('utf-8')).decode('utf-8')

    
    command = [
        "python", job_script_path, 
        job_id, job_args_base64, 
        OUTPUTS_PATH
    ]

    
    with open(log_file_path, 'w') as log_file:
       
        process = subprocess.Popen(
            command,
            stdout=log_file, 
            stderr=subprocess.STDOUT, 
            env=os.environ.copy() 
        )
        
    return process 
def check_job_status(job_id: str, process: subprocess.Popen, timeout: int) -> Tuple[str, Optional[int]]:
    """
    Checks the status of the running job process.
    Returns (JobStatus, exit_code)
    """
    
    from app.jobs_db import JOBS_DB, JobStatus 
    job = JOBS_DB.get(job_id)
    if not job:
        return JobStatus.FAILED, 1 

    exit_code = process.poll() 
    
    time_elapsed = (datetime.now() - job.started_at).total_seconds() if job.started_at else 0

    if exit_code is not None:
        # Job has finished
        if exit_code == 0:
            return JobStatus.SUCCEEDED, 0
        else:
            return JobStatus.FAILED, exit_code
    elif time_elapsed > timeout:
        # Job has exceeded the timeout
        process.terminate() 
        time.sleep(1) 
        if process.poll() is None:
            process.kill() 
        return JobStatus.TIMED_OUT, 1
    else:
        # Job is still running
        return JobStatus.RUNNING, None

def read_log_preview(log_file_path: str, max_bytes: int = 1024) -> str:
    """Reads the first few KB of a log file."""
    if not os.path.exists(log_file_path):
        return "Log file not found."
    
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(max_bytes) 
            return content.strip()
    except Exception as e:
        return f"Error reading log file: {e}"

def read_full_log(log_file_path: str) -> str:
    """Reads the entire log file."""
    if not os.path.exists(log_file_path):
        return "Log file not found."
        
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()
    except Exception as e:
        return f"Error reading log file: {e}"
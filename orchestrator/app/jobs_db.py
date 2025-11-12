import uuid
from enum import Enum
from datetime import datetime
from typing import Dict, Union, Any, Optional
import os 

from pydantic import BaseModel, Field, ConfigDict


class JobStatus(str, Enum):
    """
    Defines the possible states of an orchestrated PySpark job.
    Inherits from str for easy JSON serialization and usage.
    """
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"


class Job(BaseModel):
    """
    Model representing the metadata for a single Spark job execution.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    
    job_name: str
    job_type: str  
    timeout_seconds: int
    
    
    status: JobStatus = JobStatus.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    exit_code: Optional[int] = None
    
    
    inputs: Dict[str, str]
    output_path: str
    log_file: str
    result_file: str



JOBS_DB: Dict[str, Job] = {}




def get_job_output_path(job_id: str, outputs_base_path: str) -> str:
    """Generates the absolute path for the final JSON result file using os.path.join."""
    
    return os.path.join(outputs_base_path, f"{job_id}_result.json")

def get_job_log_path(job_id: str, logs_base_path: str) -> str:
    """Generates the absolute path for the log file using os.path.join."""
    
    return os.path.join(logs_base_path, f"{job_id}.log")
"""
FastAPI backend for Data Ingestion Pipeline
Provides REST API endpoints for file upload, validation, and pipeline execution
"""

import os
import sys
import uuid
import asyncio
import tempfile
import threading
import json
import numpy as np
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the project root to the path
sys.path.append(str(Path(__file__).parent))

from services.file_processor import FileProcessor
from services.data_validator import DataValidator
try:
    from services.data_reporter import DataQualityReporter
except ImportError:
    DataQualityReporter = None
from services.kafka_producer_enhanced import EnhancedKafkaProducer
from services.database_service import get_database_service
from services.etl_service import get_etl_service
from data_ingestion_pipeline import DataIngestionPipeline

# Initialize FastAPI app
app = FastAPI(
    title="Data Ingestion Pipeline API",
    description="API for intelligent data validation and ingestion with Kafka integration",
    version="1.0.0"
)

# Add CORS middleware to allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
file_processor = FileProcessor()
data_validator = DataValidator()
reporter = DataQualityReporter() if DataQualityReporter else None
pipeline = DataIngestionPipeline()

# Enhanced In-memory Database for job persistence
import threading

class InMemoryJobDB:
    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.RLock()
    
    def create_job(self, job_id: str, filename: str, action: str) -> Dict[str, Any]:
        """Create a new job entry"""
        with self.lock:
            job_data = {
                'job_id': job_id,
                'filename': filename,
                'action': action,
                'status': 'pending',
                'progress': 0,
                'message': 'Job created, waiting to start...',
                'result': None,
                'error': None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            self.jobs[job_id] = job_data
            return job_data.copy()
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get a job by ID"""
        with self.lock:
            job = self.jobs.get(job_id)
            if job:
                # Convert numpy types before returning
                return convert_numpy_types(job.copy())
            return None
    
    def update_job(self, job_id: str, updates: Dict[str, Any]) -> bool:
        """Update a job with new data"""
        with self.lock:
            if job_id in self.jobs:
                # Convert numpy types to native Python types
                clean_updates = convert_numpy_types(updates)
                self.jobs[job_id].update(clean_updates)
                self.jobs[job_id]['updated_at'] = datetime.now().isoformat()
                return True
            return False
    
    def list_jobs(self) -> List[Dict[str, Any]]:
        """Get all jobs, newest first"""
        with self.lock:
            jobs = list(self.jobs.values())
            # Sort by created_at descending
            jobs.sort(key=lambda x: x['created_at'], reverse=True)
            # Convert numpy types before returning
            return [convert_numpy_types(job.copy()) for job in jobs]
    
    def delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        with self.lock:
            if job_id in self.jobs:
                del self.jobs[job_id]
                return True
            return False
    
    def get_job_count(self) -> int:
        """Get total number of jobs"""
        with self.lock:
            return len(self.jobs)

# Global in-memory database instance
job_db = InMemoryJobDB()

# Legacy compatibility
job_status: Dict[str, Dict[str, Any]] = {}

# JSON serialization helper for NumPy types
def convert_numpy_types(obj):
    """Convert NumPy types to Python native types for JSON serialization"""
    if isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj

# Pydantic models for request/response
class ValidationRequest(BaseModel):
    quality_threshold: float = 0.8
    output_format: str = "json"

class KafkaRequest(BaseModel):
    topic: str
    quality_threshold: float = 0.8

class PipelineRequest(BaseModel):
    topic: str = "data_ingestion_topic"
    quality_threshold: float = 0.8
    timeout_seconds: int = 30

class ApiStreamingRequest(BaseModel):
    api_url: str
    action: str = "validate"
    quality_threshold: float = 0.8
    timeout_seconds: int = 300  # 5 minutes default for streaming

class JobStatus(BaseModel):
    job_id: str
    status: str  # "pending", "running", "completed", "failed"
    progress: int  # 0-100
    message: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Data Ingestion Pipeline API",
        "status": "running", 
        "version": "3.0.0",
        "features": ["validation", "kafka", "streaming", "data_warehouse", "dynamic_schema"],
        "endpoints": {
            "upload": "/api/upload",
            "upload_dynamic": "/api/upload-dynamic",
            "validate": "/api/validate/{job_id}",
            "kafka": "/api/kafka/{job_id}",
            "pipeline": "/api/pipeline/{job_id}",
            "stream": "/api/stream",
            "status": "/api/status/{job_id}",
            "download": "/api/download/{job_id}/{file_type}",
            "warehouse": {
                "jobs": "/api/warehouse/jobs",
                "metrics": "/api/warehouse/metrics/{job_id}",
                "quarantine": "/api/warehouse/quarantine",
                "analytics": "/api/warehouse/analytics",
                "etl_trigger": "/api/warehouse/etl/trigger"
            },
            "dynamic_schema": {
                "process": "/api/dynamic-schema/process",
                "schemas": "/api/dynamic-schema/schemas",
                "schema_details": "/api/dynamic-schema/schemas/{schema_id}",
                "schema_data": "/api/dynamic-schema/schemas/{schema_id}/data",
                "schema_evolution": "/api/dynamic-schema/schemas/{schema_name}/evolution",
                "infer_only": "/api/dynamic-schema/infer"
            }
        }
    }

@app.get("/api/test")
async def test_endpoint():
    """Simple test endpoint"""
    return {"status": "ok", "message": "API is working"}

@app.get("/api/health")
async def health_check():
    """Detailed health check"""
    return {
        "api": "healthy",
        "services": {
            "file_processor": "ready",
            "data_validator": "ready", 
            "kafka_producer": "ready",
            "reporter": "ready"
        },
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    action: str = Form("validate")  # "validate", "kafka", "pipeline"
):
    """
    Upload a file and start processing
    Returns job_id for tracking progress
    """
    # Validate file
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    # Generate job ID
    job_id = str(uuid.uuid4())
    
    # Create job entry in database
    job_data = job_db.create_job(job_id, file.filename, action)
    
    # Save uploaded file temporarily
    temp_dir = Path("temp_uploads")
    temp_dir.mkdir(exist_ok=True)
    
    file_path = temp_dir / f"{job_id}_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Update job with file path
        job_db.update_job(job_id, {
            "file_path": str(file_path),
            "message": "File saved, processing queued"
        })
        
        # Start background processing based on action
        if action == "validate":
            background_tasks.add_task(process_validation, job_id, str(file_path))
        elif action == "kafka":
            background_tasks.add_task(process_kafka, job_id, str(file_path))
        elif action == "pipeline":
            background_tasks.add_task(process_full_pipeline, job_id, str(file_path))
        else:
            raise HTTPException(status_code=400, detail=f"Invalid action: {action}")
        
        return {"job_id": job_id, "status": "accepted", "message": "Processing started"}
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")

@app.post("/api/upload-multiple")
async def upload_multiple_files(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    action: str = Form("validate")
):
    """
    Upload multiple files and start processing each as a separate job.
    Returns list of job_ids for tracking progress.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    results: List[Dict[str, Any]] = []

    temp_dir = Path("temp_uploads")
    temp_dir.mkdir(exist_ok=True)

    for file in files:
        if not file.filename:
            continue

        job_id = str(uuid.uuid4())
        job_db.create_job(job_id, file.filename, action)

        file_path = temp_dir / f"{job_id}_{file.filename}"
        try:
            content = await file.read()
            with open(file_path, "wb") as buffer:
                buffer.write(content)

            job_db.update_job(job_id, {
                "file_path": str(file_path),
                "message": "File saved, processing queued"
            })

            if action == "validate":
                background_tasks.add_task(process_validation, job_id, str(file_path))
            elif action == "kafka":
                background_tasks.add_task(process_kafka, job_id, str(file_path))
            elif action == "pipeline":
                background_tasks.add_task(process_full_pipeline, job_id, str(file_path))
            else:
                job_db.update_job(job_id, {"status": "failed", "error": f"Invalid action: {action}"})
                continue

            results.append({"job_id": job_id, "status": "accepted", "message": "Processing started"})
        except Exception as e:
            job_db.update_job(job_id, {"status": "failed", "error": str(e)})
            results.append({"job_id": job_id, "status": "failed", "message": str(e)})

    if not results:
        raise HTTPException(status_code=400, detail="No valid files processed")

    return {"jobs": results}

@app.post("/api/stream")
async def start_api_streaming(
    background_tasks: BackgroundTasks,
    request: ApiStreamingRequest
):
    """
    Start streaming data from an API endpoint and process it.
    Returns job_id for tracking progress.
    """
    # Generate job ID
    job_id = str(uuid.uuid4())
    
    # Create job entry
    job_db.create_job(job_id, f"API Stream: {request.api_url}", request.action)
    
    # Start background streaming task
    background_tasks.add_task(
        process_api_streaming, 
        job_id, 
        request.api_url, 
        request.action,
        request.quality_threshold,
        request.timeout_seconds
    )
    
    return {"job_id": job_id, "status": "accepted", "message": "API streaming started"}

async def process_validation(job_id: str, file_path: str):
    """Background task for file validation"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Starting validation..."
        })
        
        # Load and validate file
        df = file_processor.load_data_from_file(file_path)
        job_db.update_job(job_id, {
            "progress": 30,
            "message": f"Loaded {len(df)} records"
        })
        
        records = file_processor.convert_dataframe_to_records(df, clean_data=True)
        job_db.update_job(job_id, {
            "progress": 50,
            "message": "Running intelligent validation..."
        })
        
        # Validate data
        validation_results = data_validator.validate_batch(records, file_path)
        job_db.update_job(job_id, {
            "progress": 80,
            "message": "Generating report..."
        })
        
        # Generate statistics
        stats = data_validator.get_statistics()
        good_data = data_validator.get_good_data()
        bad_data = data_validator.get_bad_data()
        
        # Create output directory
        output_dir = Path(f"api_output/{job_id}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate report
        report_path = None
        if reporter:
            report_path = reporter.generate_comprehensive_report(
                validation_results,
                stats,
                output_format="html"
            )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "Validation completed successfully",
            "result": {
                "total_records": len(records),
                "valid_records": len(good_data),
                "invalid_records": len(bad_data),
                "success_rate": f"{(len(good_data) / len(records) * 100):.2f}%" if records else "0%",
                "statistics": stats,
                "report_path": str(report_path) if report_path else None,
                "schema_info": data_validator.get_schema_info() if hasattr(data_validator, 'get_schema_info') else None
            }
        })
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

async def process_kafka(job_id: str, file_path: str, topic: str = "data_ingestion_api"):
    """Background task for Kafka sending"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Starting Kafka processing..."
        })
        
        # Run file-to-kafka pipeline
        results = pipeline.run_file_to_kafka_pipeline(
            input_file=file_path,
            topic=topic,
            validate_before_send=True
        )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "Data sent to Kafka successfully",
            "result": results
        })
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

async def process_full_pipeline(job_id: str, file_path: str, topic: str = "data_ingestion_api"):
    """Background task for full pipeline"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Starting full pipeline..."
        })
        
        # Create output directory
        output_dir = f"api_output/{job_id}"
        
        # Run full pipeline
        results = pipeline.run_full_pipeline(
            input_file=file_path,
            topic=topic,
            quality_threshold=0.8,
            timeout_seconds=30,
            output_dir=output_dir
        )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "Full pipeline completed successfully",
            "result": results
        })
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

async def process_api_streaming(job_id: str, api_url: str, action: str, quality_threshold: float, timeout_seconds: int):
    """Background task for API streaming"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Connecting to API..."
        })
        
        # Import streaming service
        from services.api_streaming_service import ApiStreamingService
        
        # Create streaming service
        streaming_service = ApiStreamingService()
        
        # Create output directory
        output_dir = f"api_output/{job_id}"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        job_db.update_job(job_id, {
            "progress": 20,
            "message": "Starting data stream..."
        })
        
        # Stream and process data
        results = await streaming_service.stream_and_process(
            api_url=api_url,
            action=action,
            quality_threshold=quality_threshold,
            timeout_seconds=timeout_seconds,
            output_dir=output_dir,
            progress_callback=lambda progress, message: job_db.update_job(job_id, {
                "progress": 20 + int(progress * 0.7),  # 20-90% for streaming
                "message": message
            })
        )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "API streaming completed successfully",
            "result": results
        })
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

@app.get("/api/status/{job_id}")
async def get_job_status(job_id: str):
    """Get job status and progress"""
    job = job_db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job

@app.get("/api/jobs")
async def list_jobs():
    """List all jobs"""
    jobs = job_db.list_jobs()
    return {
        "jobs": jobs,
        "count": len(jobs)
    }

@app.get("/api/recent-validations")
async def get_recent_validations(limit: int = 5):
    """Get recent validation jobs for dashboard display"""
    jobs = job_db.list_jobs()
    
    # Filter only validation jobs and limit results
    recent_validations = []
    for job in jobs[:limit]:
        # Format the validation data for frontend consumption
        validation = {
            "id": job.get("job_id", ""),
            "name": job.get("filename", "Unknown file"),
            "status": map_job_status(job.get("status", "unknown")),
            "timestamp": format_timestamp(job.get("created_at", "")),
            "action": job.get("action", "validation"),
            "progress": job.get("progress", 0),
            "result": job.get("result", {})
        }
        recent_validations.append(validation)
    
    return {
        "validations": recent_validations,
        "count": len(recent_validations)
    }

def map_job_status(status: str) -> str:
    """Map internal job status to user-friendly status"""
    status_map = {
        "completed": "completed",
        "failed": "error",
        "running": "running",
        "pending": "pending"
    }
    return status_map.get(status.lower(), "unknown")

def format_timestamp(timestamp_str: str) -> str:
    """Format ISO timestamp to human-readable relative time"""
    if not timestamp_str:
        return "Unknown time"
    
    try:
        from datetime import datetime, timezone
        job_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc) if job_time.tzinfo else datetime.now()
        
        diff = now - job_time
        
        if diff.days > 0:
            return f"{diff.days} day{'s' if diff.days > 1 else ''} ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
        else:
            return "Just now"
    except Exception:
        return "Unknown time"

@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its files"""
    job = job_db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Clean up files
    try:
        if "file_path" in job:
            Path(job["file_path"]).unlink(missing_ok=True)
        
        # Remove output directory
        output_dir = Path(f"api_output/{job_id}")
        if output_dir.exists():
            import shutil
            shutil.rmtree(output_dir)
        
        # Remove job from database
        job_db.delete_job(job_id)
        
        return {"message": "Job deleted successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")

@app.get("/api/download/{job_id}/{file_type}")
async def download_file(job_id: str, file_type: str):
    """Download generated files (report, data, etc.)"""
    job = job_db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    
    output_dir = Path(f"api_output/{job_id}")
    
    if file_type == "report":
        # Find HTML report
        html_files = list(output_dir.glob("*.html"))
        if html_files:
            return FileResponse(html_files[0], filename=f"report_{job_id}.html")
    elif file_type == "good_data":
        # Find good data CSV
        csv_files = list(output_dir.glob("good_data.csv"))
        if csv_files:
            return FileResponse(csv_files[0], filename=f"good_data_{job_id}.csv")
    elif file_type == "all_data":
        # Find processed data
        csv_files = list(output_dir.glob("high_quality_data.csv"))
        if csv_files:
            return FileResponse(csv_files[0], filename=f"processed_data_{job_id}.csv")
    
    raise HTTPException(status_code=404, detail=f"File type '{file_type}' not found")

@app.get("/api/quarantine")
async def get_quarantined_items():
    """Get all quarantined items from completed jobs"""
    jobs = job_db.list_jobs()
    quarantined_items = []
    
    for job in jobs:
        if job["status"] == "completed" and job.get("result", {}).get("invalid_records", 0) > 0:
            quarantined_items.append({
                "id": job["job_id"],
                "filename": job["filename"],
                "reason": f"Validation failed: {job['result']['invalid_records']} invalid records",
                "severity": "high" if job["result"]["invalid_records"] > job["result"]["total_records"] * 0.1 else "medium",
                "records": job["result"]["invalid_records"],
                "total_records": job["result"]["total_records"],
                "quarantine_date": job["updated_at"],
                "risk_score": max(0, 100 - int(float(job["result"]["success_rate"].replace("%", ""))))
            })
    
    return {"quarantined_items": quarantined_items, "count": len(quarantined_items)}

@app.get("/api/statistics")
async def get_statistics():
    """Get overall system statistics"""
    jobs = job_db.list_jobs()
    
    total_jobs = len(jobs)
    completed_jobs = len([j for j in jobs if j["status"] == "completed"])
    failed_jobs = len([j for j in jobs if j["status"] == "failed"])
    running_jobs = len([j for j in jobs if j["status"] == "running"])
    
    total_records = sum([j.get("result", {}).get("total_records", 0) for j in jobs if j["status"] == "completed"])
    valid_records = sum([j.get("result", {}).get("valid_records", 0) for j in jobs if j["status"] == "completed"])
    invalid_records = sum([j.get("result", {}).get("invalid_records", 0) for j in jobs if j["status"] == "completed"])
    
    return {
        "jobs": {
            "total": total_jobs,
            "completed": completed_jobs,
            "failed": failed_jobs,
            "running": running_jobs
        },
        "records": {
            "total": total_records,
            "valid": valid_records,
            "invalid": invalid_records,
            "success_rate": f"{(valid_records / total_records * 100):.2f}%" if total_records > 0 else "0%"
        }
    }

# ========== DATA WAREHOUSE ENDPOINTS ==========

@app.get("/api/warehouse/jobs")
async def get_warehouse_jobs(limit: int = 50):
    """Get recent jobs from data warehouse"""
    try:
        db_service = await get_database_service()
        jobs = await db_service.get_recent_jobs(limit)
        return {"jobs": jobs, "count": len(jobs)}
    except Exception as e:
        logger.error(f"Error fetching warehouse jobs: {str(e)}")
        # Fallback to in-memory jobs
        jobs = job_db.list_jobs()[:limit]
        return {"jobs": jobs, "count": len(jobs)}

@app.get("/api/warehouse/metrics/{job_id}")
async def get_job_metrics(job_id: str):
    """Get detailed metrics for a job from data warehouse"""
    try:
        db_service = await get_database_service()
        job_status = await db_service.get_job_status(job_id)
        
        if not job_status:
            raise HTTPException(status_code=404, detail="Job not found in warehouse")
        
        # Get detailed validation results
        query = """
            SELECT 
                COUNT(*) as total_validations,
                COUNT(CASE WHEN is_valid THEN 1 END) as valid_count,
                AVG(quality_score) as avg_quality_score,
                COUNT(CASE WHEN quality_level = 'excellent' THEN 1 END) as excellent_count,
                COUNT(CASE WHEN quality_level = 'good' THEN 1 END) as good_count,
                COUNT(CASE WHEN quality_level = 'fair' THEN 1 END) as fair_count,
                COUNT(CASE WHEN quality_level = 'poor' THEN 1 END) as poor_count
            FROM warehouse.fact_validation_result 
            WHERE job_id = :job_id
        """
        
        validation_metrics = await db_service.database.fetch_one(query=query, values={'job_id': job_id})
        
        # Get quality metrics by field
        field_metrics_query = """
            SELECT field_name, metric_type, metric_value, data_type
            FROM warehouse.fact_quality_metric
            WHERE job_id = :job_id
            ORDER BY field_name, metric_type
        """
        
        field_metrics = await db_service.database.fetch_all(query=field_metrics_query, values={'job_id': job_id})
        
        return {
            "job_status": dict(job_status),
            "validation_metrics": dict(validation_metrics) if validation_metrics else {},
            "field_metrics": [dict(fm) for fm in field_metrics]
        }
        
    except Exception as e:
        logger.error(f"Error fetching job metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch metrics: {str(e)}")

@app.get("/api/warehouse/quarantine")
async def get_warehouse_quarantine(limit: int = 100):
    """Get quarantined data from warehouse"""
    try:
        db_service = await get_database_service()
        
        query = """
            SELECT 
                q.quarantine_id, q.job_id, q.record_id, q.quarantine_reason,
                q.severity_score, q.status, q.quarantined_at, q.issue_types,
                j.job_name, j.job_type, ds.source_name
            FROM warehouse.quarantine_data q
            LEFT JOIN warehouse.fact_processing_job j ON q.job_id = j.job_id
            LEFT JOIN warehouse.dim_data_source ds ON j.source_id = ds.source_id
            ORDER BY q.quarantined_at DESC
            LIMIT :limit
        """
        
        quarantine_data = await db_service.database.fetch_all(query=query, values={'limit': limit})
        
        return {
            "quarantined_items": [dict(item) for item in quarantine_data],
            "count": len(quarantine_data)
        }
        
    except Exception as e:
        logger.error(f"Error fetching quarantine data: {str(e)}")
        # Fallback to in-memory quarantine
        return await get_quarantined_items()

@app.get("/api/warehouse/analytics")
async def get_warehouse_analytics():
    """Get analytics and summary statistics from warehouse"""
    try:
        db_service = await get_database_service()
        
        # Job statistics
        job_stats_query = """
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_jobs,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_jobs,
                COUNT(CASE WHEN status = 'processing' THEN 1 END) as running_jobs,
                AVG(processing_duration_seconds) as avg_processing_time,
                SUM(total_records) as total_records_processed,
                AVG(avg_quality_score) as overall_avg_quality
            FROM warehouse.fact_processing_job
            WHERE started_at >= NOW() - INTERVAL '30 days'
        """
        
        job_stats = await db_service.database.fetch_one(query=job_stats_query)
        
        # Data source statistics
        source_stats_query = """
            SELECT 
                ds.source_type,
                COUNT(pj.job_id) as job_count,
                AVG(pj.success_rate) as avg_success_rate
            FROM warehouse.dim_data_source ds
            LEFT JOIN warehouse.fact_processing_job pj ON ds.source_id = pj.source_id
            WHERE pj.started_at >= NOW() - INTERVAL '30 days'
            GROUP BY ds.source_type
        """
        
        source_stats = await db_service.database.fetch_all(query=source_stats_query)
        
        # Quality distribution
        quality_dist_query = """
            SELECT 
                quality_level,
                COUNT(*) as count
            FROM warehouse.fact_validation_result vr
            JOIN warehouse.fact_processing_job pj ON vr.job_id = pj.job_id
            WHERE pj.started_at >= NOW() - INTERVAL '30 days'
            GROUP BY quality_level
        """
        
        quality_distribution = await db_service.database.fetch_all(query=quality_dist_query)
        
        return {
            "job_statistics": dict(job_stats) if job_stats else {},
            "source_statistics": [dict(stat) for stat in source_stats],
            "quality_distribution": [dict(dist) for dist in quality_distribution],
            "period": "last_30_days"
        }
        
    except Exception as e:
        logger.error(f"Error fetching analytics: {str(e)}")
        # Fallback to in-memory statistics
        return await get_statistics()

@app.post("/api/warehouse/etl")
async def trigger_warehouse_etl(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    source_name: str = Form(...),
    job_name: str = Form(None),
    quality_threshold: float = Form(0.8)
):
    """Trigger ETL process to data warehouse"""
    try:
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Save uploaded file
        temp_dir = Path("temp_uploads")
        temp_dir.mkdir(exist_ok=True)
        file_path = temp_dir / f"{job_id}_{file.filename}"
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Create in-memory job entry for immediate response
        job_db.create_job(job_id, file.filename, "warehouse_etl")
        
        # Start background ETL process
        background_tasks.add_task(
            process_warehouse_etl,
            job_id,
            str(file_path),
            source_name,
            job_name,
            quality_threshold
        )
        
        return {
            "job_id": job_id,
            "status": "accepted",
            "message": "ETL process started",
            "warehouse_enabled": True
        }
        
    except Exception as e:
        logger.error(f"ETL trigger failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start ETL: {str(e)}")

async def process_warehouse_etl(job_id: str, file_path: str, source_name: str, 
                               job_name: str = None, quality_threshold: float = 0.8):
    """Background task for warehouse ETL processing"""
    try:
        # Update job status
        job_db.update_job(job_id, {
            "status": "processing",
            "message": "Starting ETL process..."
        })
        
        # Get ETL service and process file
        etl_service = await get_etl_service()
        result = await etl_service.process_file_to_warehouse(
            file_path=file_path,
            source_name=source_name,
            job_name=job_name or f"ETL_{Path(file_path).stem}",
            quality_threshold=quality_threshold
        )
        
        # Update job with results
        job_db.update_job(job_id, {
            "status": "completed",
            "message": "ETL process completed successfully",
            "result": result,
            "warehouse_job_id": result.get("job_id")  # Store warehouse job ID
        })
        
        logger.info(f"Warehouse ETL completed for job {job_id}")
        
    except Exception as e:
        logger.error(f"Warehouse ETL failed for job {job_id}: {str(e)}")
        job_db.update_job(job_id, {
            "status": "failed", 
            "error": str(e)
        })

# ========================================
# DYNAMIC SCHEMA ENDPOINTS
# ========================================

class DynamicSchemaRequest(BaseModel):
    data_records: List[Dict[str, Any]]
    source_name: str
    source_type: str = "api"
    storage_method: str = "flexible"  # 'flexible' or 'structured'

@app.post("/api/dynamic-schema/process")
async def process_dynamic_schema(
    background_tasks: BackgroundTasks,
    request: DynamicSchemaRequest
):
    """
    Process data with dynamic schema detection and storage
    Automatically infers schema and stores data in optimal format
    """
    try:
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Create job entry
        job_db.create_job(job_id, f"Dynamic Schema: {request.source_name}", "dynamic_schema")
        
        # Start background processing
        background_tasks.add_task(
            process_dynamic_schema_background,
            job_id,
            request.data_records,
            request.source_name,
            request.source_type,
            request.storage_method
        )
        
        return {
            "job_id": job_id,
            "status": "accepted",
            "message": "Dynamic schema processing started",
            "source_name": request.source_name,
            "record_count": len(request.data_records),
            "storage_method": request.storage_method
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start dynamic schema processing: {str(e)}")

async def process_dynamic_schema_background(
    job_id: str,
    data_records: List[Dict[str, Any]],
    source_name: str,
    source_type: str,
    storage_method: str
):
    """Background task for dynamic schema processing"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Starting dynamic schema detection..."
        })
        
        # Get ETL service
        etl_service = await get_etl_service()
        
        job_db.update_job(job_id, {
            "progress": 30,
            "message": "Inferring schema from data..."
        })
        
        # Process with dynamic schema
        result = await etl_service.process_with_dynamic_schema(
            data_records=data_records,
            source_name=source_name,
            source_type=source_type,
            job_id=job_id,
            storage_method=storage_method
        )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "Dynamic schema processing completed",
            "result": result
        })
        
        logger.info(f"Dynamic schema processing completed for job {job_id}")
        
    except Exception as e:
        logger.error(f"Dynamic schema processing failed for job {job_id}: {str(e)}")
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

@app.get("/api/dynamic-schema/schemas")
async def list_dynamic_schemas():
    """List all registered dynamic schemas"""
    try:
        from services.dynamic_schema_service import get_dynamic_schema_service
        dynamic_service = await get_dynamic_schema_service()
        
        schemas = dynamic_service.list_schemas()
        
        return {
            "schemas": schemas,
            "count": len(schemas)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {str(e)}")

@app.get("/api/dynamic-schema/schemas/{schema_id}")
async def get_schema_details(schema_id: str):
    """Get detailed information about a specific schema"""
    try:
        from services.dynamic_schema_service import get_dynamic_schema_service
        dynamic_service = await get_dynamic_schema_service()
        
        schema_info = dynamic_service.get_schema_info(schema_id)
        if not schema_info:
            raise HTTPException(status_code=404, detail="Schema not found")
        
        return schema_info
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get schema details: {str(e)}")

@app.get("/api/dynamic-schema/schemas/{schema_id}/data")
async def query_schema_data(
    schema_id: str,
    limit: int = 100,
    offset: int = 0,
    validation_status: Optional[str] = None,
    job_id: Optional[str] = None
):
    """Query data stored for a specific schema"""
    try:
        from services.dynamic_schema_service import get_dynamic_schema_service
        dynamic_service = await get_dynamic_schema_service()
        
        # Build filters
        filters = {}
        if validation_status:
            filters['validation_status'] = validation_status
        if job_id:
            filters['job_id'] = job_id
        
        result = dynamic_service.query_data(
            schema_id=schema_id,
            filters=filters,
            limit=limit,
            offset=offset
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query schema data: {str(e)}")

@app.get("/api/dynamic-schema/schemas/{schema_name}/evolution")
async def get_schema_evolution(schema_name: str):
    """Get evolution history of a schema"""
    try:
        from services.dynamic_schema_service import get_dynamic_schema_service
        dynamic_service = await get_dynamic_schema_service()
        
        evolution = dynamic_service.get_schema_evolution(schema_name)
        
        return {
            "schema_name": schema_name,
            "versions": evolution,
            "version_count": len(evolution)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get schema evolution: {str(e)}")

@app.post("/api/dynamic-schema/infer")
async def infer_schema_only(request: Dict[str, Any]):
    """Infer schema from sample data without storing"""
    try:
        data_records = request.get("data_records", [])
        source_name = request.get("source_name", "sample_data")
        
        if not data_records:
            raise HTTPException(status_code=400, detail="No data records provided")
        
        # Get ETL service for schema inference
        etl_service = await get_etl_service()
        
        # Infer schema only
        schema_definition = await etl_service._infer_dynamic_schema(data_records, source_name)
        
        return {
            "schema_definition": schema_definition,
            "record_count": len(data_records),
            "field_count": len(schema_definition.get('fields', {})),
            "inference_method": "intelligent_validator"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to infer schema: {str(e)}")

# File upload with dynamic schema
@app.post("/api/upload-dynamic")
async def upload_file_dynamic_schema(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    storage_method: str = Form("flexible")  # "flexible" or "structured"
):
    """
    Upload file and process with dynamic schema detection
    Automatically adapts to any file structure
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    # Generate job ID
    job_id = str(uuid.uuid4())
    
    # Create job entry
    job_data = job_db.create_job(job_id, file.filename, "upload_dynamic")
    
    # Save uploaded file temporarily
    temp_dir = Path("temp_uploads")
    temp_dir.mkdir(exist_ok=True)
    
    file_path = temp_dir / f"{job_id}_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Update job with file path
        job_db.update_job(job_id, {
            "file_path": str(file_path),
            "message": "File saved, processing with dynamic schema detection..."
        })
        
        # Start background processing
        background_tasks.add_task(
            process_file_dynamic_schema,
            job_id,
            str(file_path),
            file.filename,
            storage_method
        )
        
        return {
            "job_id": job_id,
            "status": "accepted",
            "message": "File uploaded, dynamic schema processing started",
            "storage_method": storage_method
        }
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")

async def process_file_dynamic_schema(
    job_id: str,
    file_path: str,
    filename: str,
    storage_method: str
):
    """Background task for processing file with dynamic schema"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Loading file data..."
        })
        
        # Load file data
        df = file_processor.load_data_from_file(file_path)
        records = file_processor.convert_dataframe_to_records(df, clean_data=True)
        
        job_db.update_job(job_id, {
            "progress": 30,
            "message": f"Loaded {len(records)} records, inferring schema..."
        })
        
        # Get ETL service
        etl_service = await get_etl_service()
        
        # Process with dynamic schema
        result = await etl_service.process_with_dynamic_schema(
            data_records=records,
            source_name=filename,
            source_type='file',
            job_id=job_id,
            storage_method=storage_method
        )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "Dynamic schema processing completed",
            "result": result
        })
        
        logger.info(f"Dynamic file processing completed for job {job_id}")
        
    except Exception as e:
        logger.error(f"Dynamic file processing failed for job {job_id}: {str(e)}")
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

# Reports and Analytics Endpoints
@app.get("/api/reports/dashboard-stats")
async def get_dashboard_stats():
    """Get dashboard statistics for charts"""
    try:
        db_service = await get_database_service()
        
        # Get processing job statistics
        query = """
        SELECT 
            status,
            COUNT(*) as count,
            AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration
        FROM warehouse.fact_processing_job 
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY status
        """
        
        job_stats = await db_service.execute_query(query)
        
        # Get data quality metrics
        quality_query = """
        SELECT 
            DATE(created_at) as date,
            AVG(completeness_score) as completeness,
            AVG(validity_score) as validity,
            AVG(consistency_score) as consistency,
            COUNT(*) as records_processed
        FROM warehouse.fact_quality_metric 
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY DATE(created_at)
        ORDER BY date
        """
        
        quality_stats = await db_service.execute_query(quality_query)
        
        # Get error distribution
        error_query = """
        SELECT 
            error_type,
            COUNT(*) as count
        FROM warehouse.fact_validation_result 
        WHERE is_valid = false
        AND created_at >= NOW() - INTERVAL '30 days'
        GROUP BY error_type
        ORDER BY count DESC
        LIMIT 10
        """
        
        error_stats = await db_service.execute_query(error_query)
        
        # Convert to dictionaries
        job_stats_list = [dict(row) for row in job_stats] if job_stats else []
        quality_stats_list = [dict(row) for row in quality_stats] if quality_stats else []
        error_stats_list = [dict(row) for row in error_stats] if error_stats else []
        
        return {
            "job_statistics": job_stats_list,
            "quality_trends": quality_stats_list,
            "error_distribution": error_stats_list,
            "summary": {
                "total_jobs": sum(row.get("count", 0) for row in job_stats_list),
                "avg_quality_score": sum(row.get("completeness", 0) + row.get("validity", 0) + row.get("consistency", 0) for row in quality_stats_list) / (3 * len(quality_stats_list)) if quality_stats_list else 0,
                "total_errors": sum(row.get("count", 0) for row in error_stats_list)
            }
        }
        
    except Exception as e:
        # Return empty data structure if database queries fail
        logger.error(f"Dashboard stats query failed: {e}")
        return {
            "job_statistics": [],
            "quality_trends": [],
            "error_distribution": [],
            "summary": {
                "total_jobs": 0,
                "avg_quality_score": 0,
                "total_errors": 0
            }
        }

@app.get("/api/reports/processing-timeline")
async def get_processing_timeline(days: int = 7):
    """Get processing timeline data for charts"""
    try:
        db_service = await get_database_service()
        
        query = """
        SELECT 
            DATE_TRUNC('hour', created_at) as timestamp,
            status,
            COUNT(*) as count,
            AVG(records_processed) as avg_records,
            SUM(records_processed) as total_records
        FROM warehouse.fact_processing_job 
        WHERE created_at >= NOW() - INTERVAL %s
        GROUP BY DATE_TRUNC('hour', created_at), status
        ORDER BY timestamp
        """
        
        timeline_data = await db_service.execute_query(query, (f"{days} days",))
        
        return {
            "timeline": [dict(row) for row in timeline_data] if timeline_data else [],
            "period_days": days
        }
        
    except Exception as e:
        # Return empty timeline if database queries fail
        logger.error(f"Processing timeline query failed: {e}")
        return {
            "timeline": [],
            "period_days": days
        }

@app.get("/api/reports/data-sources")
async def get_data_sources_stats():
    """Get data sources statistics for charts"""
    try:
        db_service = await get_database_service()
        
        query = """
        SELECT 
            ds.source_name,
            ds.source_type,
            COUNT(fpj.id) as total_jobs,
            SUM(CASE WHEN fpj.status = 'completed' THEN 1 ELSE 0 END) as successful_jobs,
            AVG(fpj.records_processed) as avg_records,
            MAX(fpj.created_at) as last_processed
        FROM dim_data_source ds
        LEFT JOIN fact_processing_job fpj ON ds.id = fpj.data_source_id
        WHERE fpj.created_at >= NOW() - INTERVAL '30 days'
        GROUP BY ds.id, ds.source_name, ds.source_type
        ORDER BY total_jobs DESC
        """
        
        sources_data = await db_service.execute_query(query)
        
        return {
            "data_sources": [dict(row) for row in sources_data]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get data sources stats: {str(e)}")

@app.get("/api/reports/quality-metrics")
async def get_quality_metrics(days: int = 30):
    """Get detailed quality metrics for charts"""
    try:
        db_service = await get_database_service()
        
        query = """
        SELECT 
            DATE(created_at) as date,
            metric_name,
            AVG(metric_value) as avg_value,
            MIN(metric_value) as min_value,
            MAX(metric_value) as max_value,
            COUNT(*) as sample_count
        FROM fact_quality_metric 
        WHERE created_at >= NOW() - INTERVAL %s
        GROUP BY DATE(created_at), metric_name
        ORDER BY date, metric_name
        """
        
        metrics_data = await db_service.execute_query(query, (f"{days} days",))
        
        # Get validation results distribution
        validation_query = """
        SELECT 
            validation_rule,
            SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) as failed
        FROM fact_validation_result 
        WHERE created_at >= NOW() - INTERVAL %s
        GROUP BY validation_rule
        ORDER BY (passed + failed) DESC
        """
        
        validation_data = await db_service.execute_query(validation_query, (f"{days} days",))
        
        return {
            "quality_metrics": [dict(row) for row in metrics_data],
            "validation_results": [dict(row) for row in validation_data],
            "period_days": days
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get quality metrics: {str(e)}")

# Serve the React frontend (for production)
# Create necessary directories and startup
@app.on_event("startup")
async def complete_startup_event():
    """Complete startup initialization"""
    # Create necessary directories
    Path("temp_uploads").mkdir(exist_ok=True)
    Path("api_output").mkdir(exist_ok=True)
    
    # Initialize database and ETL services
    try:
        db_service = await get_database_service()
        etl_service = await get_etl_service()
        logger.info("✅ Database and ETL services initialized")
    except Exception as e:
        logger.warning(f"⚠️  Database services unavailable: {str(e)}")
        logger.info("📝 Running in file-only mode")
    
    print("🚀 Data Ingestion Pipeline API started!")
    print("📊 Frontend available at: http://localhost:8000")
    print("📝 API docs available at: http://localhost:8000/docs")
    print("🗄️  Data warehouse endpoints available at: /api/warehouse/*")

@app.on_event("shutdown")
async def complete_shutdown_event():
    """Complete shutdown cleanup"""
    try:
        db_service = await get_database_service()
        await db_service.close()
        logger.info("🔒 Database connections closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")

# Mount static files for production (commented out for development)
# frontend_dist = Path("frontend/dist")
# if frontend_dist.exists():
#     app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")

if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

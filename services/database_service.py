"""
Database service layer for PostgreSQL data warehouse operations
Handles connection management, CRUD operations, and ETL processes
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import json
import hashlib

from sqlalchemy import create_engine, text, MetaData
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import redis.asyncio as redis

from database.models import (
    Base, DimDate, DimDataSource, DimDataSchema, FactProcessingJob,
    FactValidationResult, FactQualityMetric, StagingData, QuarantineData,
    DataLineage, AnalyticsJobSummary
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration settings"""
    
    def __init__(self):
        # PostgreSQL settings
        self.db_host = "localhost"
        self.db_port = 5432
        self.db_name = "data_warehouse"
        self.db_user = "datauser"
        self.db_password = "datapass123"
        
        # Connection pool settings
        self.pool_size = 10
        self.max_overflow = 20
        self.pool_pre_ping = True
        self.pool_recycle = 3600
        
        # Redis settings
        self.redis_host = "localhost"
        self.redis_port = 6379
        self.redis_db = 0
        
    @property
    def database_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    @property
    def async_database_url(self) -> str:
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


class DatabaseService:
    """Main database service for data warehouse operations"""
    
    def __init__(self, config: DatabaseConfig = None):
        self.config = config or DatabaseConfig()
        
        # SQLAlchemy engines
        self.engine = None
        self.async_engine = None
        self.async_session_factory = None
        
        # Redis connection
        self.redis_client = None
        
        # Session factories
        self.session_factory = None
        
        self._initialized = False
    
    async def initialize(self):
        """Initialize database connections and create tables"""
        if self._initialized:
            return
        
        try:
            # Create engines
            self.engine = create_engine(
                self.config.database_url,
                poolclass=QueuePool,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_pre_ping=self.config.pool_pre_ping,
                pool_recycle=self.config.pool_recycle,
                echo=False  # Set to True for SQL debugging
            )
            
            self.async_engine = create_async_engine(
                self.config.async_database_url,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_pre_ping=self.config.pool_pre_ping,
                pool_recycle=self.config.pool_recycle,
                echo=False
            )
            
            # Create session factories
            self.session_factory = sessionmaker(bind=self.engine)
            self.async_session_factory = async_sessionmaker(
                bind=self.async_engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Redis connection
            self.redis_client = redis.from_url(self.config.redis_url)
            
            # Create tables
            await self.create_tables()
            
            # Initialize date dimension
            await self.initialize_date_dimension()
            
            self._initialized = True
            logger.info("Database service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database service: {str(e)}")
            raise
    
    async def create_tables(self):
        """Create all database tables"""
        try:
            async with self.async_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise
    
    async def close(self):
        """Close all database connections"""
        try:
            if self.redis_client:
                await self.redis_client.close()
            
            if self.async_engine:
                await self.async_engine.dispose()
            
            if self.engine:
                self.engine.dispose()
            
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")
    
    async def execute_query(self, query: str, params: dict = None):
        """Execute a query and return results as list of dictionaries"""
        try:
            async with self.async_engine.begin() as conn:
                if params:
                    result = await conn.execute(text(query), params)
                else:
                    result = await conn.execute(text(query))
                
                # Check if query returns rows (SELECT statements)
                if result.returns_rows:
                    # Convert rows to dictionaries
                    rows = result.fetchall()
                    if rows:
                        return [dict(row._mapping) for row in rows]
                    return []
                else:
                    # For INSERT/UPDATE/DELETE statements, return affected row count
                    return {"affected_rows": result.rowcount}
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            logger.error(f"Query was: {query}")
            logger.error(f"Parameters: {params}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise
    
    @asynccontextmanager
    async def get_async_session(self):
        """Get async database session with automatic cleanup"""
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    def get_sync_session(self) -> Session:
        """Get synchronous database session"""
        return self.session_factory()
    
    async def initialize_date_dimension(self):
        """Populate date dimension table"""
        async with self.get_async_session() as session:
            # Check if already populated
            result = await session.execute(text("SELECT COUNT(*) FROM warehouse.dim_date"))
            count = result.scalar()
            
            if count > 0:
                logger.info("Date dimension already populated")
                return
            
            logger.info("Populating date dimension...")
            
            # Generate dates for 5 years (past 2 years + current + future 2 years)
            start_date = datetime.now().replace(month=1, day=1) - timedelta(days=730)
            end_date = datetime.now().replace(month=12, day=31) + timedelta(days=730)
            
            current_date = start_date
            date_records = []
            
            while current_date <= end_date:
                date_record = DimDate(
                    date_id=int(current_date.strftime('%Y%m%d')),
                    date_value=current_date,
                    year=current_date.year,
                    quarter=(current_date.month - 1) // 3 + 1,
                    month=current_date.month,
                    week=current_date.isocalendar()[1],
                    day=current_date.day,
                    day_of_week=current_date.weekday() + 1,
                    day_name=current_date.strftime('%A'),
                    month_name=current_date.strftime('%B'),
                    is_weekend=current_date.weekday() >= 5
                )
                date_records.append(date_record)
                current_date += timedelta(days=1)
            
            session.add_all(date_records)
            await session.commit()
            
            logger.info(f"Date dimension populated with {len(date_records)} records")
    
    async def create_data_source(self, source_name: str, source_type: str, 
                                source_format: str = None, source_url: str = None,
                                description: str = None) -> str:
        """Create a new data source record"""
        async with self.get_async_session() as session:
            data_source = DimDataSource(
                source_name=source_name,
                source_type=source_type,
                source_format=source_format,
                source_url=source_url,
                source_description=description
            )
            
            session.add(data_source)
            await session.commit()
            await session.refresh(data_source)
            
            logger.info(f"Created data source: {source_name} ({data_source.source_id})")
            return str(data_source.source_id)
    
    async def create_data_schema(self, schema_name: str, schema_version: str,
                                schema_definition: Dict[str, Any]) -> str:
        """Create a new data schema record"""
        async with self.get_async_session() as session:
            # Create schema hash for comparison
            schema_json = json.dumps(schema_definition, sort_keys=True)
            schema_hash = hashlib.md5(schema_json.encode()).hexdigest()
            
            data_schema = DimDataSchema(
                schema_name=schema_name,
                schema_version=schema_version,
                field_count=len(schema_definition.get('fields', {})),
                schema_definition=schema_definition,
                schema_hash=schema_hash
            )
            
            session.add(data_schema)
            await session.commit()
            await session.refresh(data_schema)
            
            logger.info(f"Created data schema: {schema_name} v{schema_version}")
            return str(data_schema.schema_id)
    
    async def create_processing_job(self, job_name: str, job_type: str,
                                   source_id: str, schema_id: str = None,
                                   job_config: Dict[str, Any] = None) -> str:
        """Create a new processing job record"""
        async with self.get_async_session() as session:
            # Get current date ID
            today = datetime.now().date()
            date_id = int(today.strftime('%Y%m%d'))
            
            processing_job = FactProcessingJob(
                job_name=job_name,
                job_type=job_type,
                source_id=source_id,
                schema_id=schema_id,
                start_date_id=date_id,
                status='pending',
                started_at=datetime.now(),
                job_config=job_config or {}
            )
            
            session.add(processing_job)
            await session.commit()
            await session.refresh(processing_job)
            
            logger.info(f"Created processing job: {job_name} ({processing_job.job_id})")
            return str(processing_job.job_id)
    
    async def update_job_status(self, job_id: str, status: str, 
                               error_message: str = None, 
                               metrics: Dict[str, Any] = None):
        """Update processing job status and metrics"""
        async with self.get_async_session() as session:
            query = text("""
                UPDATE warehouse.fact_processing_job 
                SET status = :status,
                    completed_at = CASE WHEN :status IN ('completed', 'failed') THEN NOW() ELSE completed_at END,
                    error_message = :error_message,
                    total_records = COALESCE(:total_records, total_records),
                    valid_records = COALESCE(:valid_records, valid_records),
                    invalid_records = COALESCE(:invalid_records, invalid_records),
                    quarantined_records = COALESCE(:quarantined_records, quarantined_records),
                    avg_quality_score = COALESCE(:avg_quality_score, avg_quality_score),
                    success_rate = COALESCE(:success_rate, success_rate),
                    processing_duration_seconds = COALESCE(:processing_duration, processing_duration_seconds),
                    throughput_records_per_second = COALESCE(:throughput, throughput_records_per_second),
                    memory_usage_mb = COALESCE(:memory_usage, memory_usage_mb)
                WHERE job_id = :job_id
            """)
            
            params = {
                'job_id': job_id,
                'status': status,
                'error_message': error_message,
                'total_records': metrics.get('total_records') if metrics else None,
                'valid_records': metrics.get('valid_records') if metrics else None,
                'invalid_records': metrics.get('invalid_records') if metrics else None,
                'quarantined_records': metrics.get('quarantined_records') if metrics else None,
                'avg_quality_score': metrics.get('avg_quality_score') if metrics else None,
                'success_rate': metrics.get('success_rate') if metrics else None,
                'processing_duration': metrics.get('processing_duration_seconds') if metrics else None,
                'throughput': metrics.get('throughput_records_per_second') if metrics else None,
                'memory_usage': metrics.get('memory_usage_mb') if metrics else None
            }
            
            await session.execute(query, params)
            await session.commit()
    
    async def store_validation_results(self, job_id: str, 
                                     validation_results: List[Dict[str, Any]]):
        """Store validation results for a processing job"""
        async with self.get_async_session() as session:
            validation_records = []
            
            for i, result in enumerate(validation_results):
                validation_record = FactValidationResult(
                    job_id=job_id,
                    record_id=i,
                    is_valid=result.get('is_valid', False),
                    quality_score=result.get('quality_score', 0.0),
                    quality_level=self._determine_quality_level(result.get('quality_score', 0.0)),
                    field_scores=result.get('field_scores', {}),
                    issue_count=len(result.get('issues', [])),
                    warning_count=len(result.get('warnings', [])),
                    issues=result.get('issues', []),
                    warnings=result.get('warnings', []),
                    original_data=result.get('original_data', {}),
                    processed_data=result.get('processed_data', {})
                )
                validation_records.append(validation_record)
            
            session.add_all(validation_records)
            await session.commit()
            
            logger.info(f"Stored {len(validation_records)} validation results for job {job_id}")

    async def bulk_store_validation_results(self, job_id: str, validation_results, chunk_size: int = 5000):
        """High-performance bulk insert of validation results using Core executemany.

        Expects each item to be an EnhancedValidationResult (or similar) with attributes:
          is_valid, quality_score, quality_level, field_scores, errors, warnings, original_data, validated_data
        """
        from database.models import FactValidationResult  # local import to avoid circular
        total = 0
        async with self.get_async_session() as session:
            table = FactValidationResult.__table__
            batch = []
            for idx, result in enumerate(validation_results):
                batch.append({
                    'result_id': None,  # default generated
                    'job_id': job_id,
                    'record_id': idx,
                    'is_valid': getattr(result, 'is_valid', False),
                    'quality_score': float(getattr(result, 'quality_score', 0.0) or 0.0),
                    'quality_level': str(getattr(result, 'quality_level', 'poor')).lower(),
                    'field_scores': getattr(result, 'field_scores', {}) or {},
                    'issue_count': len(getattr(result, 'errors', []) or []),
                    'warning_count': len(getattr(result, 'warnings', []) or []),
                    'issues': getattr(result, 'errors', []) or [],
                    'warnings': getattr(result, 'warnings', []) or [],
                    'original_data': getattr(result, 'original_data', {}) or {},
                    'processed_data': getattr(result, 'validated_data', None) or {},
                    'validated_at': datetime.now()
                })
                if len(batch) >= chunk_size:
                    await session.execute(table.insert(), batch)
                    await session.commit()
                    total += len(batch)
                    logger.info(f"Bulk inserted {total} validation rows (chunk size {chunk_size}) for job {job_id}")
                    batch.clear()
            if batch:
                await session.execute(table.insert(), batch)
                await session.commit()
                total += len(batch)
            logger.info(f"Bulk insert completed: {total} validation rows stored for job {job_id}")
    
    def _determine_quality_level(self, quality_score: float) -> str:
        """Determine quality level based on score"""
        if quality_score >= 0.9:
            return 'excellent'
        elif quality_score >= 0.8:
            return 'good'
        elif quality_score >= 0.6:
            return 'fair'
        else:
            return 'poor'
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get processing job status and metrics"""
        query = """
            SELECT 
                job_id, job_name, job_type, status, started_at, completed_at,
                total_records, valid_records, invalid_records, quarantined_records,
                avg_quality_score, success_rate, processing_duration_seconds,
                error_message
            FROM warehouse.fact_processing_job 
            WHERE job_id = :job_id
        """
        
        result = await self.database.fetch_one(query=query, values={'job_id': job_id})
        
        if result:
            return dict(result)
        return None
    
    async def get_recent_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent processing jobs"""
        query = """
            SELECT 
                pj.job_id, pj.job_name, pj.job_type, pj.status, pj.started_at, pj.completed_at,
                pj.total_records, pj.valid_records, pj.invalid_records, pj.avg_quality_score,
                ds.source_name, ds.source_type
            FROM warehouse.fact_processing_job pj
            LEFT JOIN warehouse.dim_data_source ds ON pj.source_id = ds.source_id
            ORDER BY pj.started_at DESC
            LIMIT :limit
        """
        
        results = await self.database.fetch_all(query=query, values={'limit': limit})
        return [dict(result) for result in results]
    
    async def cache_set(self, key: str, value: Any, expire: int = 3600):
        """Set value in Redis cache"""
        if self.redis_client:
            await self.redis_client.setex(key, expire, json.dumps(value))
    
    async def cache_get(self, key: str) -> Any:
        """Get value from Redis cache"""
        if self.redis_client:
            cached_value = await self.redis_client.get(key)
            if cached_value:
                return json.loads(cached_value)
        return None


# Global database service instance
db_service = DatabaseService()
_db_unavailable = False  # Flag to track if database is unavailable


async def get_database_service() -> DatabaseService:
    """Get the global database service instance"""
    global _db_unavailable
    
    # If we already know the database is unavailable, don't try again
    if _db_unavailable:
        raise Exception("Database service is unavailable")
    
    if not db_service._initialized:
        try:
            await db_service.initialize()
        except Exception as e:
            # Mark database as unavailable to avoid repeated connection attempts
            _db_unavailable = True
            logger.warning("Database marked as unavailable, API will run in file-only mode")
            raise e
    return db_service


def reset_database_availability():
    """Reset the database availability flag to allow retry"""
    global _db_unavailable
    _db_unavailable = False
    logger.info("Database availability flag reset - will retry connection on next request")
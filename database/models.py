"""
SQLAlchemy database models for Data Warehouse
Implements dimensional modeling with fact and dimension tables
"""

from sqlalchemy import (
    Column, Integer, String, DateTime, Float, Boolean, Text, JSON, 
    ForeignKey, Index, UniqueConstraint, CheckConstraint, BigInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session
from sqlalchemy.dialects.postgresql import UUID, ENUM, ARRAY
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional, Dict, Any, List
import uuid

Base = declarative_base()

# Enums for PostgreSQL
data_quality_level_enum = ENUM(
    'excellent', 'good', 'fair', 'poor',
    name='data_quality_level',
    create_type=False
)

processing_status_enum = ENUM(
    'pending', 'processing', 'completed', 'failed', 'quarantined',
    name='processing_status',
    create_type=False
)

validation_issue_type_enum = ENUM(
    'missing_value', 'invalid_format', 'out_of_range', 
    'inconsistent_pattern', 'duplicate_value', 'outlier', 
    'encoding_issue', 'length_violation',
    name='validation_issue_type',
    create_type=False
)


class DimDate(Base):
    """Date dimension table for time-based analysis"""
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'warehouse'}
    
    date_id = Column(Integer, primary_key=True)
    date_value = Column(DateTime, unique=True, nullable=False)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String(10), nullable=False)
    month_name = Column(String(10), nullable=False)
    is_weekend = Column(Boolean, nullable=False)
    is_holiday = Column(Boolean, default=False)
    
    created_at = Column(DateTime, default=func.now())


class DimDataSource(Base):
    """Data source dimension for tracking data origins"""
    __tablename__ = 'dim_data_source'
    __table_args__ = {'schema': 'warehouse'}
    
    source_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_name = Column(String(255), nullable=False)
    source_type = Column(String(50), nullable=False)  # file, api, stream
    source_format = Column(String(50))  # csv, json, excel, etc.
    source_url = Column(Text)
    source_description = Column(Text)
    
    # Metadata
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    
    # Relationships
    processing_jobs = relationship("FactProcessingJob", back_populates="data_source")


class DimDataSchema(Base):
    """Schema dimension for tracking data structures"""
    __tablename__ = 'dim_data_schema'
    __table_args__ = {'schema': 'warehouse'}
    
    schema_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    schema_name = Column(String(255), nullable=False)
    schema_version = Column(String(50), nullable=False)
    field_count = Column(Integer, nullable=False)
    schema_definition = Column(JSON, nullable=False)
    schema_hash = Column(String(64), nullable=False)  # MD5 hash for quick comparison
    
    # Quality metrics
    avg_quality_score = Column(Float)
    completeness_score = Column(Float)
    consistency_score = Column(Float)
    
    created_at = Column(DateTime, default=func.now())
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('schema_name', 'schema_version'),
        {'schema': 'warehouse'}
    )


class FactProcessingJob(Base):
    """Fact table for data processing jobs"""
    __tablename__ = 'fact_processing_job'
    __table_args__ = {'schema': 'warehouse'}
    
    job_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Dimension foreign keys
    source_id = Column(UUID(as_uuid=True), ForeignKey('warehouse.dim_data_source.source_id'))
    schema_id = Column(UUID(as_uuid=True), ForeignKey('warehouse.dim_data_schema.schema_id'))
    start_date_id = Column(Integer, ForeignKey('warehouse.dim_date.date_id'))
    end_date_id = Column(Integer, ForeignKey('warehouse.dim_date.date_id'))
    
    # Job details
    job_name = Column(String(255))
    job_type = Column(String(50), nullable=False)  # validation, kafka, pipeline, streaming
    status = Column(processing_status_enum, nullable=False)
    
    # Metrics (measures)
    total_records = Column(BigInteger, default=0)
    valid_records = Column(BigInteger, default=0)
    invalid_records = Column(BigInteger, default=0)
    quarantined_records = Column(BigInteger, default=0)
    
    # Quality scores
    avg_quality_score = Column(Float)
    success_rate = Column(Float)
    
    # Performance metrics
    processing_duration_seconds = Column(Float)
    throughput_records_per_second = Column(Float)
    memory_usage_mb = Column(Float)
    
    # Timestamps
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=func.now())
    
    # Error information
    error_message = Column(Text)
    error_details = Column(JSON)
    
    # Configuration
    job_config = Column(JSON)
    
    # Relationships
    data_source = relationship("DimDataSource", back_populates="processing_jobs")
    validation_results = relationship("FactValidationResult", back_populates="processing_job")
    quality_metrics = relationship("FactQualityMetric", back_populates="processing_job")


class FactValidationResult(Base):
    """Fact table for individual record validation results"""
    __tablename__ = 'fact_validation_result'
    __table_args__ = {'schema': 'warehouse'}
    
    result_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey('warehouse.fact_processing_job.job_id'))
    record_id = Column(BigInteger, nullable=False)
    
    # Validation outcome
    is_valid = Column(Boolean, nullable=False)
    quality_score = Column(Float, nullable=False)
    quality_level = Column(data_quality_level_enum, nullable=False)
    
    # Field-level scores
    field_scores = Column(JSON)  # {field_name: score}
    
    # Issues and warnings
    issue_count = Column(Integer, default=0)
    warning_count = Column(Integer, default=0)
    issues = Column(JSON)  # Array of issue details
    warnings = Column(JSON)  # Array of warning messages
    
    # Original and processed data
    original_data = Column(JSON)
    processed_data = Column(JSON)
    
    # Timestamps
    validated_at = Column(DateTime, default=func.now())
    
    # Relationships
    processing_job = relationship("FactProcessingJob", back_populates="validation_results")
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_validation_job_id', 'job_id'),
        Index('idx_validation_quality', 'quality_score'),
        Index('idx_validation_valid', 'is_valid'),
        {'schema': 'warehouse'}
    )


class FactQualityMetric(Base):
    """Fact table for detailed quality metrics"""
    __tablename__ = 'fact_quality_metric'
    __table_args__ = {'schema': 'warehouse'}
    
    metric_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey('warehouse.fact_processing_job.job_id'))
    
    field_name = Column(String(255), nullable=False)
    metric_type = Column(String(50), nullable=False)  # completeness, validity, consistency, etc.
    metric_value = Column(Float, nullable=False)
    metric_details = Column(JSON)
    
    # Field profile information
    data_type = Column(String(50))
    total_values = Column(BigInteger)
    null_count = Column(BigInteger)
    unique_count = Column(BigInteger)
    
    # Statistical measures
    min_value = Column(Float)
    max_value = Column(Float)
    mean_value = Column(Float)
    std_dev = Column(Float)
    
    # String metrics
    min_length = Column(Integer)
    max_length = Column(Integer)
    avg_length = Column(Float)
    
    # Categorical metrics
    top_categories = Column(JSON)  # Top N categories with frequencies
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    processing_job = relationship("FactProcessingJob", back_populates="quality_metrics")
    
    # Indexes
    __table_args__ = (
        Index('idx_quality_job_field', 'job_id', 'field_name'),
        Index('idx_quality_metric_type', 'metric_type'),
        {'schema': 'warehouse'}
    )


class StagingData(Base):
    """Staging table for raw ingested data"""
    __tablename__ = 'staging_data'
    __table_args__ = {'schema': 'staging'}
    
    staging_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), nullable=False)
    record_id = Column(BigInteger, nullable=False)
    
    # Raw data
    raw_data = Column(JSON, nullable=False)
    processed_data = Column(JSON)
    
    # Status tracking
    status = Column(processing_status_enum, default='pending')
    error_message = Column(Text)
    
    # Timestamps
    ingested_at = Column(DateTime, default=func.now())
    processed_at = Column(DateTime)
    
    # Partitioning by date for performance
    partition_date = Column(DateTime, default=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_staging_job_id', 'job_id'),
        Index('idx_staging_status', 'status'),
        Index('idx_staging_partition', 'partition_date'),
        {'schema': 'staging'}
    )


class QuarantineData(Base):
    """Quarantine table for problematic data"""
    __tablename__ = 'quarantine_data'
    __table_args__ = {'schema': 'warehouse'}
    
    quarantine_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), nullable=False)
    record_id = Column(BigInteger, nullable=False)
    
    # Data and issues
    original_data = Column(JSON, nullable=False)
    issues = Column(JSON, nullable=False)  # Array of validation issues
    severity_score = Column(Float, nullable=False)
    
    # Classification
    quarantine_reason = Column(String(255), nullable=False)
    issue_types = Column(ARRAY(String))
    
    # Resolution tracking
    status = Column(String(50), default='quarantined')  # quarantined, reviewed, resolved, rejected
    reviewed_by = Column(String(255))
    reviewed_at = Column(DateTime)
    resolution_notes = Column(Text)
    
    # Timestamps
    quarantined_at = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_quarantine_job_id', 'job_id'),
        Index('idx_quarantine_severity', 'severity_score'),
        Index('idx_quarantine_status', 'status'),
        {'schema': 'warehouse'}
    )


class DataLineage(Base):
    """Track data lineage and transformations"""
    __tablename__ = 'data_lineage'
    __table_args__ = {'schema': 'warehouse'}
    
    lineage_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_job_id = Column(UUID(as_uuid=True))
    target_job_id = Column(UUID(as_uuid=True))
    
    transformation_type = Column(String(100), nullable=False)
    transformation_details = Column(JSON)
    
    # Data flow metrics
    records_in = Column(BigInteger)
    records_out = Column(BigInteger)
    transformation_duration = Column(Float)
    
    created_at = Column(DateTime, default=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_lineage_source', 'source_job_id'),
        Index('idx_lineage_target', 'target_job_id'),
        {'schema': 'warehouse'}
    )


# Analytics Views (implemented as materialized views)
class AnalyticsJobSummary(Base):
    """Analytics view for job performance summary"""
    __tablename__ = 'analytics_job_summary'
    __table_args__ = {'schema': 'analytics'}
    
    summary_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    date_period = Column(DateTime, nullable=False)
    period_type = Column(String(20), nullable=False)  # daily, weekly, monthly
    
    # Aggregated metrics
    total_jobs = Column(Integer)
    successful_jobs = Column(Integer)
    failed_jobs = Column(Integer)
    avg_processing_time = Column(Float)
    total_records_processed = Column(BigInteger)
    avg_quality_score = Column(Float)
    
    created_at = Column(DateTime, default=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_analytics_period', 'date_period', 'period_type'),
        {'schema': 'analytics'}
    )
"""
ETL Service for Data Warehouse Operations
Handles Extract, Transform, Load operations for data ingestion pipeline
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
import pandas as pd
from pathlib import Path

from services.database_service import DatabaseService, get_database_service
from services.data_validator import DataValidator
from services.intelligent_validator import DynamicDataValidator, FieldProfile
from services.file_processor import FileProcessor
from services.dynamic_schema_service import DynamicSchemaService, get_dynamic_schema_service
from database.models import (
    FactProcessingJob, FactValidationResult, FactQualityMetric,
    StagingData, QuarantineData
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataWarehouseETL:
    """ETL service for data warehouse operations with dynamic schema support"""
    
    def __init__(self, db_service: DatabaseService = None, dynamic_schema_service: DynamicSchemaService = None):
        self.db_service = db_service
        self.dynamic_schema_service = dynamic_schema_service
        self.file_processor = FileProcessor()
        self.data_validator = DataValidator()
        self.dynamic_validator = DynamicDataValidator()
    
    async def initialize(self):
        """Initialize ETL service"""
        if not self.db_service:
            self.db_service = await get_database_service()
        if not self.dynamic_schema_service:
            self.dynamic_schema_service = await get_dynamic_schema_service()
        logger.info("ETL service initialized with dynamic schema support")
    
    async def process_file_to_warehouse(self, 
                                       file_path: str,
                                       source_name: str,
                                       job_name: str = None,
                                       quality_threshold: float = 0.8) -> Dict[str, Any]:
        """
        Complete ETL process: Extract data from file, Transform and validate, Load to warehouse
        
        Args:
            file_path: Path to the data file
            source_name: Name of the data source
            job_name: Optional job name
            quality_threshold: Minimum quality threshold for data acceptance
            
        Returns:
            Dict containing processing results and metrics
        """
        logger.info(f"Starting ETL process for file: {file_path}")
        start_time = datetime.now()
        
        try:
            # Step 1: Extract - Create data source and schema
            source_id = await self._create_or_get_data_source(file_path, source_name)
            
            # Step 2: Extract - Load data from file
            df = self.file_processor.load_data_from_file(file_path)
            records = self.file_processor.convert_dataframe_to_records(df, clean_data=True)
            
            # Step 3: Transform - Learn schema and validate
            schema = await self._learn_and_store_schema(df, source_name)
            schema_id = schema['schema_id']
            
            # Step 4: Create processing job
            job_name = job_name or f"ETL_{Path(file_path).stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            job_id = await self.db_service.create_processing_job(
                job_name=job_name,
                job_type="etl_file",
                source_id=source_id,
                schema_id=schema_id,
                job_config={
                    "file_path": file_path,
                    "quality_threshold": quality_threshold,
                    "total_records": len(records)
                }
            )
            
            # Update job status to processing
            await self.db_service.update_job_status(job_id, "processing")
            
            # Step 5: Transform - Validate all records
            validation_results = await self._validate_and_stage_data(job_id, records, quality_threshold)
            
            # Step 6: Load - Store results in warehouse
            warehouse_results = await self._load_to_warehouse(job_id, validation_results, schema['profiles'])
            
            # Step 7: Calculate final metrics
            processing_duration = (datetime.now() - start_time).total_seconds()
            final_metrics = self._calculate_processing_metrics(
                validation_results, processing_duration, len(records)
            )
            
            # Update job with final status and metrics
            await self.db_service.update_job_status(
                job_id, "completed", metrics=final_metrics
            )
            
            logger.info(f"ETL process completed for job {job_id}")
            
            return {
                "job_id": job_id,
                "status": "completed",
                "metrics": final_metrics,
                "schema_info": schema,
                "warehouse_results": warehouse_results
            }
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            if 'job_id' in locals():
                await self.db_service.update_job_status(
                    job_id, "failed", error_message=str(e)
                )
            raise
    
    async def process_api_stream_to_warehouse(self,
                                            api_url: str,
                                            source_name: str,
                                            job_name: str = None,
                                            quality_threshold: float = 0.8) -> Dict[str, Any]:
        """
        ETL process for API streaming data
        
        Args:
            api_url: API endpoint URL
            source_name: Name of the data source
            job_name: Optional job name
            quality_threshold: Minimum quality threshold
            
        Returns:
            Dict containing processing results
        """
        logger.info(f"Starting ETL process for API stream: {api_url}")
        start_time = datetime.now()
        
        try:
            # Import API streaming service
            from services.api_streaming_service import ApiStreamingService
            
            # Step 1: Create data source
            source_id = await self.db_service.create_data_source(
                source_name=source_name,
                source_type="api",
                source_format="json",
                source_url=api_url,
                description=f"API stream from {api_url}"
            )
            
            # Step 2: Stream data from API
            async with ApiStreamingService() as streaming_service:
                raw_data = await streaming_service._stream_api_data(api_url, timeout_seconds=300)
            
            # Convert to DataFrame for schema learning
            df = pd.DataFrame(raw_data)
            records = df.to_dict('records')
            
            # Step 3: Learn schema and create processing job
            schema = await self._learn_and_store_schema(df, source_name)
            schema_id = schema['schema_id']
            
            job_name = job_name or f"ETL_API_{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            job_id = await self.db_service.create_processing_job(
                job_name=job_name,
                job_type="etl_api_stream",
                source_id=source_id,
                schema_id=schema_id,
                job_config={
                    "api_url": api_url,
                    "quality_threshold": quality_threshold,
                    "total_records": len(records)
                }
            )
            
            await self.db_service.update_job_status(job_id, "processing")
            
            # Step 4: Validate and load data
            validation_results = await self._validate_and_stage_data(job_id, records, quality_threshold)
            warehouse_results = await self._load_to_warehouse(job_id, validation_results, schema['profiles'])
            
            # Step 5: Finalize
            processing_duration = (datetime.now() - start_time).total_seconds()
            final_metrics = self._calculate_processing_metrics(
                validation_results, processing_duration, len(records)
            )
            
            await self.db_service.update_job_status(
                job_id, "completed", metrics=final_metrics
            )
            
            logger.info(f"API stream ETL completed for job {job_id}")
            
            return {
                "job_id": job_id,
                "status": "completed",
                "metrics": final_metrics,
                "schema_info": schema,
                "warehouse_results": warehouse_results
            }
            
        except Exception as e:
            logger.error(f"API stream ETL failed: {str(e)}")
            if 'job_id' in locals():
                await self.db_service.update_job_status(
                    job_id, "failed", error_message=str(e)
                )
            raise
    
    async def _create_or_get_data_source(self, file_path: str, source_name: str) -> str:
        """Create or retrieve data source"""
        file_path_obj = Path(file_path)
        source_format = file_path_obj.suffix.lower().lstrip('.')
        
        source_id = await self.db_service.create_data_source(
            source_name=source_name,
            source_type="file",
            source_format=source_format,
            source_url=str(file_path_obj.absolute()),
            description=f"File source: {file_path_obj.name}"
        )
        
        return source_id
    
    async def _learn_and_store_schema(self, df: pd.DataFrame, source_name: str) -> Dict[str, Any]:
        """Learn schema from DataFrame and store in warehouse"""
        logger.info("Learning data schema...")
        
        # Learn schema using intelligent validator
        schema_profiles = self.dynamic_validator.inference_engine.infer_schema(df)
        
        # Convert field profiles to serializable format
        schema_definition = {
            "fields": {},
            "metadata": {
                "total_records": len(df),
                "total_fields": len(df.columns),
                "learned_at": datetime.now().isoformat()
            }
        }
        
        for field_name, profile in schema_profiles.items():
            schema_definition["fields"][field_name] = {
                "name": profile.name,
                "detected_type": profile.detected_type.value,
                "nullable": profile.nullable,
                "unique_values": profile.unique_values,
                "total_values": profile.total_values,
                "null_count": profile.null_count,
                "quality_score": profile.overall_quality_score,
                "validation_rules": profile.validation_rules,
                "sample_values": profile.sample_values[:5]  # Store first 5 samples
            }
        
        # Create schema version based on timestamp
        schema_version = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Store schema in warehouse
        schema_id = await self.db_service.create_data_schema(
            schema_name=source_name,
            schema_version=schema_version,
            schema_definition=schema_definition
        )
        
        logger.info(f"Schema stored with ID: {schema_id}")
        
        return {
            "schema_id": schema_id,
            "schema_definition": schema_definition,
            "profiles": schema_profiles
        }
    
    async def _validate_and_stage_data(self, 
                                     job_id: str, 
                                     records: List[Dict[str, Any]], 
                                     quality_threshold: float) -> List[Dict[str, Any]]:
        """Validate records and stage data"""
        logger.info(f"Validating {len(records)} records...")
        
        # Set the schema for the validator
        self.data_validator.dynamic_validator = self.dynamic_validator
        
        # Validate all records
        validation_results = []
        staging_records = []
        quarantine_records = []
        
        for i, record in enumerate(records):
            try:
                # Validate record
                result = self.data_validator.validate_record(record, source_file=f"job_{job_id}")
                
                validation_data = {
                    "record_id": i,
                    "is_valid": result.is_valid,
                    "quality_score": result.quality_score,
                    "field_scores": result.field_scores,
                    "issues": [str(error) for error in result.errors],
                    "warnings": result.warnings,
                    "original_data": record,
                    "processed_data": result.validated_data or record
                }
                
                validation_results.append(validation_data)
                
                # Stage data based on quality
                if result.quality_score >= quality_threshold:
                    staging_records.append({
                        "job_id": job_id,
                        "record_id": i,
                        "raw_data": record,
                        "processed_data": result.validated_data or record,
                        "status": "completed" if result.is_valid else "pending"
                    })
                else:
                    # Quarantine low quality data
                    quarantine_records.append({
                        "job_id": job_id,
                        "record_id": i,
                        "original_data": record,
                        "issues": validation_data["issues"],
                        "severity_score": 1.0 - result.quality_score,
                        "quarantine_reason": f"Quality score {result.quality_score:.2f} below threshold {quality_threshold}"
                    })
                
                if (i + 1) % 1000 == 0:
                    logger.info(f"Validated {i + 1}/{len(records)} records")
                    
            except Exception as e:
                logger.error(f"Error validating record {i}: {str(e)}")
                # Add to quarantine with error
                quarantine_records.append({
                    "job_id": job_id,
                    "record_id": i,
                    "original_data": record,
                    "issues": [f"Validation error: {str(e)}"],
                    "severity_score": 1.0,
                    "quarantine_reason": "Validation exception"
                })
        
        # Bulk insert staging data
        if staging_records:
            await self._bulk_insert_staging_data(staging_records)
        
        # Bulk insert quarantine data
        if quarantine_records:
            await self._bulk_insert_quarantine_data(quarantine_records)
        
        logger.info(f"Validation completed: {len(staging_records)} staged, {len(quarantine_records)} quarantined")
        
        return validation_results
    
    async def _bulk_insert_staging_data(self, staging_records: List[Dict[str, Any]]):
        """Bulk insert staging data"""
        async with self.db_service.get_async_session() as session:
            staging_objects = []
            for record in staging_records:
                staging_obj = StagingData(
                    job_id=record["job_id"],
                    record_id=record["record_id"],
                    raw_data=record["raw_data"],
                    processed_data=record["processed_data"],
                    status=record["status"]
                )
                staging_objects.append(staging_obj)
            
            session.add_all(staging_objects)
            await session.commit()
            
        logger.info(f"Bulk inserted {len(staging_objects)} staging records")
    
    async def _bulk_insert_quarantine_data(self, quarantine_records: List[Dict[str, Any]]):
        """Bulk insert quarantine data"""
        async with self.db_service.get_async_session() as session:
            quarantine_objects = []
            for record in quarantine_records:
                quarantine_obj = QuarantineData(
                    job_id=record["job_id"],
                    record_id=record["record_id"],
                    original_data=record["original_data"],
                    issues=record["issues"],
                    severity_score=record["severity_score"],
                    quarantine_reason=record["quarantine_reason"],
                    issue_types=[issue.split(":")[0] for issue in record["issues"]]  # Extract issue types
                )
                quarantine_objects.append(quarantine_obj)
            
            session.add_all(quarantine_objects)
            await session.commit()
            
        logger.info(f"Bulk inserted {len(quarantine_objects)} quarantine records")
    
    async def _load_to_warehouse(self, 
                               job_id: str, 
                               validation_results: List[Dict[str, Any]], 
                               field_profiles: Dict[str, FieldProfile]) -> Dict[str, Any]:
        """Load validation results and quality metrics to warehouse"""
        logger.info("Loading results to data warehouse...")
        
        # Store validation results
        await self.db_service.store_validation_results(job_id, validation_results)
        
        # Store quality metrics
        await self._store_quality_metrics(job_id, field_profiles, validation_results)
        
        # Calculate and return warehouse load results
        total_loaded = len(validation_results)
        valid_loaded = sum(1 for r in validation_results if r["is_valid"])
        
        warehouse_results = {
            "total_records_loaded": total_loaded,
            "valid_records_loaded": valid_loaded,
            "invalid_records_loaded": total_loaded - valid_loaded,
            "quality_metrics_stored": len(field_profiles),
            "load_timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Warehouse load completed: {warehouse_results}")
        return warehouse_results
    
    async def _store_quality_metrics(self, 
                                   job_id: str, 
                                   field_profiles: Dict[str, FieldProfile],
                                   validation_results: List[Dict[str, Any]]):
        """Store detailed quality metrics"""
        async with self.db_service.get_async_session() as session:
            quality_metrics = []
            
            for field_name, profile in field_profiles.items():
                # Calculate field-specific metrics from validation results
                field_scores = [r.get("field_scores", {}).get(field_name, 0.0) for r in validation_results]
                avg_field_score = sum(field_scores) / len(field_scores) if field_scores else 0.0
                
                # Create quality metric record
                quality_metric = FactQualityMetric(
                    job_id=job_id,
                    field_name=field_name,
                    metric_type="comprehensive",
                    metric_value=avg_field_score,
                    metric_details={
                        "completeness_score": profile.completeness_score,
                        "consistency_score": profile.consistency_score,
                        "validity_score": profile.validity_score,
                        "overall_quality_score": profile.overall_quality_score
                    },
                    data_type=profile.detected_type.value,
                    total_values=profile.total_values,
                    null_count=profile.null_count,
                    unique_count=profile.unique_values,
                    min_value=profile.min_value,
                    max_value=profile.max_value,
                    mean_value=profile.mean_value,
                    std_dev=profile.std_value,
                    min_length=profile.min_length,
                    max_length=profile.max_length,
                    avg_length=profile.avg_length,
                    top_categories=profile.category_frequencies if profile.is_categorical else None
                )
                
                quality_metrics.append(quality_metric)
            
            session.add_all(quality_metrics)
            await session.commit()
            
        logger.info(f"Stored {len(quality_metrics)} quality metrics")
    
    def _calculate_processing_metrics(self, 
                                    validation_results: List[Dict[str, Any]], 
                                    processing_duration: float, 
                                    total_records: int) -> Dict[str, Any]:
        """Calculate final processing metrics"""
        valid_count = sum(1 for r in validation_results if r["is_valid"])
        invalid_count = total_records - valid_count
        quarantined_count = sum(1 for r in validation_results if r["quality_score"] < 0.6)
        
        avg_quality_score = sum(r["quality_score"] for r in validation_results) / len(validation_results) if validation_results else 0.0
        success_rate = valid_count / total_records if total_records > 0 else 0.0
        throughput = total_records / processing_duration if processing_duration > 0 else 0.0
        
        return {
            "total_records": total_records,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "quarantined_records": quarantined_count,
            "avg_quality_score": avg_quality_score,
            "success_rate": success_rate,
            "processing_duration_seconds": processing_duration,
            "throughput_records_per_second": throughput,
            "memory_usage_mb": 0.0  # TODO: Implement memory tracking
        }

    async def process_with_dynamic_schema(self,
                                        data_records: List[Dict[str, Any]],
                                        source_name: str,
                                        source_type: str = 'file',
                                        job_id: str = None,
                                        storage_method: str = 'flexible') -> Dict[str, Any]:
        """
        Process data with dynamic schema detection and storage
        
        Args:
            data_records: List of data records to process
            source_name: Name of the data source (file name, API endpoint, etc.)
            source_type: Type of source ('file', 'api', 'stream')
            job_id: Optional job ID for tracking
            storage_method: 'flexible' (JSONB) or 'structured' (dynamic tables)
            
        Returns:
            Dict containing processing results with schema information
        """
        logger.info(f"Processing {len(data_records)} records with dynamic schema detection")
        
        try:
            # Step 1: Infer schema from data
            schema_name = f"{source_name}_{source_type}"
            schema_definition = await self._infer_dynamic_schema(data_records, schema_name)
            
            # Step 2: Register schema in dynamic schema service
            schema_id = self.dynamic_schema_service.register_schema(
                schema_name=schema_name,
                schema_definition=schema_definition,
                source_type=source_type,
                sample_data=data_records[:5]  # Store first 5 samples
            )
            
            # Step 3: Validate data using inferred schema
            validation_results = await self._validate_against_dynamic_schema(
                data_records, schema_definition
            )
            
            # Step 4: Store data using chosen method
            if storage_method == 'structured':
                storage_result = self.dynamic_schema_service.store_data_structured(
                    schema_id=schema_id,
                    data_records=data_records,
                    job_id=job_id or f"dynamic_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    source_file=source_name,
                    validation_results={'record_results': validation_results}
                )
            else:
                storage_result = self.dynamic_schema_service.store_data_flexible(
                    schema_id=schema_id,
                    data_records=data_records,
                    job_id=job_id or f"dynamic_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    source_file=source_name,
                    validation_results={'record_results': validation_results}
                )
            
            # Step 5: Calculate quality metrics
            quality_metrics = self._calculate_dynamic_quality_metrics(
                data_records, validation_results, schema_definition
            )
            
            return {
                'schema_id': schema_id,
                'schema_name': schema_name,
                'schema_definition': schema_definition,
                'storage_result': storage_result,
                'validation_summary': {
                    'total_records': len(data_records),
                    'valid_records': sum(1 for v in validation_results if v['is_valid']),
                    'average_quality_score': sum(v['quality_score'] for v in validation_results) / len(validation_results),
                    'field_count': len(schema_definition.get('fields', {}))
                },
                'quality_metrics': quality_metrics,
                'processing_method': 'dynamic_schema'
            }
            
        except Exception as e:
            logger.error(f"Dynamic schema processing failed: {str(e)}")
            raise

    async def _infer_dynamic_schema(self, data_records: List[Dict[str, Any]], schema_name: str) -> Dict[str, Any]:
        """Infer schema from data records using intelligent validator"""
        if not data_records:
            return {'fields': {}, 'metadata': {'record_count': 0}}
        
        # Use the dynamic validator to infer schema
        profiles = self.dynamic_validator.generate_comprehensive_profiles(data_records)
        
        # Convert profiles to schema definition
        schema_fields = {}
        for field_name, profile in profiles.items():
            schema_fields[field_name] = {
                'type': profile.detected_type.value,
                'required': profile.null_count == 0,  # No nulls means required
                'constraints': {
                    'min_length': profile.min_length,
                    'max_length': profile.max_length,
                    'min_value': profile.min_value,
                    'max_value': profile.max_value,
                    'pattern': profile.pattern if hasattr(profile, 'pattern') else None,
                    'categories': list(profile.category_frequencies.keys()) if profile.is_categorical else None
                },
                'quality_indicators': {
                    'completeness': profile.completeness_score,
                    'consistency': profile.consistency_score,
                    'validity': profile.validity_score,
                    'overall_quality': profile.overall_quality_score
                },
                'statistics': {
                    'total_values': profile.total_values,
                    'unique_values': profile.unique_values,
                    'null_count': profile.null_count,
                    'mean': profile.mean_value,
                    'std_dev': profile.std_value
                }
            }
        
        return {
            'schema_name': schema_name,
            'fields': schema_fields,
            'metadata': {
                'record_count': len(data_records),
                'inferred_at': datetime.utcnow().isoformat(),
                'inference_method': 'intelligent_validator'
            }
        }

    async def _validate_against_dynamic_schema(self, data_records: List[Dict[str, Any]], 
                                             schema_definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate data records against inferred schema"""
        validation_results = []
        
        for idx, record in enumerate(data_records):
            result = {
                'record_index': idx,
                'is_valid': True,
                'quality_score': 1.0,
                'errors': [],
                'warnings': []
            }
            
            # Validate each field
            field_scores = []
            schema_fields = schema_definition.get('fields', {})
            
            for field_name, field_schema in schema_fields.items():
                field_value = record.get(field_name)
                field_score = 1.0
                
                # Check required fields
                if field_schema.get('required', False) and (field_value is None or field_value == ''):
                    result['errors'].append(f"Required field '{field_name}' is missing")
                    result['is_valid'] = False
                    field_score = 0.0
                
                # Check data type consistency
                if field_value is not None:
                    expected_type = field_schema.get('type', 'string')
                    if not self._validate_field_type(field_value, expected_type):
                        result['warnings'].append(f"Field '{field_name}' type mismatch: expected {expected_type}")
                        field_score *= 0.8
                
                # Check constraints
                constraints = field_schema.get('constraints', {})
                if field_value is not None and constraints:
                    constraint_score = self._validate_field_constraints(field_value, constraints)
                    field_score *= constraint_score
                
                field_scores.append(field_score)
            
            # Calculate overall quality score
            result['quality_score'] = sum(field_scores) / len(field_scores) if field_scores else 1.0
            validation_results.append(result)
        
        return validation_results

    def _validate_field_type(self, value: Any, expected_type: str) -> bool:
        """Validate if value matches expected type"""
        type_validators = {
            'integer': lambda x: isinstance(x, int) or (isinstance(x, str) and x.isdigit()),
            'float': lambda x: isinstance(x, (int, float)) or (isinstance(x, str) and self._is_numeric(x)),
            'boolean': lambda x: isinstance(x, bool) or str(x).lower() in ['true', 'false', '1', '0'],
            'string': lambda x: isinstance(x, str),
            'datetime': lambda x: self._is_datetime(x),
            'email': lambda x: '@' in str(x) and '.' in str(x),
            'phone': lambda x: len(str(x).replace(' ', '').replace('-', '')) >= 10,
            'url': lambda x: str(x).startswith(('http://', 'https://'))
        }
        
        validator = type_validators.get(expected_type, lambda x: True)
        return validator(value)

    def _validate_field_constraints(self, value: Any, constraints: Dict[str, Any]) -> float:
        """Validate field against constraints and return score (0.0 to 1.0)"""
        score = 1.0
        
        # Length constraints
        if 'min_length' in constraints and len(str(value)) < constraints['min_length']:
            score *= 0.7
        if 'max_length' in constraints and len(str(value)) > constraints['max_length']:
            score *= 0.7
        
        # Value constraints
        if 'min_value' in constraints:
            try:
                if float(value) < constraints['min_value']:
                    score *= 0.5
            except (ValueError, TypeError):
                pass
        
        if 'max_value' in constraints:
            try:
                if float(value) > constraints['max_value']:
                    score *= 0.5
            except (ValueError, TypeError):
                pass
        
        # Category constraints
        if 'categories' in constraints and constraints['categories']:
            if str(value) not in constraints['categories']:
                score *= 0.6
        
        return score

    def _calculate_dynamic_quality_metrics(self, data_records: List[Dict[str, Any]], 
                                         validation_results: List[Dict[str, Any]], 
                                         schema_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate quality metrics for dynamic schema processing"""
        total_records = len(data_records)
        valid_records = sum(1 for v in validation_results if v['is_valid'])
        
        # Field-level metrics
        field_metrics = {}
        schema_fields = schema_definition.get('fields', {})
        
        for field_name in schema_fields.keys():
            field_values = [record.get(field_name) for record in data_records]
            non_null_values = [v for v in field_values if v is not None and v != '']
            
            field_metrics[field_name] = {
                'completeness': len(non_null_values) / total_records,
                'uniqueness': len(set(str(v) for v in non_null_values)) / len(non_null_values) if non_null_values else 0,
                'null_rate': (total_records - len(non_null_values)) / total_records
            }
        
        return {
            'overall_quality_score': sum(v['quality_score'] for v in validation_results) / total_records,
            'data_completeness': valid_records / total_records,
            'schema_coverage': len([f for f in schema_fields if any(record.get(f) is not None for record in data_records)]) / len(schema_fields) if schema_fields else 1.0,
            'field_metrics': field_metrics,
            'error_rate': sum(1 for v in validation_results if v['errors']) / total_records,
            'warning_rate': sum(1 for v in validation_results if v['warnings']) / total_records
        }

    def _is_numeric(self, value: str) -> bool:
        """Check if string value is numeric"""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_datetime(self, value: Any) -> bool:
        """Check if value is a datetime"""
        if isinstance(value, datetime):
            return True
        try:
            pd.to_datetime(str(value))
            return True
        except:
            return False


# Global ETL service instance
etl_service = DataWarehouseETL()


async def get_etl_service() -> DataWarehouseETL:
    """Get the global ETL service instance"""
    await etl_service.initialize()
    return etl_service
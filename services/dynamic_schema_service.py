"""
Dynamic Schema Service for handling varying data structures and schema evolution.

This service provides:
1. Dynamic table creation based on inferred schemas
2. Schema versioning and evolution tracking
3. Flexible data storage (both structured and semi-structured)
4. Runtime SQLAlchemy model generation
5. Schema registry and metadata management
"""

import json
import hashlib
from typing import Dict, List, Any, Optional, Union, Type
from datetime import datetime
from sqlalchemy import (
    create_engine, Table, Column, Integer, String, DateTime, 
    JSON, Text, Boolean, Float, MetaData, inspect, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.types import TypeDecorator
import uuid
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()

class SchemaRegistry(Base):
    """Registry for tracking all discovered schemas and their evolution"""
    __tablename__ = "schema_registry"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    schema_name = Column(String(255), nullable=False)
    schema_version = Column(Integer, nullable=False, default=1)
    schema_hash = Column(String(64), nullable=False)  # SHA-256 hash of schema
    schema_definition = Column(JSONB, nullable=False)  # Full schema as JSON
    source_type = Column(String(50), nullable=False)  # 'file', 'api', 'stream'
    table_name = Column(String(255))  # Generated table name if created
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    sample_data = Column(JSONB)  # Sample records for reference
    
    def __repr__(self):
        return f"<SchemaRegistry({self.schema_name} v{self.schema_version})>"

class DynamicDataTable(Base):
    """Generic table for storing data with flexible schemas using JSONB"""
    __tablename__ = "dynamic_data"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    schema_id = Column(UUID(as_uuid=True), nullable=False)  # References schema_registry
    job_id = Column(String(255))  # Processing job ID
    source_file = Column(String(500))  # Original source file/API
    row_index = Column(Integer)  # Original row number in source
    data_payload = Column(JSONB, nullable=False)  # Actual data as JSON
    validation_status = Column(String(20))  # 'valid', 'invalid', 'warning'
    quality_score = Column(Float)
    error_details = Column(JSONB)  # Validation errors if any
    ingestion_time = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<DynamicData({self.id} - {self.validation_status})>"

class DynamicSchemaService:
    """Service for managing dynamic schemas and data storage"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        self._created_tables = {}  # Cache of dynamically created tables
        
        # Create base schema registry and dynamic data tables
        Base.metadata.create_all(self.engine)
    
    def calculate_schema_hash(self, schema_definition: Dict[str, Any]) -> str:
        """Calculate SHA-256 hash of schema definition for versioning"""
        schema_str = json.dumps(schema_definition, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()
    
    def infer_sqlalchemy_type(self, field_info: Dict[str, Any]) -> str:
        """Convert intelligent validator field info to SQLAlchemy type"""
        field_type = field_info.get('type', 'string').lower()
        
        type_mapping = {
            'integer': 'Integer',
            'float': 'Float',
            'boolean': 'Boolean',
            'datetime': 'DateTime',
            'date': 'Date',
            'string': 'String(255)',
            'text': 'Text',
            'email': 'String(255)',
            'phone': 'String(20)',
            'url': 'String(500)',
            'uuid': 'UUID(as_uuid=True)',
            'json': 'JSON',
            'array': 'ARRAY(String)',
        }
        
        return type_mapping.get(field_type, 'String(255)')
    
    def register_schema(
        self, 
        schema_name: str,
        schema_definition: Dict[str, Any],
        source_type: str,
        sample_data: Optional[List[Dict]] = None
    ) -> str:
        """Register a new schema or get existing schema ID"""
        schema_hash = self.calculate_schema_hash(schema_definition)
        
        with self.Session() as session:
            # Check if schema already exists
            existing = session.query(SchemaRegistry).filter_by(
                schema_name=schema_name,
                schema_hash=schema_hash
            ).first()
            
            if existing:
                logger.info(f"Schema {schema_name} already registered with hash {schema_hash}")
                return str(existing.id)
            
            # Check for schema evolution (same name, different hash)
            latest_version = session.query(SchemaRegistry).filter_by(
                schema_name=schema_name
            ).order_by(SchemaRegistry.schema_version.desc()).first()
            
            version = 1 if not latest_version else latest_version.schema_version + 1
            
            # Create new schema entry
            new_schema = SchemaRegistry(
                schema_name=schema_name,
                schema_version=version,
                schema_hash=schema_hash,
                schema_definition=schema_definition,
                source_type=source_type,
                sample_data=sample_data[:5] if sample_data else None  # Store first 5 samples
            )
            
            session.add(new_schema)
            session.commit()
            
            logger.info(f"Registered new schema: {schema_name} v{version}")
            return str(new_schema.id)
    
    def create_dynamic_table(self, schema_id: str, force_recreate: bool = False) -> Optional[str]:
        """Create a dedicated table for a specific schema (structured approach)"""
        with self.Session() as session:
            schema_info = session.query(SchemaRegistry).filter_by(id=schema_id).first()
            if not schema_info:
                logger.error(f"Schema {schema_id} not found")
                return None
            
            table_name = f"data_{schema_info.schema_name}_{schema_info.schema_version}".lower()
            table_name = table_name.replace(' ', '_').replace('-', '_')
            
            # Check if table already exists
            if table_name in self._created_tables and not force_recreate:
                return table_name
            
            inspector = inspect(self.engine)
            if table_name in inspector.get_table_names() and not force_recreate:
                self._created_tables[table_name] = table_name
                return table_name
            
            # Build dynamic table columns
            columns = [
                Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                Column('job_id', String(255)),
                Column('source_file', String(500)),
                Column('row_index', Integer),
                Column('validation_status', String(20)),
                Column('quality_score', Float),
                Column('ingestion_time', DateTime, default=datetime.utcnow),
            ]
            
            # Add dynamic columns based on schema
            schema_def = schema_info.schema_definition
            for field_name, field_info in schema_def.get('fields', {}).items():
                if field_name.lower() in ['id', 'job_id', 'source_file', 'row_index', 'validation_status', 'quality_score', 'ingestion_time']:
                    continue  # Skip reserved columns
                
                sql_type = self.infer_sqlalchemy_type(field_info)
                nullable = not field_info.get('required', False)
                
                # Create column dynamically
                try:
                    col_type = eval(f"sqlalchemy.{sql_type}")
                    columns.append(Column(field_name, col_type, nullable=nullable))
                except:
                    # Fallback to String if type inference fails
                    columns.append(Column(field_name, String(255), nullable=nullable))
            
            # Create the table
            try:
                dynamic_table = Table(table_name, self.metadata, *columns)
                dynamic_table.create(self.engine, checkfirst=True)
                
                # Update schema registry with table name
                schema_info.table_name = table_name
                session.commit()
                
                self._created_tables[table_name] = table_name
                logger.info(f"Created dynamic table: {table_name}")
                return table_name
                
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {str(e)}")
                return None
    
    def store_data_flexible(
        self,
        schema_id: str,
        data_records: List[Dict[str, Any]],
        job_id: str,
        source_file: str,
        validation_results: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Store data using flexible JSONB approach"""
        with self.Session() as session:
            stored_records = []
            
            for idx, record in enumerate(data_records):
                # Determine validation status
                validation_status = 'valid'
                quality_score = 1.0
                error_details = None
                
                if validation_results:
                    # Extract validation info for this record if available
                    if idx < len(validation_results.get('record_results', [])):
                        record_result = validation_results['record_results'][idx]
                        validation_status = record_result.get('status', 'valid')
                        quality_score = record_result.get('quality_score', 1.0)
                        error_details = record_result.get('errors')
                
                # Create dynamic data entry
                dynamic_entry = DynamicDataTable(
                    schema_id=schema_id,
                    job_id=job_id,
                    source_file=source_file,
                    row_index=idx,
                    data_payload=record,
                    validation_status=validation_status,
                    quality_score=quality_score,
                    error_details=error_details
                )
                
                session.add(dynamic_entry)
                stored_records.append(dynamic_entry.id)
            
            session.commit()
            
            return {
                'stored_count': len(stored_records),
                'record_ids': [str(rid) for rid in stored_records],
                'schema_id': schema_id,
                'storage_method': 'flexible_jsonb'
            }
    
    def store_data_structured(
        self,
        schema_id: str,
        data_records: List[Dict[str, Any]],
        job_id: str,
        source_file: str,
        validation_results: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Store data using structured table approach"""
        # First ensure the dynamic table exists
        table_name = self.create_dynamic_table(schema_id)
        if not table_name:
            # Fallback to flexible storage
            logger.warning(f"Could not create structured table for schema {schema_id}, using flexible storage")
            return self.store_data_flexible(schema_id, data_records, job_id, source_file, validation_results)
        
        # Insert data into structured table
        with self.engine.connect() as conn:
            inserted_count = 0
            
            for idx, record in enumerate(data_records):
                try:
                    # Prepare record with metadata
                    insert_record = {
                        'id': str(uuid.uuid4()),
                        'job_id': job_id,
                        'source_file': source_file,
                        'row_index': idx,
                        'validation_status': 'valid',
                        'quality_score': 1.0,
                        'ingestion_time': datetime.utcnow()
                    }
                    
                    # Add validation results if available
                    if validation_results and idx < len(validation_results.get('record_results', [])):
                        record_result = validation_results['record_results'][idx]
                        insert_record['validation_status'] = record_result.get('status', 'valid')
                        insert_record['quality_score'] = record_result.get('quality_score', 1.0)
                    
                    # Add data fields
                    insert_record.update(record)
                    
                    # Insert into dynamic table
                    conn.execute(text(f"""
                        INSERT INTO {table_name} ({', '.join(insert_record.keys())})
                        VALUES ({', '.join([':' + k for k in insert_record.keys()])})
                    """), insert_record)
                    
                    inserted_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to insert record {idx} into {table_name}: {str(e)}")
            
            conn.commit()
            
            return {
                'stored_count': inserted_count,
                'table_name': table_name,
                'schema_id': schema_id,
                'storage_method': 'structured_table'
            }
    
    def get_schema_info(self, schema_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a schema"""
        with self.Session() as session:
            schema_info = session.query(SchemaRegistry).filter_by(id=schema_id).first()
            if not schema_info:
                return None
            
            return {
                'id': str(schema_info.id),
                'name': schema_info.schema_name,
                'version': schema_info.schema_version,
                'hash': schema_info.schema_hash,
                'definition': schema_info.schema_definition,
                'source_type': schema_info.source_type,
                'table_name': schema_info.table_name,
                'created_at': schema_info.created_at.isoformat(),
                'sample_data': schema_info.sample_data
            }
    
    def list_schemas(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """List all registered schemas"""
        with self.Session() as session:
            query = session.query(SchemaRegistry)
            if active_only:
                query = query.filter_by(is_active=True)
            
            schemas = query.order_by(SchemaRegistry.created_at.desc()).all()
            
            return [
                {
                    'id': str(s.id),
                    'name': s.schema_name,
                    'version': s.schema_version,
                    'source_type': s.source_type,
                    'table_name': s.table_name,
                    'created_at': s.created_at.isoformat(),
                    'field_count': len(s.schema_definition.get('fields', {}))
                }
                for s in schemas
            ]
    
    def query_data(
        self,
        schema_id: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Query data from either structured table or flexible storage"""
        with self.Session() as session:
            schema_info = session.query(SchemaRegistry).filter_by(id=schema_id).first()
            if not schema_info:
                return {'error': 'Schema not found', 'data': []}
            
            if schema_info.table_name:
                # Query structured table
                try:
                    with self.engine.connect() as conn:
                        query = f"SELECT * FROM {schema_info.table_name}"
                        
                        if filters:
                            where_clauses = []
                            for key, value in filters.items():
                                where_clauses.append(f"{key} = '{value}'")
                            if where_clauses:
                                query += " WHERE " + " AND ".join(where_clauses)
                        
                        query += f" LIMIT {limit} OFFSET {offset}"
                        
                        result = conn.execute(text(query))
                        columns = result.keys()
                        rows = result.fetchall()
                        
                        return {
                            'data': [dict(zip(columns, row)) for row in rows],
                            'count': len(rows),
                            'schema_id': schema_id,
                            'storage_method': 'structured_table'
                        }
                except Exception as e:
                    logger.error(f"Failed to query structured table: {str(e)}")
            
            # Query flexible storage
            query = session.query(DynamicDataTable).filter_by(schema_id=schema_id)
            
            if filters:
                for key, value in filters.items():
                    if key in ['validation_status', 'job_id', 'source_file']:
                        query = query.filter(getattr(DynamicDataTable, key) == value)
                    else:
                        # Filter on JSONB data
                        query = query.filter(DynamicDataTable.data_payload[key].astext == str(value))
            
            results = query.offset(offset).limit(limit).all()
            
            return {
                'data': [
                    {
                        'id': str(r.id),
                        'job_id': r.job_id,
                        'source_file': r.source_file,
                        'row_index': r.row_index,
                        'validation_status': r.validation_status,
                        'quality_score': r.quality_score,
                        'ingestion_time': r.ingestion_time.isoformat(),
                        **r.data_payload
                    }
                    for r in results
                ],
                'count': len(results),
                'schema_id': schema_id,
                'storage_method': 'flexible_jsonb'
            }
    
    def get_schema_evolution(self, schema_name: str) -> List[Dict[str, Any]]:
        """Get evolution history of a schema"""
        with self.Session() as session:
            versions = session.query(SchemaRegistry).filter_by(
                schema_name=schema_name
            ).order_by(SchemaRegistry.schema_version.asc()).all()
            
            return [
                {
                    'version': v.schema_version,
                    'hash': v.schema_hash,
                    'created_at': v.created_at.isoformat(),
                    'field_count': len(v.schema_definition.get('fields', {})),
                    'table_name': v.table_name,
                    'changes': self._compare_schemas(
                        versions[i-1].schema_definition if i > 0 else {},
                        v.schema_definition
                    ) if i > 0 else []
                }
                for i, v in enumerate(versions)
            ]
    
    def _compare_schemas(self, old_schema: Dict, new_schema: Dict) -> List[str]:
        """Compare two schemas and return list of changes"""
        changes = []
        
        old_fields = old_schema.get('fields', {})
        new_fields = new_schema.get('fields', {})
        
        # Added fields
        for field in new_fields:
            if field not in old_fields:
                changes.append(f"Added field: {field}")
        
        # Removed fields
        for field in old_fields:
            if field not in new_fields:
                changes.append(f"Removed field: {field}")
        
        # Modified fields
        for field in set(old_fields.keys()) & set(new_fields.keys()):
            if old_fields[field] != new_fields[field]:
                changes.append(f"Modified field: {field}")
        
        return changes

# Global service instance
_dynamic_schema_service = None

async def get_dynamic_schema_service() -> DynamicSchemaService:
    """Get global dynamic schema service instance"""
    global _dynamic_schema_service
    if _dynamic_schema_service is None:
        from .database_service import get_database_service
        db_service = await get_database_service()
        _dynamic_schema_service = DynamicSchemaService(db_service.config.database_url)
    return _dynamic_schema_service
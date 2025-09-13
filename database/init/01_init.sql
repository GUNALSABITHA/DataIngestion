-- Database initialization script for Data Warehouse
-- This script sets up the initial database structure and extensions

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create custom data types
CREATE TYPE data_quality_level AS ENUM ('excellent', 'good', 'fair', 'poor');
CREATE TYPE processing_status AS ENUM ('pending', 'processing', 'completed', 'failed', 'quarantined');
CREATE TYPE validation_issue_type AS ENUM (
    'missing_value', 'invalid_format', 'out_of_range', 
    'inconsistent_pattern', 'duplicate_value', 'outlier', 
    'encoding_issue', 'length_violation'
);

-- Grant permissions
GRANT USAGE ON SCHEMA staging TO datauser;
GRANT USAGE ON SCHEMA warehouse TO datauser;
GRANT USAGE ON SCHEMA analytics TO datauser;

GRANT CREATE ON SCHEMA staging TO datauser;
GRANT CREATE ON SCHEMA warehouse TO datauser;
GRANT CREATE ON SCHEMA analytics TO datauser;

-- Create indexes for performance
-- (Will be added after table creation)
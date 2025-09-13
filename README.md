# 🚀 Advanced Data Ingestion & Validation Platform

**A comprehensive, enterprise-grade data ingestion and validation system with real-time analytics, dynamic schema evolution, persistent job tracking, and interactive visualizations.**

## 🌟 Overview

This platform provides a complete data lifecycle management solution featuring intelligent schema inference, real-time validation, quality analytics, interactive dashboards, and persistent PostgreSQL storage. Built with FastAPI, React, PostgreSQL data warehouse, and Redis for caching, this system handles complex data processing workflows with enterprise reliability.

## 🚀 What's New in This Version

### ✅ **Persistent Job Storage**
- **PostgreSQL Backend**: All jobs, validation results, and quality metrics now stored in database
- **Survives Restarts**: Complete job history and reports persist after server restarts
- **Historical Analytics**: Rich reporting from persistent data warehouse
- **Real Data**: Frontend charts and dashboard now use real database data (no more mock data)

### 📊 **Enhanced Reporting**
- **Database Reports**: New `/api/reports` endpoints for comprehensive reporting
- **Quality Metrics**: Persistent tracking of data quality over time
- **Validation History**: Complete audit trail of all validation results
- **Detailed Job Reports**: Deep dive into specific job performance and results

### 🔧 **Infrastructure Improvements**
- **Docker Compose**: Complete infrastructure stack (PostgreSQL, Redis, PgAdmin, Kafka)
- **Database Schema**: Optimized warehouse schema with fact and dimension tables
- **Production Ready**: Async database operations with proper error handling
- **API Documentation**: Comprehensive API docs with all new endpoints

## ✨ Key Features

### 🧠 **Intelligent Schema Processing**
- **Dynamic Schema Detection**: Automatically understands any data structure without configuration
- **Schema Evolution**: Tracks and adapts to changing data structures over time
- **15+ Data Type Detection**: Advanced pattern recognition for emails, phones, currencies, dates, etc.
- **Flexible Storage**: Both structured (PostgreSQL) and flexible (JSON) storage options

### 🎯 **Enterprise Validation Engine**
- **Custom Validation Rules**: Define business-specific validation logic
- **Quality Scoring**: Advanced quality metrics with configurable thresholds
- **Error Categorization**: Detailed error analysis with actionable insights
- **Batch & Streaming**: Support for both batch uploads and real-time streaming
- **Persistent Results**: All validation results stored in PostgreSQL warehouse

### 📊 **Interactive Analytics Dashboard**
- **Real-time Charts**: Interactive Chart.js visualizations with live data
- **Quality Trends**: Track data quality metrics over time
- **Error Distribution**: Visual breakdown of validation issues
- **Performance Metrics**: Processing throughput and system health monitoring
- **Database Reports**: Comprehensive reports from persistent job storage

### 💾 **Persistent Job Management**
- **PostgreSQL Storage**: All jobs, validation results, and quality metrics stored in database
- **Survives Restarts**: Job history and reports persist after server restarts
- **Warehouse Schema**: Fact and dimension tables optimized for analytics
- **Report Generation**: Rich reports from database history
- **Export Capabilities**: Download reports in multiple formats

### 🏗️ **Data Warehousing & ETL**
- **PostgreSQL Data Warehouse**: Dimensional modeling with fact and dimension tables
- **ETL Pipeline**: Automated Extract, Transform, Load processes
- **Historical Tracking**: Complete audit trail of all data processing activities
- **Analytics Ready**: Pre-built aggregations for fast reporting

### 🔄 **Real-time Streaming**
- **Kafka Integration**: High-throughput message streaming with fault tolerance
- **API Streaming**: Direct API-to-validation workflows
- **Background Processing**: Asynchronous job execution with progress tracking
- **Event Sourcing**: Complete event history for data lineage

## 📁 Architecture Overview

```
DataIngestion/
├── 🎨 frontend/                    # React + TypeScript frontend
│   ├── src/
│   │   ├── components/
│   │   │   ├── charts/             # Chart.js visualization components
│   │   │   └── ui/                 # Reusable UI components (shadcn/ui)
│   │   ├── pages/                  # Main application pages
│   │   │   ├── Index.tsx           # Dashboard with recent validations
│   │   │   ├── Upload.tsx          # File upload interface
│   │   │   ├── Reports.tsx         # Analytics & visualizations
│   │   │   ├── Results.tsx         # Job history & management
│   │   │   └── Dashboard.tsx       # Real-time monitoring
│   │   ├── hooks/                  # Custom React hooks for API integration
│   │   └── lib/                    # API client and utilities
│   └── package.json
├── 🐍 Backend Services/
│   ├── api.py                      # FastAPI main application
│   ├── models/
│   │   ├── __init__.py
│   │   └── data_models.py          # Pydantic models and validation schemas
│   ├── services/
│   │   ├── __init__.py
│   │   ├── file_processor.py       # Multi-format file processing
│   │   ├── data_validator.py       # Advanced validation engine
│   │   ├── data_reporter.py        # Quality reporting service
│   │   ├── database_service.py     # PostgreSQL data warehouse
│   │   ├── dynamic_schema_service.py # Schema evolution management
│   │   ├── etl_service.py          # ETL pipeline orchestration
│   │   ├── kafka_producer_enhanced.py # Enhanced Kafka producer
│   │   └── kafka_consumer_enhanced.py # Enhanced Kafka consumer
│   └── data_ingestion_pipeline.py  # CLI pipeline orchestrator
├── 🐳 Infrastructure/
│   ├── docker-compose.yml          # Multi-service Docker setup
│   │   ├── PostgreSQL              # Data warehouse
│   │   ├── Redis                   # Caching & session management
│   │   ├── PgAdmin                 # Database administration
│   │   └── Kafka + Zookeeper       # Message streaming
├── 📊 Configuration/
│   ├── config.ini                  # Application configuration
│   ├── config.yaml                 # Advanced configuration
│   └── requirements.txt            # Python dependencies
└── 📁 Data Directories/
    ├── output/                     # Validation results
    ├── reports/                    # Generated reports
    ├── temp_uploads/               # Temporary file storage
    └── intelligent_test_output/    # Test outputs
```

## 🛠️ Installation & Setup

### Prerequisites
- **Python 3.8+** (with pip)
- **Node.js 16+** & **npm** (for frontend)
- **Docker & Docker Compose** (for infrastructure)
- **Git** (for cloning the repository)

### 1. Clone Repository
```bash
git clone https://github.com/GUNALSABITHA/DataIngestion.git
cd DataIngestion
```

### 2. Backend Setup
```bash
# Install Python dependencies
pip install -r requirements.txt

# Configure environment (optional)
cp config.ini.example config.ini
# Edit config.ini with your settings
```

### 3. Frontend Setup
```bash
# Navigate to frontend directory
cd frontend

# Install Node.js dependencies
npm install

# Return to project root
cd ..
```

### 4. Infrastructure Setup (Docker)
```bash
# Start all infrastructure services
docker compose up -d

# Verify services are running
docker compose ps
```

**Services Started:**
- 🐘 **PostgreSQL** (port 5432) - Data warehouse
- 🟥 **Redis** (port 6379) - Caching & sessions  
- 🌐 **PgAdmin** (port 8081) - Database management UI
- 📨 **Kafka** (port 9092) - Message streaming
- 🔧 **Zookeeper** (port 2181) - Kafka coordination

### 5. Database Initialization
The database tables are automatically created when you first start the backend API.

## 🚀 Quick Start

### Method 1: Full Web Application (Recommended)

#### 1. Start Backend API
```bash
# In project root directory
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

#### 2. Start Frontend (New Terminal)
```bash
# In frontend directory
cd frontend
npm run dev
```

#### 3. Access the Application
- 🌐 **Frontend**: http://localhost:8080 (or displayed port)
- 📡 **API Docs**: http://localhost:8000/docs
- 🗄️ **Database Admin**: http://localhost:8081

#### 4. Upload & Validate Data
1. Navigate to the **Upload** page
2. Drag & drop or select your data file (CSV, JSON, Excel, etc.)
3. Choose validation action (validate, kafka, pipeline)
4. Monitor progress in real-time
5. View results in **Reports** and **History** pages with persistent data

#### 5. Explore New Features
- **Reports Page**: View comprehensive reports from database
- **Persistent History**: See all jobs survive server restarts
- **Quality Analytics**: Analyze trends with real historical data
- **Database Management**: Use PgAdmin to explore warehouse data
3. Choose validation action (validate, kafka, pipeline)
4. Monitor progress in real-time
5. View results in **Reports** and **History** pages

### Method 2: Command Line Interface

#### Basic File Validation
```bash
python data_ingestion_pipeline.py validate-file --input-file data.csv
```

#### Full Pipeline with Kafka
```bash
python data_ingestion_pipeline.py full-pipeline --input-file data.csv --topic customer_data
```

#### API Streaming Validation
```bash
python data_ingestion_pipeline.py api-streaming --api-url "http://api.example.com/data"
```

### Method 3: Direct API Usage

#### Upload File via API
```bash
curl -X POST "http://localhost:8000/api/upload" \
  -F "file=@data.csv" \
  -F "action=validate"
```

#### Check Job Status
```bash
curl "http://localhost:8000/api/job/YOUR_JOB_ID/status"
```


## 📡 API Endpoints

### Core Upload & Validation
- `POST /api/upload` - Upload and process files
- `POST /api/upload-dynamic` - Upload with dynamic schema detection
- `GET /api/job/{job_id}/status` - Get job status and progress
- `GET /api/jobs` - List all jobs
- `GET /api/recent-validations` - Get recent validation history
- `DELETE /api/jobs/{job_id}` - Delete a job

### Data Warehouse & Analytics
- `GET /api/warehouse/jobs` - Get warehouse job statistics
- `POST /api/warehouse/store` - Store processed data in warehouse
- `GET /api/warehouse/schema/{table_name}` - Get table schema information

### Dynamic Schema Management
- `GET /api/schema/registry` - Get all registered schemas
- `POST /api/schema/register` - Register a new schema
- `GET /api/schema/evolution/{schema_name}` - Get schema evolution history

### Reports & Analytics
- `GET /api/reports/dashboard-stats` - Dashboard statistics
- `GET /api/reports/processing-timeline` - Processing timeline data
- `GET /api/reports/data-sources` - Data source performance
- `GET /api/reports/quality-metrics` - Quality metrics over time

### Streaming & Real-time
- `POST /api/streaming/validate` - Real-time API streaming validation
- `POST /api/kafka/{job_id}` - Send job data to Kafka
- `POST /api/pipeline/{job_id}` - Execute full pipeline

## 🎨 Frontend Features

### 📊 Interactive Dashboard
- **Recent Validations**: Live updates of latest validation jobs
- **Quality Metrics**: Real-time quality score tracking
- **System Health**: Monitor processing performance

### 📈 Analytics & Reports
- **Quality Trends**: Chart.js visualizations of data quality over time
- **Error Distribution**: Visual breakdown of validation issues
- **Processing Volume**: Timeline of data processing activity
- **Data Sources**: Performance metrics by data source type

### 📁 File Management
- **Drag & Drop Upload**: Intuitive file upload interface
- **Progress Tracking**: Real-time upload and processing progress
- **Job History**: Complete history of all validation jobs
- **Download Results**: Export validation results and reports

### ⚙️ Configuration
- **Validation Rules**: Configure custom validation thresholds
- **Quality Settings**: Adjust quality scoring parameters
- **Export Options**: Choose output formats and destinations
## � Data Processing Outputs & Storage

### Persistent Database Storage
All job data, validation results, and quality metrics are stored in PostgreSQL for persistence across server restarts:

#### Job Storage Tables
- **`fact_processing_job`**: Complete job history with status, metrics, and timing
- **`fact_validation_result`**: Detailed validation results for each rule and field
- **`fact_quality_metric`**: Quality metrics and threshold compliance
- **`dim_data_source`**: Data source information and metadata

#### File-Based Outputs
- **`good_data.csv`**: Records meeting quality standards (≥80% quality score)
- **`high_quality_data.csv`**: Premium quality records (≥90% quality score)  
- **`quarantine.csv`**: Records requiring manual review
- **`violations.csv`**: Records with validation errors

### Report Generation

#### Interactive Database Reports
- Quality score distributions and trends over time from persistent data
- Field-level validation statistics and error patterns
- Processing performance metrics and throughput analysis
- Historical comparison and trend analysis
- Error categorization with actionable insights

#### JSON Processing Reports
```json
{
  "processing_summary": {
    "total_records": 1000,
    "good_data_count": 850,
    "high_quality_count": 750,
    "quarantine_count": 100,
    "violations_count": 50,
    "processing_duration": 2.5,
    "success_rate": 95.0
  },
  "quality_metrics": {
    "overall_score": 0.85,
    "completeness": 0.92,
    "validity": 0.88,
    "consistency": 0.82,
    "uniqueness": 0.91
  },
  "field_analysis": {
    "customer_id": {"quality_score": 0.99, "errors": 1},
    "email": {"quality_score": 0.87, "errors": 13},
    "phone": {"quality_score": 0.92, "errors": 8}
  }
}
```

## 📡 API Endpoints

### Core Upload & Validation
- `POST /api/upload` - Upload and process files (persistent job storage)
- `POST /api/upload-dynamic` - Upload with dynamic schema detection
- `GET /api/status/{job_id}` - Get job status and progress from database
- `GET /api/jobs` - List all jobs from persistent storage
- `GET /api/recent-validations` - Get recent validation history
- `DELETE /api/jobs/{job_id}` - Delete a job and its data

### Reports & Analytics (New!)
- `GET /api/reports` - Get all reports from database
- `GET /api/reports/{job_id}` - Get detailed report for specific job
- `GET /api/quarantine` - Get quarantined items from completed jobs

### Data Warehouse & Analytics
- `GET /api/warehouse/jobs` - Get warehouse job statistics
- `POST /api/warehouse/store` - Store processed data in warehouse
- `GET /api/warehouse/schema/{table_name}` - Get table schema information

### Dynamic Schema Management
- `GET /api/schema/registry` - Get all registered schemas
- `POST /api/schema/register` - Register a new schema
- `GET /api/schema/evolution/{schema_name}` - Get schema evolution history

### Streaming & Real-time
- `POST /api/stream` - Real-time API streaming validation
- `POST /api/kafka/{job_id}` - Send job data to Kafka
- `POST /api/pipeline/{job_id}` - Execute full pipeline

## 🎨 Frontend Features

### 📊 Interactive Dashboard
- **Recent Validations**: Live updates from persistent database storage
- **Quality Metrics**: Real-time quality score tracking with historical data
- **System Health**: Monitor processing performance over time
- **Persistent Data**: All charts now use real data from PostgreSQL

### 📈 Analytics & Reports  
- **Quality Trends**: Chart.js visualizations with persistent historical data
- **Error Distribution**: Visual breakdown from database validation results
- **Processing Volume**: Timeline of data processing activity
- **Data Sources**: Performance metrics by data source type
- **Historical Analysis**: Compare current vs. historical performance

### 📁 File Management
- **Drag & Drop Upload**: Intuitive file upload interface
- **Progress Tracking**: Real-time upload and processing progress
- **Job History**: Complete persistent history of all validation jobs
- **Download Results**: Export validation results and reports
- **Persistent Jobs**: Job history survives server restarts

## �🔧 Validation Rules & Configuration

### Built-in Validation Rules

#### Data Type Validation
- **String Types**: Text validation with length limits and pattern matching
- **Numeric Types**: Integer and float validation with range checking
- **Date/Time**: Multiple date format detection and validation
- **Boolean**: Flexible boolean value recognition
- **Email**: RFC-compliant email format validation
- **Phone**: International phone number format validation
- **URL**: Web URL format and accessibility validation

#### Business Logic Validation
- **Required Fields**: Mark fields as mandatory
- **Unique Constraints**: Ensure value uniqueness across dataset
- **Referential Integrity**: Cross-field validation rules
- **Custom Patterns**: Regular expression-based validation
- **Value Lists**: Validate against predefined acceptable values
- **Range Validation**: Min/max value constraints

### Custom Validation Rules

#### Creating Custom Rules
```python
from models.data_models import ValidationRule

# Define custom validation rule
custom_rule = ValidationRule(
    field_name="customer_id",
    rule_type="pattern",
    rule_value=r"^CUST\d{6}$",
    error_message="Customer ID must start with CUST followed by 6 digits"
)
```

#### Configuration Options
```yaml
# config.yaml
validation:
  quality_threshold: 0.8
  strict_mode: false
  max_errors_per_record: 10
  
quality_levels:
  excellent: 0.9
  good: 0.8
  fair: 0.6
  poor: 0.0

processing:
  batch_size: 1000
  timeout_seconds: 300
  max_file_size_mb: 100
```

### Schema Evolution & Dynamic Detection

#### Automatic Schema Detection
- **Field Type Inference**: Automatically detect appropriate data types
- **Pattern Recognition**: Identify common patterns (emails, phones, IDs)
- **Null Handling**: Smart null value detection and handling
- **Encoding Detection**: Automatic character encoding detection

#### Schema Evolution Tracking
- **Version Management**: Track schema changes over time
- **Backward Compatibility**: Maintain compatibility with previous versions
- **Migration Support**: Automatic data migration between schema versions
- **Change Documentation**: Log all schema modifications

## 📊 Data Models & Quality Assessment

### Enhanced Customer Transaction Model

```python
class CustomerTransaction(BaseModel):
    # Customer Information
    customer_id: str = Field(..., pattern=r"^[A-Z]{2,5}\d{3,8}$")
    company_name: str = Field(..., min_length=1, max_length=100)
    contact_name: str = Field(..., min_length=1, max_length=50)
    contact_title: Optional[str] = Field(None, max_length=50)
    
    # Address Information
    address: Optional[str] = Field(None, max_length=200)
    city: str = Field(..., min_length=1, max_length=50)
    region: Optional[str] = Field(None, max_length=50)
    postal_code: str = Field(..., pattern=r"^\d{5}(-\d{4})?$")
    country: str = Field(..., min_length=2, max_length=50)
    
    # Contact Information
    phone: Optional[str] = Field(None, pattern=r"^\+?[\d\s\-\(\)]+$")
    email: Optional[EmailStr] = None
    
    # Business Information
    segment: str = Field(..., pattern=r"^(Enterprise|SMB|Consumer)$")
    industry: Optional[str] = Field(None, max_length=50)
    revenue: Optional[float] = Field(None, ge=0)
    
    # Metadata
    created_date: datetime = Field(default_factory=datetime.now)
    last_updated: Optional[datetime] = None
    
    # Quality Scoring
    quality_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    validation_errors: List[str] = Field(default_factory=list)
```

### Quality Assessment Framework

#### Quality Dimensions
1. **Completeness**: Percentage of non-null required fields
2. **Validity**: Compliance with data type and format rules
3. **Consistency**: Internal consistency across related fields
4. **Uniqueness**: Absence of duplicate records
5. **Accuracy**: Correctness of data values
6. **Timeliness**: Freshness and currency of data

#### Quality Score Calculation
```python
def calculate_quality_score(record):
    scores = {
        'completeness': calculate_completeness(record),
        'validity': calculate_validity(record),
        'consistency': calculate_consistency(record),
        'uniqueness': calculate_uniqueness(record)
    }
    
    # Weighted average
    weights = {'completeness': 0.3, 'validity': 0.4, 'consistency': 0.2, 'uniqueness': 0.1}
    return sum(scores[dim] * weights[dim] for dim in scores)
```

### Data Warehouse Schema

#### Fact Tables
- `data_processing_fact`: Core processing metrics and results
- `validation_fact`: Detailed validation results and errors
- `quality_fact`: Quality scores and trends over time

#### Dimension Tables
- `processing_job_dim`: Job metadata and configuration
- `data_source_dim`: Data source information and characteristics
- `time_dim`: Time dimension for temporal analysis
- `schema_dim`: Schema version and evolution tracking

## 🔄 ETL Pipeline & Data Warehouse

### ETL Process Flow

1. **Extract**: Load data from various sources (files, APIs, streams)
2. **Transform**: Apply validation rules, quality scoring, and data cleaning
3. **Load**: Store processed data in both operational and analytical stores

### Warehouse Features

#### Data Storage Strategy
- **Operational Store**: Real-time data for immediate processing
- **Analytical Store**: Optimized for reporting and analytics
- **Archive Store**: Long-term storage for compliance and auditing

#### Performance Optimization
- **Partitioning**: Time-based and source-based partitioning
- **Indexing**: Strategic indexing for fast query performance
- **Aggregations**: Pre-computed aggregations for common queries
- **Compression**: Data compression for storage efficiency

## 📈 Reporting and Analytics

### Report Types

1. **HTML Reports**: Interactive reports with tables and metrics
2. **Markdown Reports**: Documentation-friendly format
3. **JSON Reports**: Machine-readable format for integration
4. **Dashboard**: Real-time text-based dashboard

### Report Contents

- **Executive Summary**: High-level metrics and KPIs
- **Quality Distribution**: Breakdown by quality levels
- **Error Analysis**: Common error patterns and frequencies
- **Data Insights**: Automated insights and recommendations
- **Processing Statistics**: Performance and throughput metrics

### Sample Report Structure

```
📊 PROCESSING SUMMARY
- Total Records: 1,000
- Valid Records: 950
- Success Rate: 95.0%
- Processing Duration: 2.5s

🎯 QUALITY DISTRIBUTION
- Excellent: 800 (84.2%)
- Good: 120 (12.6%)
- Fair: 25 (2.6%)
- Poor: 5 (0.5%)

💡 INSIGHTS
- Excellent data quality with very high success rate
- Most common issue: postal_code validation
```

## 🔧 Configuration

### File Format Support

The pipeline automatically detects and processes:

- **CSV/TSV**: With encoding detection and delimiter auto-detection
- **JSON**: Standard JSON and JSON Lines formats
- **Excel**: .xlsx and .xls files with sheet selection
- **Parquet**: High-performance columnar format
- **XML**: With configurable parsing options

### Kafka Configuration

Default Kafka settings can be customized:

```python
# Producer settings
producer = EnhancedKafkaProducer(
    bootstrap_servers=["localhost:9092"],
    batch_size=100,
    linger_ms=10
)

# Consumer settings
consumer = EnhancedKafkaConsumer(
    topic="my_topic",
    consumer_group="my_group",
    batch_size=100
)
```

### Quality Thresholds

Adjust quality thresholds for your use case:

```python
# High-quality threshold
high_quality, low_quality = validator.get_quality_filtered_data(
    min_quality_score=0.9
)

# Lenient threshold
high_quality, low_quality = validator.get_quality_filtered_data(
    min_quality_score=0.6
)
```

## 🚦 Error Handling

### Graceful Degradation

- **File Reading**: Multiple encoding fallbacks
- **Kafka Connectivity**: Timeout and retry mechanisms
- **Validation Errors**: Continue processing with error collection
- **Partial Failures**: Process what's possible, report what failed

### Error Categories

1. **File Errors**: File not found, format issues, encoding problems
2. **Validation Errors**: Schema violations, data type mismatches
3. **Kafka Errors**: Connection issues, serialization problems
4. **Processing Errors**: Memory issues, timeout errors

## 📏 Performance Considerations

### Optimization Features

- **Batch Processing**: Configurable batch sizes
- **Memory Management**: Streaming processing for large files
- **Parallel Processing**: Multi-threaded validation
- **Efficient Formats**: Parquet support for large datasets

### Performance Metrics

The pipeline tracks:
- Records processed per second
- Memory usage patterns
- Error rates and patterns
- Processing duration by stage

## 🧪 Testing

### Run Examples

```bash
python examples.py
```

### Manual Testing

1. **File Validation**:
   ```bash
   python data_ingestion_pipeline.py validate-file -i CustomersData.csv
   ```

2. **Performance Test**:
   ```bash
   python data_ingestion_pipeline.py full-pipeline -i CustomersData.csv --verbose
   ```

## 🐛 Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   - Ensure Kafka is running: `docker-compose up -d`
   - Check ports: `localhost:9092`

2. **File Encoding Issues**
   - Pipeline auto-detects encoding
   - Check file with: `file -bi filename`

3. **Memory Issues with Large Files**
   - Reduce batch size: `--batch-size 50`
   - Use streaming mode for very large files

4. **Permission Errors**
   - Ensure write permissions for output directory
   - Check file access permissions

### Debug Mode

Enable verbose logging:

```bash
python data_ingestion_pipeline.py --verbose full-pipeline -i data.csv
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Pydantic for robust data validation
- Kafka for reliable message streaming
- Pandas for efficient data processing
- The open-source community for inspiration and tools

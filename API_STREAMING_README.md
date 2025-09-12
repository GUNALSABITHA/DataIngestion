# API Streaming Feature

## Overview

The Data Ingestion Pipeline now supports streaming data directly from external APIs in addition to file uploads. This feature allows you to process real-time data streams through the same validation and quality assessment pipeline.

## Features

- **Real-time Data Streaming**: Connect to any API endpoint and stream data in real-time
- **Automatic Format Detection**: Supports JSON, CSV, and other formats automatically
- **Same Validation Pipeline**: Uses the same intelligent validation and quality assessment
- **Progress Tracking**: Real-time progress updates during streaming
- **Comprehensive Reporting**: Generates the same quality reports and analytics

## Usage

### Web Interface

1. Navigate to the Upload page
2. Click on the "API Streaming" tab
3. Enter the API endpoint URL
4. Click "Start Streaming"
5. Monitor progress and view results

### API Endpoint

```bash
POST /api/stream
Content-Type: application/json

{
  "api_url": "https://api.example.com/data",
  "action": "validate",
  "quality_threshold": 0.8,
  "timeout_seconds": 300
}
```

### Python API

```python
from services.api_streaming_service import stream_from_api

# Stream data from an API
results = await stream_from_api(
    api_url="https://api.example.com/data",
    action="validate",
    quality_threshold=0.8,
    timeout_seconds=300,
    output_dir="output"
)

print(f"Processed {results['total_records']} records")
print(f"Success rate: {results['success_rate']}")
```

## Supported API Formats

### JSON Arrays
```json
[
  {"id": 1, "name": "John", "email": "john@example.com"},
  {"id": 2, "name": "Jane", "email": "jane@example.com"}
]
```

### JSON Objects with Array Property
```json
{
  "data": [
    {"id": 1, "name": "John", "email": "john@example.com"},
    {"id": 2, "name": "Jane", "email": "jane@example.com"}
  ]
}
```

### CSV Format
```csv
id,name,email
1,John,john@example.com
2,Jane,jane@example.com
```

## Configuration Options

- **action**: Processing action (`validate`, `kafka`, `pipeline`)
- **quality_threshold**: Minimum quality score (0.0-1.0)
- **timeout_seconds**: Maximum streaming time (default: 300 seconds)

## Error Handling

The streaming service includes comprehensive error handling:

- **Connection Errors**: Invalid URLs, network issues
- **Format Errors**: Unsupported data formats
- **Timeout Errors**: Long-running streams
- **Validation Errors**: Data quality issues

## Output Files

The streaming process generates the same output files as file uploads:

- `good_data.csv` - Validated records
- `high_quality_data.csv` - High-quality records
- `bad_data.csv` - Invalid records with error details
- `processing_summary.json` - Processing statistics
- `data_quality_report.html` - Comprehensive quality report

## Testing

Run the test script to verify the API streaming functionality:

```bash
python test_api_streaming.py
```

This will test with sample APIs and generate test reports.

## Dependencies

The API streaming feature requires:

- `aiohttp` - Async HTTP client
- `pandas` - Data processing
- `asyncio` - Async operations

Install with:
```bash
pip install aiohttp
```

## Examples

### Example 1: JSONPlaceholder API
```python
results = await stream_from_api(
    api_url="https://jsonplaceholder.typicode.com/users",
    action="validate"
)
```

### Example 2: GitHub API
```python
results = await stream_from_api(
    api_url="https://api.github.com/users/octocat",
    action="validate"
)
```

### Example 3: Custom API with CSV
```python
results = await stream_from_api(
    api_url="https://api.example.com/export.csv",
    action="validate",
    timeout_seconds=600
)
```

## Best Practices

1. **API Design**: Ensure your API returns data in a consistent format
2. **Rate Limiting**: Be aware of API rate limits and adjust timeout accordingly
3. **Data Size**: Consider the amount of data being streamed
4. **Error Handling**: Implement proper error handling for your API endpoints
5. **Authentication**: For private APIs, ensure proper authentication is configured

## Troubleshooting

### Common Issues

1. **Connection Timeout**: Increase `timeout_seconds` parameter
2. **Invalid JSON**: Ensure API returns valid JSON format
3. **Empty Response**: Check API endpoint and data availability
4. **Memory Issues**: Process data in smaller batches for large streams

### Debug Mode

Enable debug logging to troubleshoot issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Integration with Existing Pipeline

The API streaming feature integrates seamlessly with the existing data ingestion pipeline:

- Uses the same validation services
- Generates the same report formats
- Supports the same quality metrics
- Compatible with Kafka integration
- Works with the existing job management system

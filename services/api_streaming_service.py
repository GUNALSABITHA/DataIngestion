"""
API Streaming Service for consuming data from external APIs and processing it through the pipeline.
"""

import asyncio
import aiohttp
import json
import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from pathlib import Path
import time

from services.data_validator import DataValidator
from services.data_reporter import DataQualityReporter
from services.file_processor import FileProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ApiStreamingService:
    """Service for streaming data from APIs and processing it through the validation pipeline"""
    
    def __init__(self):
        self.data_validator = DataValidator()
        self.reporter = DataQualityReporter()
        self.file_processor = FileProcessor()
        self.session = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def stream_and_process(self,
                                api_url: str,
                                action: str = "validate",
                                quality_threshold: float = 0.8,
                                timeout_seconds: int = 300,
                                output_dir: str = "output",
                                progress_callback: Optional[Callable[[float, str], None]] = None) -> Dict[str, Any]:
        """
        Stream data from API and process it through the validation pipeline
        
        Args:
            api_url: URL of the API endpoint to stream from
            action: Processing action (validate, kafka, pipeline)
            quality_threshold: Quality threshold for data classification
            timeout_seconds: Timeout for streaming operation
            output_dir: Output directory for results
            progress_callback: Callback function for progress updates
            
        Returns:
            Dict containing processing results
        """
        logger.info(f"Starting API streaming from: {api_url}")
        
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        try:
            # Step 1: Test API connection
            if progress_callback:
                progress_callback(0.1, "Testing API connection...")
            
            await self._test_api_connection(api_url)
            
            # Step 2: Stream data
            if progress_callback:
                progress_callback(0.2, "Streaming data from API...")
            
            raw_data = await self._stream_api_data(api_url, timeout_seconds, progress_callback)
            
            if not raw_data:
                raise ValueError("No data received from API")
            
            # Step 3: Process and validate data
            if progress_callback:
                progress_callback(0.6, "Processing and validating data...")
            
            results = await self._process_streamed_data(
                raw_data, 
                action, 
                quality_threshold, 
                output_dir,
                progress_callback
            )
            
            # Step 4: Generate reports
            if progress_callback:
                progress_callback(0.9, "Generating reports...")
            
            results = await self._generate_reports(results, output_dir)
            
            if progress_callback:
                progress_callback(1.0, "API streaming completed successfully")
            
            logger.info(f"API streaming completed successfully. Processed {results.get('total_records', 0)} records")
            return results
            
        except Exception as e:
            logger.error(f"API streaming failed: {str(e)}")
            raise
        finally:
            if self.session:
                await self.session.close()
                self.session = None
    
    async def _test_api_connection(self, api_url: str):
        """Test API connection and validate response format"""
        try:
            async with self.session.get(api_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    raise ValueError(f"API returned status {response.status}")
                
                # Try to parse as JSON
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' not in content_type:
                    logger.warning(f"API response is not JSON (content-type: {content_type})")
                
                # Read a small sample to validate format
                sample_data = await response.text()
                try:
                    json.loads(sample_data)
                except json.JSONDecodeError:
                    raise ValueError("API response is not valid JSON")
                
                logger.info("API connection test successful")
                
        except aiohttp.ClientError as e:
            raise ValueError(f"Failed to connect to API: {str(e)}")
    
    async def _stream_api_data(self, 
                              api_url: str, 
                              timeout_seconds: int,
                              progress_callback: Optional[Callable[[float, str], None]] = None) -> List[Dict[str, Any]]:
        """Stream data from API endpoint"""
        logger.info(f"Streaming data from {api_url}")
        
        all_data = []
        start_time = time.time()
        
        try:
            async with self.session.get(api_url, timeout=aiohttp.ClientTimeout(total=timeout_seconds)) as response:
                if response.status != 200:
                    raise ValueError(f"API returned status {response.status}")
                
                # Handle different response types
                content_type = response.headers.get('content-type', '').lower()
                
                if 'application/json' in content_type:
                    # JSON response
                    data = await response.json()
                    
                    if isinstance(data, list):
                        all_data = data
                    elif isinstance(data, dict):
                        # Try to find array in the response
                        for key, value in data.items():
                            if isinstance(value, list):
                                all_data = value
                                break
                        if not all_data:
                            all_data = [data]  # Single object
                    else:
                        raise ValueError("Unexpected JSON response format")
                
                elif 'text/csv' in content_type or 'text/plain' in content_type:
                    # CSV response
                    csv_text = await response.text()
                    df = pd.read_csv(pd.StringIO(csv_text))
                    all_data = df.to_dict('records')
                
                else:
                    # Try to parse as JSON anyway
                    try:
                        text_data = await response.text()
                        data = json.loads(text_data)
                        if isinstance(data, list):
                            all_data = data
                        elif isinstance(data, dict):
                            all_data = [data]
                    except json.JSONDecodeError:
                        raise ValueError(f"Unsupported content type: {content_type}")
                
                # Update progress
                if progress_callback:
                    progress_callback(0.5, f"Received {len(all_data)} records from API")
                
                logger.info(f"Successfully streamed {len(all_data)} records from API")
                return all_data
                
        except asyncio.TimeoutError:
            raise ValueError(f"API request timed out after {timeout_seconds} seconds")
        except aiohttp.ClientError as e:
            raise ValueError(f"Failed to stream data from API: {str(e)}")
    
    async def _process_streamed_data(self,
                                   raw_data: List[Dict[str, Any]],
                                   action: str,
                                   quality_threshold: float,
                                   output_dir: str,
                                   progress_callback: Optional[Callable[[float, str], None]] = None) -> Dict[str, Any]:
        """Process streamed data through validation pipeline"""
        logger.info(f"Processing {len(raw_data)} records through validation pipeline")
        
        # Reset validator for new data
        self.data_validator.reset_stats()
        
        # Validate data
        validation_results = self.data_validator.validate_batch(raw_data, source_file="api_stream")
        
        # Get data splits
        good_data = self.data_validator.get_good_data()
        bad_data = self.data_validator.get_bad_data()
        high_quality_data, low_quality_data = self.data_validator.get_quality_filtered_data(quality_threshold)
        
        # Save results to files
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        if good_data:
            good_df = pd.DataFrame(good_data)
            self.file_processor.save_dataframe(good_df, f"{output_dir}/good_data.csv")
        
        if high_quality_data:
            hq_df = pd.DataFrame(high_quality_data)
            self.file_processor.save_dataframe(hq_df, f"{output_dir}/high_quality_data.csv")
        
        if bad_data:
            # Save bad data with error details
            bad_data_records = []
            for result in bad_data:
                bad_data_records.append({
                    'original_data': result.original_data,
                    'errors': result.errors,
                    'warnings': result.warnings,
                    'quality_score': result.quality_score
                })
            
            bad_df = pd.DataFrame(bad_data_records)
            self.file_processor.save_dataframe(bad_df, f"{output_dir}/bad_data.csv")
        
        # Update progress
        if progress_callback:
            progress_callback(0.8, f"Validated {len(good_data)}/{len(raw_data)} records")
        
        # Get statistics
        stats = self.data_validator.get_statistics()
        
        results = {
            "status": "success",
            "total_records": len(raw_data),
            "valid_records": len(good_data),
            "invalid_records": len(bad_data),
            "high_quality_records": len(high_quality_data),
            "low_quality_records": len(low_quality_data),
            "success_rate": f"{(len(good_data) / len(raw_data) * 100):.2f}%" if raw_data else "0%",
            "statistics": stats,
            "output_directory": output_dir,
            "source": "api_stream"
        }
        
        return results
    
    async def _generate_reports(self, results: Dict[str, Any], output_dir: str) -> Dict[str, Any]:
        """Generate comprehensive reports for streamed data"""
        logger.info("Generating reports for streamed data")
        
        # Get validation results for reporting
        validation_results = self.data_validator.validation_results
        stats = self.data_validator.get_statistics()
        
        # Generate HTML report
        report_path = self.reporter.generate_comprehensive_report(
            validation_results,
            stats,
            output_format="html"
        )
        
        # Save processing summary
        summary_data = {
            "processing_summary": {
                "total_records": results["total_records"],
                "valid_records": results["valid_records"],
                "invalid_records": results["invalid_records"],
                "success_rate": results["success_rate"],
                "processing_duration": stats.processing_duration,
                "source": "api_stream"
            },
            "quality_metrics": {
                "excellent_quality": stats.excellent_quality,
                "good_quality": stats.good_quality,
                "fair_quality": stats.fair_quality,
                "poor_quality": stats.poor_quality
            },
            "timestamp": datetime.now().isoformat()
        }
        
        summary_file = Path(output_dir) / "processing_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2, default=str)
        
        results["report_path"] = str(report_path)
        results["summary_file"] = str(summary_file)
        
        return results


# Convenience function for direct usage
async def stream_from_api(api_url: str, 
                         action: str = "validate",
                         quality_threshold: float = 0.8,
                         timeout_seconds: int = 300,
                         output_dir: str = "output") -> Dict[str, Any]:
    """
    Convenience function to stream data from an API endpoint
    
    Args:
        api_url: URL of the API endpoint
        action: Processing action
        quality_threshold: Quality threshold
        timeout_seconds: Timeout for streaming
        output_dir: Output directory
        
    Returns:
        Dict containing processing results
    """
    async with ApiStreamingService() as service:
        return await service.stream_and_process(
            api_url=api_url,
            action=action,
            quality_threshold=quality_threshold,
            timeout_seconds=timeout_seconds,
            output_dir=output_dir
        )

"""
Main application orchestrating the complete data ingestion pipeline.
Provides command-line interface and orchestrates all services.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import json
import pandas as pd
import requests
import uuid
import time

# Add the project root to the path
sys.path.append(str(Path(__file__).parent))

from services.file_processor import FileProcessor
from services.kafka_producer_enhanced import EnhancedKafkaProducer
from services.kafka_consumer_enhanced import EnhancedKafkaConsumer
from services.data_validator import DataValidator
from services.data_reporter import DataQualityReporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------
# ✅ Utility function moved OUTSIDE the class
# -------------------------------------------------------
def load_data_from_api(api_url: str) -> pd.DataFrame:
    """
    Utility function to fetch data from an API endpoint and return as DataFrame.
    Assumes the API returns JSON data compatible with pandas.DataFrame.
    """
    logger.info(f"Fetching data from API: {api_url}")
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    # If data is a dict with a key containing records, adjust as needed
    if isinstance(data, dict):
        for key in ["records", "data", "results"]:
            if key in data:
                data = data[key]
                break
    df = pd.DataFrame(data)
    logger.info(f"Loaded {len(df)} records from API")
    return df


class DataIngestionPipeline:
    """Main orchestrator for the data ingestion pipeline"""

    def __init__(self, kafka_servers: list = None, default_topic: str = "data_ingestion_topic"):
        self.kafka_servers = kafka_servers or ["localhost:9092"]
        self.default_topic = default_topic
        self.file_processor = FileProcessor()
        self.data_validator = DataValidator()
        self.reporter = DataQualityReporter()

        logger.info("Data Ingestion Pipeline initialized")
        logger.info(f"Kafka servers: {self.kafka_servers}")
        logger.info(f"Default topic: {self.default_topic}")

    # -------------------------------------------------------
    # ✅ API to Kafka Pipeline
    # -------------------------------------------------------
    def run_api_to_kafka_pipeline(self, api_url: str, topic: str = None) -> Dict[str, Any]:
        topic = topic or self.default_topic
        results = {
            "pipeline_type": "api_to_kafka",
            "api_url": api_url,
            "topic": topic
        }
        try:
            df = load_data_from_api(api_url)
            records = self.file_processor.convert_dataframe_to_records(df, clean_data=True)
            results["api_processing"] = {
                "total_records_loaded": len(records),
                "columns": list(df.columns) if not df.empty else []
            }

            validation_results = self.data_validator.validate_batch(records, "api_input")
            good_data = self.data_validator.get_good_data()
            results["validation"] = {
                "total_validated": len(validation_results),
                "valid_records": len(good_data),
                "invalid_records": len(validation_results) - len(good_data),
            }

            producer = EnhancedKafkaProducer(self.kafka_servers)
            try:
                kafka_stats = producer.send_batch(good_data, topic)
                results["kafka_sending"] = kafka_stats
            finally:
                producer.close()

            stats = self.data_validator.get_statistics()
            report_path = self.reporter.generate_comprehensive_report(
                self.data_validator.validation_results,
                stats,
                output_format="html"
            )
            results["report_path"] = report_path
            results["status"] = "success"
        except Exception as e:
            results["status"] = "error"
            results["error"] = str(e)
            logger.error(f"API-to-Kafka pipeline failed: {str(e)}")
            raise
        return results

    # -------------------------------------------------------
    # ✅ File to Kafka pipeline (your old version)
    # -------------------------------------------------------
    def run_file_to_kafka_pipeline(self, input_file: str, topic: str = None, validate_before_send: bool = True):
        topic = topic or self.default_topic
        logger.info(f"Starting File-to-Kafka pipeline: {input_file} → {topic}")

        df = self.file_processor.load_data_from_file(input_file)
        records = self.file_processor.convert_dataframe_to_records(df, clean_data=True)

        if validate_before_send:
            validation_results = self.data_validator.validate_batch(records, input_file)
            good_data = self.data_validator.get_good_data()
        else:
            good_data = records

        producer = EnhancedKafkaProducer(self.kafka_servers)
        try:
            kafka_stats = producer.send_batch(good_data, topic)
        finally:
            producer.close()

        stats = self.data_validator.get_statistics()
        report_path = self.reporter.generate_comprehensive_report(
            self.data_validator.validation_results,
            stats,
            output_format="html"
        )

        return {
            "status": "success",
            "input_file": input_file,
            "sent_records": len(good_data),
            "report_path": report_path,
            "kafka_sending": kafka_stats
        }

    # -------------------------------------------------------
    # ✅ Kafka to Analysis pipeline
    # -------------------------------------------------------
    def run_kafka_to_analysis_pipeline(self, topic: str = None, max_records: int = None,
                                       timeout_seconds: int = 60,
                                       quality_threshold: float = 0.8,
                                       output_dir: str = "output"):
        topic = topic or self.default_topic
        logger.info(f"Starting Kafka-to-analysis pipeline from topic: {topic}")

        consumer = EnhancedKafkaConsumer(
            topic,
            self.kafka_servers,
            consumer_group=f"pipeline_{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest"
        )
        results = consumer.start_consuming(max_records=max_records,
                                           timeout_seconds=timeout_seconds,
                                           quality_threshold=quality_threshold)

        consumer.save_results_to_files(results, output_dir)

        stats = consumer.validator.get_statistics()
        report_path = self.reporter.generate_comprehensive_report(
            consumer.validator.validation_results,
            stats,
            output_format="html"
        )
        results["report_path"] = report_path
        results["output_directory"] = output_dir
        results["status"] = "success"
        consumer.stop_consuming()
        return results

    # … (keep validate_file_only and run_full_pipeline unchanged)
def main():
    parser = argparse.ArgumentParser(description="Enhanced Data Ingestion Pipeline")
    parser.add_argument("command", choices=["file-to-kafka", "kafka-to-analysis", "full-pipeline", "validate-file", "api-to-kafka"])
    parser.add_argument("--input-file", "-i")
    parser.add_argument("--api-url")
    parser.add_argument("--topic", "-t", default="data_ingestion_topic")
    parser.add_argument("--kafka-servers", "-k", nargs="+", default=["localhost:9092"])
    parser.add_argument("--max-records", "-m", type=int)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--quality-threshold", "-q", type=float, default=0.8)
    parser.add_argument("--output-dir", "-o", default="output")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    pipeline = DataIngestionPipeline(kafka_servers=args.kafka_servers, default_topic=args.topic)

    if args.command == "file-to-kafka":
        results = pipeline.run_file_to_kafka_pipeline(args.input_file, topic=args.topic)
    elif args.command == "api-to-kafka":
        results = pipeline.run_api_to_kafka_pipeline(args.api_url, topic=args.topic)
    elif args.command == "kafka-to-analysis":
        results = pipeline.run_kafka_to_analysis_pipeline(topic=args.topic, max_records=args.max_records,
                                                          timeout_seconds=args.timeout,
                                                          quality_threshold=args.quality_threshold,
                                                          output_dir=args.output_dir)
    elif args.command == "validate-file":
        results = pipeline.validate_file_only(args.input_file, output_dir=args.output_dir,
                                              quality_threshold=args.quality_threshold)
    elif args.command == "full-pipeline":
        results = pipeline.run_full_pipeline(args.input_file, topic=args.topic, max_records=args.max_records,
                                             timeout_seconds=args.timeout,
                                             quality_threshold=args.quality_threshold,
                                             output_dir=args.output_dir)

    print("\nPIPELINE EXECUTION SUMMARY")
    print(f"Status: {results.get('status')}")
    if 'report_path' in results:
        print(f"Report generated: {results['report_path']}")
    if 'output_directory' in results:
        print(f"Output directory: {results['output_directory']}")

    results_file = Path(args.output_dir) / "pipeline_results.json"
    results_file.parent.mkdir(parents=True, exist_ok=True)
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)


if __name__ == "__main__":
    main()

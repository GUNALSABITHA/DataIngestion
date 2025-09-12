"""
Test script for API streaming functionality
"""

import asyncio
import json
from services.api_streaming_service import ApiStreamingService

async def test_api_streaming():
    """Test the API streaming service with a sample API"""
    
    # Example API endpoints that return JSON data
    test_apis = [
        "https://jsonplaceholder.typicode.com/users",  # Sample user data
        "https://jsonplaceholder.typicode.com/posts",  # Sample post data
        "https://api.github.com/users/octocat",        # Single user data
    ]
    
    for api_url in test_apis:
        print(f"\n{'='*60}")
        print(f"Testing API: {api_url}")
        print(f"{'='*60}")
        
        try:
            async with ApiStreamingService() as service:
                results = await service.stream_and_process(
                    api_url=api_url,
                    action="validate",
                    quality_threshold=0.8,
                    timeout_seconds=30,
                    output_dir=f"test_output/{api_url.split('/')[-1]}"
                )
                
                print(f"✅ Success!")
                print(f"Total records: {results['total_records']}")
                print(f"Valid records: {results['valid_records']}")
                print(f"Invalid records: {results['invalid_records']}")
                print(f"Success rate: {results['success_rate']}")
                print(f"Report path: {results.get('report_path', 'N/A')}")
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")

async def test_local_api():
    """Test with a local mock API"""
    print(f"\n{'='*60}")
    print("Testing with local mock data")
    print(f"{'='*60}")
    
    # Create sample data
    sample_data = [
        {
            "id": 1,
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30,
            "city": "New York"
        },
        {
            "id": 2,
            "name": "Jane Smith",
            "email": "jane@example.com",
            "age": 25,
            "city": "Los Angeles"
        },
        {
            "id": 3,
            "name": "Bob Johnson",
            "email": "bob@example.com",
            "age": 35,
            "city": "Chicago"
        }
    ]
    
    # Save sample data to a temporary file
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(sample_data, f)
        temp_file = f.name
    
    try:
        # Create a simple HTTP server to serve the data
        import http.server
        import socketserver
        import threading
        import time
        
        # Start a simple HTTP server
        handler = http.server.SimpleHTTPRequestHandler
        with socketserver.TCPServer(("", 0), handler) as httpd:
            port = httpd.server_address[1]
            
            # Move the temp file to the server directory
            import shutil
            server_file = os.path.join(os.path.dirname(temp_file), "test_data.json")
            shutil.move(temp_file, server_file)
            
            # Start server in a separate thread
            server_thread = threading.Thread(target=httpd.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            
            # Give server time to start
            time.sleep(1)
            
            # Test the streaming service
            local_api_url = f"http://localhost:{port}/test_data.json"
            
            async with ApiStreamingService() as service:
                results = await service.stream_and_process(
                    api_url=local_api_url,
                    action="validate",
                    quality_threshold=0.8,
                    timeout_seconds=30,
                    output_dir="test_output/local_api"
                )
                
                print(f"✅ Local API test successful!")
                print(f"Total records: {results['total_records']}")
                print(f"Valid records: {results['valid_records']}")
                print(f"Invalid records: {results['invalid_records']}")
                print(f"Success rate: {results['success_rate']}")
                print(f"Report path: {results.get('report_path', 'N/A')}")
            
            # Clean up
            httpd.shutdown()
            os.unlink(server_file)
            
    except Exception as e:
        print(f"❌ Local API test error: {str(e)}")
        # Clean up temp file if it exists
        if os.path.exists(temp_file):
            os.unlink(temp_file)

if __name__ == "__main__":
    print("🧪 Testing API Streaming Service")
    print("This will test the new API streaming functionality")
    
    # Run tests
    asyncio.run(test_api_streaming())
    asyncio.run(test_local_api())
    
    print(f"\n{'='*60}")
    print("✅ All tests completed!")
    print("Check the 'test_output' directory for generated reports and data files.")
    print(f"{'='*60}")

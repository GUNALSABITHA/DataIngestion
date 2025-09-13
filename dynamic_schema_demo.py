"""
Dynamic Schema Demo Script
Shows how the dynamic schema system works with various data structures

This demonstrates:
1. Automatic schema inference from different data formats
2. Schema evolution tracking
3. Flexible vs structured storage
4. Quality metrics calculation
"""

import json
from datetime import datetime
from typing import Dict, List, Any

# Sample data with different structures to demonstrate dynamic schema capabilities

# Example 1: E-commerce customer data
ecommerce_data = [
    {
        "customer_id": "CUST001",
        "name": "John Doe",
        "email": "john.doe@email.com",
        "phone": "+1-555-0123",
        "registration_date": "2024-01-15",
        "total_orders": 15,
        "total_spent": 1250.50,
        "loyalty_tier": "Gold",
        "active": True,
        "preferences": {
            "newsletter": True,
            "sms_alerts": False
        }
    },
    {
        "customer_id": "CUST002", 
        "name": "Jane Smith",
        "email": "jane.smith@email.com",
        "phone": "+1-555-0124",
        "registration_date": "2024-02-20",
        "total_orders": 8,
        "total_spent": 875.25,
        "loyalty_tier": "Silver",
        "active": True,
        "last_login": "2024-09-12T10:30:00Z"
    }
]

# Example 2: IoT sensor data (different structure)
iot_sensor_data = [
    {
        "device_id": "SENSOR_001",
        "timestamp": "2024-09-13T14:30:00Z",
        "temperature": 23.5,
        "humidity": 65.2,
        "pressure": 1013.25,
        "battery_level": 85,
        "location": {
            "latitude": 40.7128,
            "longitude": -74.0060
        },
        "status": "online"
    },
    {
        "device_id": "SENSOR_002",
        "timestamp": "2024-09-13T14:31:00Z", 
        "temperature": 24.1,
        "humidity": 62.8,
        "pressure": 1012.95,
        "battery_level": 92,
        "location": {
            "latitude": 40.7589,
            "longitude": -73.9851
        },
        "status": "online",
        "signal_strength": -45
    }
]

# Example 3: Financial transaction data
financial_data = [
    {
        "transaction_id": "TXN_20240913001",
        "account_number": "ACC123456789",
        "transaction_type": "CREDIT",
        "amount": 2500.00,
        "currency": "USD",
        "timestamp": "2024-09-13T09:15:30Z",
        "description": "Salary deposit",
        "merchant": None,
        "category": "INCOME",
        "balance_after": 15750.00
    },
    {
        "transaction_id": "TXN_20240913002",
        "account_number": "ACC123456789", 
        "transaction_type": "DEBIT",
        "amount": 85.50,
        "currency": "USD",
        "timestamp": "2024-09-13T12:45:15Z",
        "description": "Coffee shop purchase",
        "merchant": "Starbucks #1234",
        "category": "FOOD_BEVERAGE",
        "balance_after": 15664.50,
        "location": "New York, NY"
    }
]

def demo_schema_inference():
    """Demonstrate automatic schema inference capabilities"""
    print("🔍 DYNAMIC SCHEMA INFERENCE DEMO")
    print("=" * 50)
    
    datasets = [
        ("E-commerce Customer Data", ecommerce_data),
        ("IoT Sensor Data", iot_sensor_data), 
        ("Financial Transaction Data", financial_data)
    ]
    
    for name, data in datasets:
        print(f"\n📊 Dataset: {name}")
        print(f"Records: {len(data)}")
        
        # Simulate schema inference
        inferred_schema = infer_schema_from_data(data, name)
        
        print(f"Fields detected: {len(inferred_schema['fields'])}")
        print("Field types:")
        for field_name, field_info in inferred_schema['fields'].items():
            field_type = field_info['type']
            required = "✓" if field_info['required'] else "✗"
            quality = field_info['quality_indicators']['overall_quality']
            print(f"  • {field_name}: {field_type} (Required: {required}, Quality: {quality:.2f})")
        
        print(f"Schema hash: {calculate_simple_hash(inferred_schema)}")
        print("-" * 30)

def infer_schema_from_data(data: List[Dict[str, Any]], schema_name: str) -> Dict[str, Any]:
    """Simplified schema inference simulation"""
    if not data:
        return {'fields': {}, 'metadata': {'record_count': 0}}
    
    # Collect all field names
    all_fields = set()
    for record in data:
        all_fields.update(record.keys())
    
    # Infer field types and properties
    schema_fields = {}
    for field_name in all_fields:
        field_values = [record.get(field_name) for record in data]
        non_null_values = [v for v in field_values if v is not None]
        
        # Infer type
        field_type = infer_field_type(non_null_values)
        
        # Calculate properties
        required = len(non_null_values) == len(data)  # No nulls
        completeness = len(non_null_values) / len(data)
        uniqueness = len(set(str(v) for v in non_null_values)) / len(non_null_values) if non_null_values else 0
        
        schema_fields[field_name] = {
            'type': field_type,
            'required': required,
            'constraints': {},
            'quality_indicators': {
                'completeness': completeness,
                'consistency': 0.95,  # Simulated
                'validity': 0.98,     # Simulated 
                'overall_quality': (completeness + 0.95 + 0.98) / 3
            },
            'statistics': {
                'total_values': len(data),
                'unique_values': len(set(str(v) for v in non_null_values)) if non_null_values else 0,
                'null_count': len(data) - len(non_null_values)
            }
        }
    
    return {
        'schema_name': schema_name,
        'fields': schema_fields,
        'metadata': {
            'record_count': len(data),
            'inferred_at': datetime.utcnow().isoformat(),
            'inference_method': 'dynamic_demo'
        }
    }

def infer_field_type(values: List[Any]) -> str:
    """Simple type inference"""
    if not values:
        return 'string'
    
    # Check if all values are integers
    if all(isinstance(v, int) for v in values):
        return 'integer'
    
    # Check if all values are floats or numbers
    if all(isinstance(v, (int, float)) for v in values):
        return 'float'
    
    # Check if all values are booleans
    if all(isinstance(v, bool) for v in values):
        return 'boolean'
    
    # Check for datetime patterns
    sample_str = str(values[0])
    if any(pattern in sample_str for pattern in ['T', 'Z', '-', ':']):
        try:
            from dateutil import parser
            parser.parse(sample_str)
            return 'datetime'
        except:
            pass
    
    # Check for email pattern
    if '@' in sample_str and '.' in sample_str:
        return 'email'
    
    # Check for phone pattern
    if any(char in sample_str for char in ['+', '-', '(', ')']):
        return 'phone'
    
    # Check for nested objects
    if isinstance(values[0], dict):
        return 'json'
    
    return 'string'

def calculate_simple_hash(schema_definition: Dict[str, Any]) -> str:
    """Calculate simple hash for demo"""
    import hashlib
    schema_str = json.dumps(schema_definition, sort_keys=True)
    return hashlib.md5(schema_str.encode()).hexdigest()[:8]

def demo_storage_methods():
    """Demonstrate different storage approaches"""
    print("\n🗄️ STORAGE METHOD COMPARISON")
    print("=" * 50)
    
    sample_data = ecommerce_data[:1]  # Use first record
    
    print("📝 Sample Record:")
    print(json.dumps(sample_data[0], indent=2))
    
    print("\n🔹 FLEXIBLE STORAGE (JSONB):")
    print("Advantages:")
    print("  • Handles any schema without table modifications")
    print("  • Perfect for schema evolution")
    print("  • Rich JSON querying capabilities") 
    print("  • No DDL operations needed")
    print("\nTable structure:")
    print("  dynamic_data:")
    print("    - id (UUID)")
    print("    - schema_id (UUID)")  
    print("    - data_payload (JSONB) ← All data here")
    print("    - validation_status, quality_score, etc.")
    
    print("\n🔸 STRUCTURED STORAGE (Dynamic Tables):")
    print("Advantages:")
    print("  • Better query performance for known fields")
    print("  • Proper data types and constraints") 
    print("  • Traditional SQL operations")
    print("  • Better storage efficiency")
    print("\nGenerated table structure:")
    print("  data_ecommerce_customer_data_1:")
    print("    - id (UUID)")
    print("    - customer_id (String)")
    print("    - name (String)")
    print("    - email (String)")
    print("    - phone (String)")
    print("    - total_orders (Integer)")
    print("    - total_spent (Float)")
    print("    - active (Boolean)")
    print("    - preferences (JSON)")

def demo_schema_evolution():
    """Demonstrate schema evolution tracking"""
    print("\n🔄 SCHEMA EVOLUTION DEMO")
    print("=" * 50)
    
    # Version 1: Original customer schema
    v1_data = [
        {
            "customer_id": "CUST001",
            "name": "John Doe", 
            "email": "john.doe@email.com"
        }
    ]
    
    # Version 2: Added phone field
    v2_data = [
        {
            "customer_id": "CUST001",
            "name": "John Doe",
            "email": "john.doe@email.com", 
            "phone": "+1-555-0123"
        }
    ]
    
    # Version 3: Added loyalty program fields
    v3_data = [
        {
            "customer_id": "CUST001",
            "name": "John Doe",
            "email": "john.doe@email.com",
            "phone": "+1-555-0123",
            "loyalty_tier": "Gold",
            "total_points": 1500
        }
    ]
    
    versions = [
        ("v1.0", "Initial customer data", v1_data),
        ("v2.0", "Added phone support", v2_data), 
        ("v3.0", "Added loyalty program", v3_data)
    ]
    
    print("Schema Evolution for 'customer_data':")
    
    previous_schema = None
    for version, description, data in versions:
        schema = infer_schema_from_data(data, "customer_data")
        field_count = len(schema['fields'])
        schema_hash = calculate_simple_hash(schema)
        
        print(f"\n📌 {version}: {description}")
        print(f"   Fields: {field_count}")
        print(f"   Hash: {schema_hash}")
        
        if previous_schema:
            changes = compare_schemas(previous_schema, schema)
            if changes:
                print("   Changes:")
                for change in changes:
                    print(f"     • {change}")
            else:
                print("   Changes: None")
        
        previous_schema = schema

def compare_schemas(old_schema: Dict, new_schema: Dict) -> List[str]:
    """Compare two schemas and return list of changes"""
    changes = []
    
    old_fields = old_schema.get('fields', {})
    new_fields = new_schema.get('fields', {})
    
    # Added fields
    for field in new_fields:
        if field not in old_fields:
            changes.append(f"Added field: {field} ({new_fields[field]['type']})")
    
    # Removed fields 
    for field in old_fields:
        if field not in new_fields:
            changes.append(f"Removed field: {field}")
    
    # Modified fields
    for field in set(old_fields.keys()) & set(new_fields.keys()):
        if old_fields[field]['type'] != new_fields[field]['type']:
            changes.append(f"Type changed: {field} ({old_fields[field]['type']} → {new_fields[field]['type']})")
    
    return changes

def demo_api_usage():
    """Show how to use the dynamic schema API endpoints"""
    print("\n🌐 API USAGE EXAMPLES")
    print("=" * 50)
    
    print("1️⃣ Upload file with dynamic schema detection:")
    print("POST /api/upload-dynamic")
    print("Form data: file=your_file.csv, storage_method=flexible")
    print()
    
    print("2️⃣ Process API data with schema inference:")
    print("POST /api/dynamic-schema/process")
    print("Body: {")
    print('  "data_records": [...],')
    print('  "source_name": "api_endpoint_name",')
    print('  "storage_method": "structured"')
    print("}")
    print()
    
    print("3️⃣ List all discovered schemas:")
    print("GET /api/dynamic-schema/schemas")
    print()
    
    print("4️⃣ Query data by schema:")
    print("GET /api/dynamic-schema/schemas/{schema_id}/data?limit=100")
    print()
    
    print("5️⃣ View schema evolution:")
    print("GET /api/dynamic-schema/schemas/customer_data/evolution")
    print()
    
    print("6️⃣ Infer schema without storing data:")
    print("POST /api/dynamic-schema/infer")
    print("Body: {")
    print('  "data_records": [...],')
    print('  "source_name": "sample_analysis"')
    print("}")

if __name__ == "__main__":
    print("🎯 DYNAMIC DATA MODELS DEMO")
    print("=" * 60)
    print("This demo shows how the dynamic schema system handles")
    print("varying data structures automatically!")
    print()
    
    demo_schema_inference()
    demo_storage_methods()
    demo_schema_evolution()
    demo_api_usage()
    
    print("\n" + "=" * 60)
    print("✨ Key Benefits of Dynamic Schema System:")
    print("  🔄 Automatic schema inference from any data format")
    print("  📊 Schema evolution tracking and versioning")
    print("  🗄️ Choice of storage: flexible (JSONB) or structured (tables)")
    print("  📈 Built-in data quality assessment")
    print("  🔍 Comprehensive querying capabilities")
    print("  🚀 Zero-configuration data ingestion")
    print("=" * 60)
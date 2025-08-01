#!/usr/bin/env python3
"""
Database Connection Test Script
Run this to test PostgreSQL database creation and connection
"""

import os
import sys

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from edi271_parser import DatabaseManager, PSYCOPG2_AVAILABLE, EligibilityResponse

def test_psycopg2_availability():
    """Test if psycopg2 is available"""
    print("Testing psycopg2 availability...")
    if PSYCOPG2_AVAILABLE:
        print("✅ psycopg2 is available")
        return True
    else:
        print("❌ psycopg2 is not available")
        print("Install with: pip install psycopg2-binary")
        return False

def test_database_connection(config):
    """Test database connection and basic operations"""
    if not PSYCOPG2_AVAILABLE:
        print("❌ Cannot test database - psycopg2 not available")
        return False
    
    try:
        print(f"Testing connection to {config['host']}:{config['port']}/{config['database']}...")
        db_manager = DatabaseManager(config)
        
        # Test connection pool initialization
        print("Initializing connection pool...")
        db_manager.initialize_pool(minconn=1, maxconn=2)
        print("✅ Connection pool initialized")
        
        # Test schema creation
        print("Creating database schema...")
        db_manager.create_schema()
        print("✅ Database schema created successfully")
        
        # Test inserting sample data
        print("Testing data insertion...")
        sample_data = EligibilityResponse(
            transaction_id="TEST123",
            payer_name="Test Insurance",
            subscriber_name="Test, User",
            member_id="TEST_MEMBER_001",
            status="Active"
        )
        
        record_id = db_manager.insert_eligibility_response(sample_data)
        print(f"✅ Sample data inserted with ID: {record_id}")
        
        # Test data retrieval
        print("Testing data retrieval...")
        retrieved_data = db_manager.get_eligibility_by_member_id("TEST_MEMBER_001")
        if retrieved_data:
            print(f"✅ Data retrieved: {retrieved_data['subscriber_name']}")
        else:
            print("❌ Failed to retrieve data")
            return False
        
        # Test status update
        print("Testing status update...")
        success = db_manager.update_eligibility_status(record_id, "Inactive")
        if success:
            print("✅ Status updated successfully")
        else:
            print("❌ Failed to update status")
            return False
        
        # Clean up
        print("Closing connection pool...")
        db_manager.close_pool()
        print("✅ Connection pool closed")
        
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

def main():
    """Main test function"""
    print("Healthcare Eligibility Parser - Database Test")
    print("=" * 50)
    
    # Test psycopg2 availability
    if not test_psycopg2_availability():
        return 1
    
    # Get database configuration from environment or use defaults
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'eligibility_test'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }
    
    print(f"\nUsing database configuration:")
    print(f"Host: {config['host']}")
    print(f"Port: {config['port']}")
    print(f"Database: {config['database']}")
    print(f"User: {config['user']}")
    print(f"Password: {'*' * len(config['password']) if config['password'] else '(empty)'}")
    
    print(f"\nTo use custom settings, set environment variables:")
    print(f"DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
    
    # Test database operations
    if test_database_connection(config):
        print("\n🎉 All database tests passed!")
        print("\nYou can now use the parser with database support:")
        print(f"python src/edi271_parser.py data/sample_271.edi \\")
        print(f"  --db-host {config['host']} \\")
        print(f"  --db-user {config['user']} \\")
        print(f"  --db-name {config['database']} \\")
        print(f"  --save-to-db \\")
        print(f"  --create-schema")
        return 0
    else:
        print("\n❌ Database tests failed")
        print("\nTroubleshooting:")
        print("1. Ensure PostgreSQL is running")
        print("2. Check database credentials")
        print("3. Verify database exists (or use --create-schema)")
        print("4. Check network connectivity")
        return 1

if __name__ == "__main__":
    sys.exit(main())
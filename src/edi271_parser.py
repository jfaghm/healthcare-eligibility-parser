#!/usr/bin/env python3
"""
EDI 271 Parser with Optional PostgreSQL Support
Parses EDI files and optionally stores results in PostgreSQL database
"""

import argparse
import json
import os
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any
from contextlib import contextmanager

try:
    import psycopg2
    from psycopg2 import pool
    from psycopg2.extras import RealDictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

@dataclass
class EligibilityResponse:
    transaction_id: str = ""
    response_date: str = ""
    payer_name: str = ""
    provider_name: str = ""
    provider_npi: str = ""
    subscriber_name: str = ""
    member_id: str = ""
    group_number: str = ""
    employer: str = ""
    address: str = ""
    date_of_birth: str = ""
    gender: str = ""
    plan_name: str = ""
    individual_deductible: str = ""
    individual_deductible_met: str = ""
    preventative_care_copay: str = ""
    mental_health_covered: str = "Not specified"
    status: str = "Active"
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()

class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 not available. Install with: pip install psycopg2-binary")
        
        self.config = connection_config
        self.connection_pool = None
        self.logger = logging.getLogger(__name__)
        
    def initialize_pool(self, minconn=1, maxconn=10):
        """Initialize connection pool"""
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn, maxconn, **self.config
            )
            self.logger.info("Database connection pool initialized")
        except psycopg2.Error as e:
            self.logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        if not self.connection_pool:
            self.initialize_pool()
        
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def create_schema(self):
        """Create database schema for eligibility responses"""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS eligibility_responses (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(50),
            response_date DATE,
            payer_name VARCHAR(255),
            provider_name VARCHAR(255),
            provider_npi VARCHAR(20),
            subscriber_name VARCHAR(255),
            member_id VARCHAR(50),
            group_number VARCHAR(50),
            employer VARCHAR(255),
            address TEXT,
            date_of_birth DATE,
            gender VARCHAR(10),
            plan_name VARCHAR(255),
            individual_deductible VARCHAR(20),
            individual_deductible_met VARCHAR(20),
            preventative_care_copay VARCHAR(20),
            mental_health_covered VARCHAR(20) DEFAULT 'Not specified',
            status VARCHAR(50) DEFAULT 'Active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_transaction_id ON eligibility_responses(transaction_id);
        CREATE INDEX IF NOT EXISTS idx_member_id ON eligibility_responses(member_id);
        CREATE INDEX IF NOT EXISTS idx_created_at ON eligibility_responses(created_at);
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(schema_sql)
                    conn.commit()
                    self.logger.info("Database schema created successfully")
        except psycopg2.Error as e:
            self.logger.error(f"Failed to create schema: {e}")
            raise
    
    def insert_eligibility_response(self, data: EligibilityResponse) -> int:
        """Insert eligibility response into database"""
        insert_sql = """
        INSERT INTO eligibility_responses (
            transaction_id, response_date, payer_name, provider_name, provider_npi,
            subscriber_name, member_id, group_number, employer, address,
            date_of_birth, gender, plan_name, individual_deductible, 
            individual_deductible_met, preventative_care_copay, mental_health_covered, status
        ) VALUES (
            %(transaction_id)s, %(response_date)s, %(payer_name)s, %(provider_name)s, %(provider_npi)s,
            %(subscriber_name)s, %(member_id)s, %(group_number)s, %(employer)s, %(address)s,
            %(date_of_birth)s, %(gender)s, %(plan_name)s, %(individual_deductible)s,
            %(individual_deductible_met)s, %(preventative_care_copay)s, %(mental_health_covered)s, %(status)s
        ) RETURNING id;
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Convert date strings to proper format for PostgreSQL
                    data_dict = asdict(data)
                    data_dict['response_date'] = self._parse_date(data.response_date)
                    data_dict['date_of_birth'] = self._parse_date(data.date_of_birth)
                    
                    cursor.execute(insert_sql, data_dict)
                    record_id = cursor.fetchone()[0]
                    conn.commit()
                    self.logger.info(f"Inserted eligibility response with ID: {record_id}")
                    return record_id
        except psycopg2.Error as e:
            self.logger.error(f"Failed to insert eligibility response: {e}")
            raise
    
    def get_eligibility_by_member_id(self, member_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve eligibility response by member ID"""
        select_sql = """
        SELECT * FROM eligibility_responses 
        WHERE member_id = %s 
        ORDER BY created_at DESC 
        LIMIT 1;
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(select_sql, (member_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except psycopg2.Error as e:
            self.logger.error(f"Failed to retrieve eligibility response: {e}")
            raise
    
    def update_eligibility_status(self, record_id: int, status: str) -> bool:
        """Update eligibility response status"""
        update_sql = """
        UPDATE eligibility_responses 
        SET status = %s, updated_at = CURRENT_TIMESTAMP 
        WHERE id = %s;
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(update_sql, (status, record_id))
                    conn.commit()
                    return cursor.rowcount > 0
        except psycopg2.Error as e:
            self.logger.error(f"Failed to update eligibility status: {e}")
            raise
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to PostgreSQL format"""
        if not date_str:
            return None
        try:
            # Handle MM/DD/YYYY format
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 3:
                    return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            return date_str
        except (ValueError, IndexError):
            self.logger.warning(f"Could not parse date: {date_str}")
            return None
    
    def close_pool(self):
        """Close all connections in the pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.logger.info("Database connection pool closed")

class SimpleEDI271Parser:
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.data = EligibilityResponse()
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    def parse_file(self, file_path: str, save_to_db: bool = False) -> EligibilityResponse:
        with open(file_path, 'r') as f:
            content = f.read().strip()
        result = self.parse_content(content)
        
        if save_to_db and self.db_manager:
            try:
                record_id = self.db_manager.insert_eligibility_response(result)
                self.logger.info(f"Saved eligibility response to database with ID: {record_id}")
            except Exception as e:
                self.logger.error(f"Failed to save to database: {e}")
                
        return result
    
    def parse_content(self, content: str) -> EligibilityResponse:
        segments = [seg.strip() for seg in content.split('~') if seg.strip()]
        
        for segment in segments:
            if not segment:
                continue
            elements = segment.split('*')
            segment_type = elements[0]
            
            if segment_type == 'ST' and len(elements) > 2:
                self.data.transaction_id = elements[2]
            
            elif segment_type == 'BHT' and len(elements) > 4:
                date_str = elements[4]
                if len(date_str) == 8:
                    self.data.response_date = f"{date_str[4:6]}/{date_str[6:8]}/{date_str[:4]}"
            
            elif segment_type == 'NM1':
                if len(elements) > 1:
                    entity_type = elements[1]
                    if entity_type == 'PR' and len(elements) > 3:  # Payer
                        self.data.payer_name = elements[3]
                    elif entity_type == '1P' and len(elements) > 3:  # Provider
                        self.data.provider_name = elements[3]
                        if len(elements) > 9:
                            self.data.provider_npi = elements[9]
                    elif entity_type == 'IL':  # Subscriber
                        if len(elements) > 4:
                            last = elements[3] if len(elements) > 3 else ""
                            first = elements[4] if len(elements) > 4 else ""
                            middle = elements[5] if len(elements) > 5 else ""
                            self.data.subscriber_name = f"{last}, {first}"
                            if middle:
                                self.data.subscriber_name += f" {middle}"
                        if len(elements) > 9:
                            self.data.member_id = elements[9]
            
            elif segment_type == 'REF' and len(elements) > 2:
                ref_type = elements[1]
                if ref_type == '18':
                    self.data.group_number = elements[2]
                elif ref_type == '6P':
                    self.data.employer = elements[2]
            
            elif segment_type == 'N3' and len(elements) > 1:
                self.data.address = elements[1]
            
            elif segment_type == 'N4' and len(elements) > 3 and self.data.address:
                city = elements[1]
                state = elements[2]
                zip_code = elements[3]
                self.data.address += f", {city}, {state} {zip_code}"
            
            elif segment_type == 'DMG':
                if len(elements) > 2:
                    date_str = elements[2]
                    if len(date_str) == 8:
                        self.data.date_of_birth = f"{date_str[4:6]}/{date_str[6:8]}/{date_str[:4]}"
                if len(elements) > 3:
                    self.data.gender = "Female" if elements[3] == 'F' else "Male"
            
            elif segment_type == 'EB':
                if len(elements) > 5 and 'PLAN' in elements[5].upper():
                    self.data.plan_name = elements[5]
                
                # Parse financial amounts
                if len(elements) > 7:
                    amount = elements[7]
                    if amount and amount.replace('.', '').replace('-', '').isdigit():
                        formatted_amount = f"${float(amount):,.2f}"
                        
                        coverage_level = elements[2] if len(elements) > 2 else ""
                        time_period = elements[6] if len(elements) > 6 else ""
                        benefit_info = elements[4] if len(elements) > 4 else ""
                        
                        # Check for deductible indicators
                        if coverage_level == 'IND':
                            if time_period == '22':
                                if not self.data.individual_deductible:
                                    self.data.individual_deductible = formatted_amount
                            elif time_period == '29':
                                if not self.data.individual_deductible_met:
                                    self.data.individual_deductible_met = formatted_amount
                        
                        # Check for co-pay indicators (A3 = Preventative Care, 98 = Preventive/Wellness)
                        if benefit_info in ['A3', '98'] or 'PREVENTIVE' in elements[5].upper() if len(elements) > 5 else False:
                            if not self.data.preventative_care_copay:
                                self.data.preventative_care_copay = formatted_amount
                
                # Also check for co-pay information in other positions
                if len(elements) > 1:
                    benefit_type = elements[1]
                    # B = Coverage modifier, C = Coverage amount
                    if benefit_type in ['B', 'C'] and len(elements) > 4:
                        benefit_info = elements[4] if len(elements) > 4 else ""
                        # Look for preventative care codes
                        if benefit_info in ['A3', '98'] and len(elements) > 7:
                            amount = elements[7]
                            if amount and amount.replace('.', '').replace('-', '').isdigit():
                                if not self.data.preventative_care_copay:
                                    self.data.preventative_care_copay = f"${float(amount):,.2f}"
                
                # Check for Mental Health (MH) coverage in benefit codes
                if len(elements) > 3 and elements[3]:
                    if '^' in elements[3]:
                        benefit_codes = elements[3].split('^')
                        if 'MH' in benefit_codes:
                            self.data.mental_health_covered = "Yes"
                    elif elements[3] == 'MH':
                        self.data.mental_health_covered = "Yes"
        
        return self.data
    
    def get_member_eligibility(self, member_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve eligibility data from database by member ID"""
        if not self.db_manager:
            self.logger.warning("No database manager configured")
            return None
        
        try:
            return self.db_manager.get_eligibility_by_member_id(member_id)
        except Exception as e:
            self.logger.error(f"Failed to retrieve member eligibility: {e}")
            return None
    
    def update_member_status(self, record_id: int, status: str) -> bool:
        """Update eligibility status in database"""
        if not self.db_manager:
            self.logger.warning("No database manager configured")
            return False
        
        try:
            return self.db_manager.update_eligibility_status(record_id, status)
        except Exception as e:
            self.logger.error(f"Failed to update member status: {e}")
            return False

def generate_html_report(data: EligibilityResponse, output_file: str):
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>EDI 271 Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .container {{ max-width: 800px; }}
        ul {{ line-height: 1.6; }}
        .header {{ color: #333; border-bottom: 2px solid #007acc; padding-bottom: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1 class="header">EDI 271 Eligibility Response Report</h1>
        <ul>
            <li><strong>Transaction ID:</strong> {data.transaction_id}</li>
            <li><strong>Response Date:</strong> {data.response_date}</li>
            <li><strong>Payer:</strong> {data.payer_name}</li>
            <li><strong>Provider:</strong> {data.provider_name}</li>
            <li><strong>Provider NPI:</strong> {data.provider_npi}</li>
            <li><strong>Subscriber:</strong> {data.subscriber_name}</li>
            <li><strong>Member ID:</strong> {data.member_id}</li>
            <li><strong>Group Number:</strong> {data.group_number}</li>
            <li><strong>Employer:</strong> {data.employer}</li>
            <li><strong>Address:</strong> {data.address}</li>
            <li><strong>Date of Birth:</strong> {data.date_of_birth}</li>
            <li><strong>Gender:</strong> {data.gender}</li>
            <li><strong>Plan:</strong> {data.plan_name}</li>
            <li><strong>Individual Deductible:</strong> {data.individual_deductible}</li>
            <li><strong>Individual Deductible Met:</strong> {data.individual_deductible_met}</li>
            <li><strong>Mental Health Covered:</strong> {data.mental_health_covered}</li>
            <li><strong>Status:</strong> {data.status}</li>
        </ul>
    </div>
</body>
</html>
"""
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(html_content)
    print(f"HTML report saved to: {output_file}")

def create_db_manager_from_args(args) -> Optional[DatabaseManager]:
    """Create database manager from command line arguments"""
    if not args.db_host:
        return None
    
    if not PSYCOPG2_AVAILABLE:
        print("Warning: psycopg2 not available. Database features disabled.")
        return None
    
    config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_password or os.getenv('DB_PASSWORD', ''),
    }
    
    try:
        db_manager = DatabaseManager(config)
        if args.create_schema:
            db_manager.create_schema()
            print("Database schema created successfully")
        return db_manager
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='EDI 271 Parser with Optional PostgreSQL Support')
    parser.add_argument('input_file', nargs='?', help='Path to EDI 271 file')
    parser.add_argument('--html-output', help='Output path for HTML report')
    parser.add_argument('--json-output', help='Output path for JSON data')
    
    # Database options
    db_group = parser.add_argument_group('Database Options')
    db_group.add_argument('--db-host', help='PostgreSQL host')
    db_group.add_argument('--db-port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    db_group.add_argument('--db-name', default='eligibility_db', help='Database name (default: eligibility_db)')
    db_group.add_argument('--db-user', help='Database username')
    db_group.add_argument('--db-password', help='Database password (or set DB_PASSWORD env var)')
    db_group.add_argument('--save-to-db', action='store_true', help='Save parsed data to database')
    db_group.add_argument('--create-schema', action='store_true', help='Create database schema')
    
    # Query options
    query_group = parser.add_argument_group('Query Options')
    query_group.add_argument('--get-member', help='Retrieve eligibility by member ID')
    query_group.add_argument('--update-status', nargs=2, metavar=('RECORD_ID', 'STATUS'), 
                            help='Update eligibility status (record_id status)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize database manager if needed
        db_manager = create_db_manager_from_args(args)
        parser_obj = SimpleEDI271Parser(db_manager)
        
        # Handle query operations
        if args.get_member:
            if not db_manager:
                print("Error: Database connection required for member queries")
                return 1
            
            result = parser_obj.get_member_eligibility(args.get_member)
            if result:
                print("\n=== MEMBER ELIGIBILITY ===")
                for key, value in result.items():
                    print(f"{key}: {value}")
            else:
                print(f"No eligibility data found for member ID: {args.get_member}")
            return 0
        
        if args.update_status:
            if not db_manager:
                print("Error: Database connection required for status updates")
                return 1
            
            record_id, status = args.update_status
            success = parser_obj.update_member_status(int(record_id), status)
            if success:
                print(f"Successfully updated record {record_id} status to: {status}")
            else:
                print(f"Failed to update record {record_id}")
            return 0
        
        # Parse file if provided
        if not args.input_file:
            print("Error: input_file is required for parsing operations")
            parser.print_help()
            return 1
        
        print(f"Parsing EDI file: {args.input_file}")
        data = parser_obj.parse_file(args.input_file, save_to_db=args.save_to_db)
        
        if args.html_output:
            generate_html_report(data, args.html_output)
        
        if args.json_output:
            os.makedirs(os.path.dirname(args.json_output), exist_ok=True)
            with open(args.json_output, 'w') as f:
                json.dump(asdict(data), f, indent=2)
            print(f"JSON saved to: {args.json_output}")
        
        print("\n=== PARSING RESULTS ===")
        print(f"Subscriber: {data.subscriber_name}")
        print(f"Payer: {data.payer_name}")
        print(f"Plan: {data.plan_name}")
        print(f"Transaction ID: {data.transaction_id}")
        print(f"Mental Health Covered: {data.mental_health_covered}")
        
        if args.save_to_db and db_manager:
            print("Data saved to database successfully")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        # Clean up database connections
        if 'db_manager' in locals() and db_manager:
            db_manager.close_pool()

if __name__ == "__main__":
    main()
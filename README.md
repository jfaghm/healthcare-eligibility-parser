# Healthcare Eligibility Parser

EDI 271 Parser with optional PostgreSQL database support for healthcare eligibility data processing.

## Features

- **EDI 271 Parsing**: Parse healthcare eligibility response files
- **Multiple Output Formats**: Generate HTML reports and JSON data
- **Optional Database Storage**: Store parsed data in PostgreSQL with connection pooling
- **Robust Error Handling**: Graceful fallback when database is unavailable
- **Query Operations**: Retrieve and update eligibility records from database

## Structure
- `src/`: Source code
- `test/`: Test code and database validation
- `data/`: Sample EDI files
- `output/`: Generated reports

## Setup

### Basic Installation
```bash
pip install -r requirements.txt
```

### Database Setup (Optional)
1. Install PostgreSQL (if using database features)
2. Create a database for eligibility data
3. Set environment variables for database connection

## Usage

### Basic File Parsing (No Database)
```bash
# Parse EDI file and generate HTML report
python src/edi271_parser.py data/sample_271.edi --html-output output/report.html

# Parse and generate JSON output
python src/edi271_parser.py data/sample_271.edi --json-output output/data.json
```

### Database Operations

#### Create Database Schema
```bash
python src/edi271_parser.py --db-host localhost --db-user postgres --db-name eligibility_db --create-schema
```

#### Parse and Save to Database
```bash
python src/edi271_parser.py data/sample_271.edi \
  --db-host localhost \
  --db-user postgres \
  --db-name eligibility_db \
  --save-to-db \
  --html-output output/report.html
```

#### Query Member Eligibility
```bash
python src/edi271_parser.py --db-host localhost --db-user postgres --db-name eligibility_db --get-member "123456789"
```

#### Update Member Status
```bash
python src/edi271_parser.py --db-host localhost --db-user postgres --db-name eligibility_db --update-status 1 "Inactive"
```

### Database Configuration

Set environment variables for secure database connection:
```bash
export DB_PASSWORD="your_password"
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="eligibility_db"
export DB_USER="postgres"
```

## Testing

### Run Unit Tests
```bash
pytest test/test_edi271_parser.py -v
```

### Test Database Connection
```bash
python test_database.py
```

## Database Schema

The parser automatically creates the following table structure:

```sql
CREATE TABLE eligibility_responses (
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
    status VARCHAR(50) DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Error Handling

- **Database Unavailable**: Parser continues to work without database features
- **Connection Failures**: Automatic connection pooling with retry logic
- **Invalid Data**: Graceful parsing with detailed logging
- **Permission Issues**: Clear error messages with troubleshooting guidance

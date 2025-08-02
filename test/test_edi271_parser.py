#!/usr/bin/env python3
"""
Tests for EDI 271 Parser with PostgreSQL support
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from edi271_parser import (
    EligibilityResponse, 
    SimpleEDI271Parser, 
    DatabaseManager,
    PSYCOPG2_AVAILABLE
)

# Sample EDI 271 content for testing
SAMPLE_EDI_271 = """ST*271*1234567890~
BHT*0022*11*1*20240801*1200~
HL*1**20*1~
NM1*PR*2*BLUE CROSS BLUE SHIELD*****PI*12345~
HL*2*1*21*1~
NM1*1P*2*PROVIDER NAME*****XX*1234567890~
HL*3*2*22*0~
TRN*2*1234567890*1234567890~
NM1*IL*1*DOE*JOHN*M***MI*123456789~
REF*18*GRP12345~
REF*6P*ACME CORP~
N3*123 MAIN ST~
N4*ANYTOWN*CA*90210~
DMG*D8*19900101*M~
DTP*346*D8*20240801~
EB*1*IND**30*MEDICAL PLAN*22*2500.00~
EB*1*IND**30*MEDICAL PLAN*29*500.00~
EB*B*IND**A3*PREVENTIVE CARE*23*25.00~
SE*20*1234567890~"""

class TestEligibilityResponse:
    def test_eligibility_response_creation(self):
        response = EligibilityResponse()
        assert response.status == "Active"
        assert response.created_at != ""
        
    def test_eligibility_response_with_data(self):
        response = EligibilityResponse(
            transaction_id="123456",
            subscriber_name="Doe, John",
            member_id="987654321"
        )
        assert response.transaction_id == "123456"
        assert response.subscriber_name == "Doe, John"
        assert response.member_id == "987654321"

class TestSimpleEDI271Parser:
    def test_parser_initialization_without_db(self):
        parser = SimpleEDI271Parser()
        assert parser.db_manager is None
        assert isinstance(parser.data, EligibilityResponse)
    
    def test_parser_initialization_with_mock_db(self):
        mock_db = Mock(spec=DatabaseManager)
        parser = SimpleEDI271Parser(mock_db)
        assert parser.db_manager is mock_db
    
    def test_parse_content(self):
        parser = SimpleEDI271Parser()
        result = parser.parse_content(SAMPLE_EDI_271)
        
        assert result.transaction_id == "1234567890"
        assert result.payer_name == "BLUE CROSS BLUE SHIELD"
        assert result.provider_name == "PROVIDER NAME"
        assert result.provider_npi == "1234567890"
        assert result.subscriber_name == "DOE, JOHN M"
        assert result.member_id == "123456789"
        assert result.group_number == "GRP12345"
        assert result.employer == "ACME CORP"
        assert result.address == "123 MAIN ST, ANYTOWN, CA 90210"
        assert result.date_of_birth == "01/01/1990"
        assert result.gender == "Male"
        assert result.individual_deductible == "$2,500.00"
        assert result.individual_deductible_met == "$500.00"
        assert result.preventative_care_copay == "$25.00"
    
    def test_parse_file(self):
        parser = SimpleEDI271Parser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.edi', delete=False) as f:
            f.write(SAMPLE_EDI_271)
            temp_file = f.name
        
        try:
            result = parser.parse_file(temp_file)
            assert result.transaction_id == "1234567890"
            assert result.subscriber_name == "DOE, JOHN M"
        finally:
            os.unlink(temp_file)
    
    def test_parse_file_with_db_save(self):
        mock_db = Mock(spec=DatabaseManager)
        mock_db.insert_eligibility_response.return_value = 123
        
        parser = SimpleEDI271Parser(mock_db)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.edi', delete=False) as f:
            f.write(SAMPLE_EDI_271)
            temp_file = f.name
        
        try:
            result = parser.parse_file(temp_file, save_to_db=True)
            assert result.transaction_id == "1234567890"
            mock_db.insert_eligibility_response.assert_called_once()
        finally:
            os.unlink(temp_file)
    
    def test_get_member_eligibility_without_db(self):
        parser = SimpleEDI271Parser()
        result = parser.get_member_eligibility("123456")
        assert result is None
    
    def test_get_member_eligibility_with_db(self):
        mock_db = Mock(spec=DatabaseManager)
        mock_data = {"member_id": "123456", "subscriber_name": "Doe, John"}
        mock_db.get_eligibility_by_member_id.return_value = mock_data
        
        parser = SimpleEDI271Parser(mock_db)
        result = parser.get_member_eligibility("123456")
        
        assert result == mock_data
        mock_db.get_eligibility_by_member_id.assert_called_once_with("123456")
    
    def test_update_member_status_without_db(self):
        parser = SimpleEDI271Parser()
        result = parser.update_member_status(1, "Inactive")
        assert result is False
    
    def test_update_member_status_with_db(self):
        mock_db = Mock(spec=DatabaseManager)
        mock_db.update_eligibility_status.return_value = True
        
        parser = SimpleEDI271Parser(mock_db)
        result = parser.update_member_status(1, "Inactive")
        
        assert result is True
        mock_db.update_eligibility_status.assert_called_once_with(1, "Inactive")
    
    def test_preventative_care_copay_parsing(self):
        """Test specific parsing of preventative care co-pay"""
        parser = SimpleEDI271Parser()
        
        # Test with A3 benefit code
        test_edi_a3 = """ST*271*TEST123~
EB*B*IND**A3*PREVENTIVE CARE*23*20.00~
SE*3*TEST123~"""
        
        result = parser.parse_content(test_edi_a3)
        assert result.preventative_care_copay == "$20.00"
        
        # Test with 98 benefit code
        parser2 = SimpleEDI271Parser()
        test_edi_98 = """ST*271*TEST456~
EB*C*IND**98*WELLNESS VISIT*23*15.00~
SE*3*TEST456~"""
        
        result2 = parser2.parse_content(test_edi_98)
        assert result2.preventative_care_copay == "$15.00"

@pytest.mark.skipif(not PSYCOPG2_AVAILABLE, reason="psycopg2 not available")
class TestDatabaseManager:
    def test_database_manager_initialization_without_psycopg2(self):
        with patch('edi271_parser.PSYCOPG2_AVAILABLE', False):
            with pytest.raises(ImportError):
                DatabaseManager({"host": "localhost"})
    
    @patch('edi271_parser.psycopg2.pool.ThreadedConnectionPool')
    def test_initialize_pool(self, mock_pool):
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        
        db_manager = DatabaseManager(config)
        db_manager.initialize_pool()
        
        mock_pool.assert_called_once_with(1, 10, **config)
    
    @patch('edi271_parser.psycopg2.pool.ThreadedConnectionPool')
    def test_get_connection_context_manager(self, mock_pool):
        mock_conn = Mock()
        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance
        
        config = {"host": "localhost", "database": "test_db"}
        db_manager = DatabaseManager(config)
        
        with db_manager.get_connection() as conn:
            assert conn is mock_conn
        
        mock_pool_instance.putconn.assert_called_once_with(mock_conn)
    
    def test_parse_date_valid_formats(self):
        config = {"host": "localhost"}
        
        with patch('edi271_parser.PSYCOPG2_AVAILABLE', True):
            db_manager = DatabaseManager(config)
            
            # Test MM/DD/YYYY format
            assert db_manager._parse_date("01/15/2024") == "2024-01-15"
            assert db_manager._parse_date("12/31/2023") == "2023-12-31"
            
            # Test empty string
            assert db_manager._parse_date("") is None
            assert db_manager._parse_date(None) is None
            
            # Test invalid format
            assert db_manager._parse_date("invalid-date") == "invalid-date"

class TestIntegration:
    def test_parser_without_database_integration(self):
        """Test that parser works without any database configuration"""
        parser = SimpleEDI271Parser()
        result = parser.parse_content(SAMPLE_EDI_271)
        
        assert result.transaction_id == "1234567890"
        assert result.subscriber_name == "DOE, JOHN M"
        
        # Test database operations return appropriate defaults
        assert parser.get_member_eligibility("123456") is None
        assert parser.update_member_status(1, "Inactive") is False
    
    @patch('edi271_parser.PSYCOPG2_AVAILABLE', True)
    def test_database_operations_error_handling(self):
        """Test error handling in database operations"""
        mock_db = Mock(spec=DatabaseManager)
        mock_db.get_eligibility_by_member_id.side_effect = Exception("Database error")
        mock_db.update_eligibility_status.side_effect = Exception("Database error")
        
        parser = SimpleEDI271Parser(mock_db)
        
        # Should handle exceptions gracefully
        result = parser.get_member_eligibility("123456")
        assert result is None
        
        success = parser.update_member_status(1, "Inactive")
        assert success is False

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
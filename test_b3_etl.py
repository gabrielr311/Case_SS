import pytest
import os
import json
import pandas as pd
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime
from dotenv import load_dotenv

from etls.b3_etl import B3ETL
import formats as fmts
import network


@pytest.fixture(scope="session", autouse=True)
def setup_env():
    """Setup test environment variables."""
    load_dotenv(".env.test", override=True)
    os.environ["B3_MACRO_DATA_URL"] = "https://www.b3.com.br/api/mock/macro-data"
    os.environ["ENV"] = "test"


@pytest.fixture
def mock_config():
    """Create a mock IngestionOrchestratorConfig."""
    config = Mock(spec=fmts.IngestionOrchestratorConfig)
    config.logger = Mock()
    config.redis_handler = Mock()
    config.trace_id = "test-trace-123"
    
    # Mock the save method
    config.save_df_to_gold_export_and_serving = Mock(return_value={
        fmts.MedallionLayer.GOLD_SERVING: "s3://bucket/gold/file.parquet",
        fmts.MedallionLayer.GOLD_EXPORT: "s3://bucket/export/file.csv"
    })
    
    return config


@pytest.fixture
def b3_etl(mock_config):
    """Create B3ETL instance with mocked dependencies."""
    with patch('network.create_http_session') as mock_session:
        # Create mock sessions
        mock_session.return_value = Mock()
        etl = B3ETL(mock_config)
        return etl


@pytest.fixture
def sample_macro_data():
    """Sample macro data response from B3 API."""
    return [
        {
            "securityIdentificationCode": "USD",
            "description": "Dólar Americano",
            "groupDescription": "TAXAS DE CÂMBIO",
            "value": 5.25,
            "rate": 0.0,
            "lastUpdate": "2025-10-19"
        },
        {
            "securityIdentificationCode": "EUR",
            "description": "Euro",
            "groupDescription": "TAXAS DE CÂMBIO",
            "value": 5.75,
            "rate": 0.0,
            "lastUpdate": "2025-10-19"
        },
        {
            "securityIdentificationCode": "SELIC",
            "description": "Taxa Selic",
            "groupDescription": "TAXAS DE JUROS NACIONAL",
            "value": 0.0,
            "rate": 11.25,
            "lastUpdate": "2025-10-19"
        },
        {
            "securityIdentificationCode": "FED",
            "description": "Federal Funds Rate",
            "groupDescription": "TAXAS DE JUROS INTERNACIONAL",
            "value": 0.0,
            "rate": 5.50,
            "lastUpdate": "2025-10-19"
        }
    ]


class TestB3ETLInit:
    """Tests for B3ETL initialization."""
    
    def test_init_creates_correct_attributes(self, mock_config):
        """Test that B3ETL initializes with correct attributes."""
        with patch('network.create_http_session') as mock_session:
            mock_session.return_value = Mock()
            
            etl = B3ETL(mock_config)
            
            assert etl.config == mock_config
            assert etl.data_source == fmts.DataSources.B3
            assert etl.http_fixed_time_session is not None
            assert etl.http_exp_backoff_session is not None
    
    def test_init_creates_http_sessions(self, mock_config):
        """Test that HTTP sessions are created with correct parameters."""
        with patch('network.create_http_session') as mock_session:
            mock_session.return_value = Mock()
            
            etl = B3ETL(mock_config)
            
            # Verify sessions were created with correct parameters
            calls = mock_session.call_args_list
            assert len(calls) == 2
            assert calls[0] == call(fixed_delay_retry=10, backoff_factor=0)
            assert calls[1] == call(backoff_factor=1)


class TestB3ETLMacroDataTypeMap:
    """Tests for macro data type mapping."""
    
    def test_macro_data_type_map_contains_all_types(self):
        """Test that MACRO_DATA_TYPE_MAP contains expected mappings."""
        assert B3ETL.MACRO_DATA_TYPE_MAP["TAXAS DE CÂMBIO"] == "FX"
        assert B3ETL.MACRO_DATA_TYPE_MAP["TAXAS DE JUROS INTERNACIONAL"] == "INTERNATIONAL_RATES"
        assert B3ETL.MACRO_DATA_TYPE_MAP["TAXAS DE JUROS NACIONAL"] == "DOMESTIC_RATES"


class TestMacroDataFullETL:
    """Tests for macro_data_full_etl method."""
    
    def test_macro_data_full_etl_success(self, b3_etl, sample_macro_data):
        """Test successful ETL execution with valid data."""
        # Mock the HTTP response
        mock_response = Mock()
        mock_response.content = json.dumps(sample_macro_data).encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        b3_etl.http_exp_backoff_session.request = Mock(return_value=mock_response)
        
        # Mock the schema and conversion functions
        with patch('formats.DocumentSchemas') as mock_schemas, \
             patch('formats.convert_brazilian_numbers_to_float') as mock_convert, \
             patch('formats.create_ref_date') as mock_ref_date:
            
            mock_schemas.B3_DADOS_MACRO.get_column_names = [
                "security_id", "description", "data_type", "value", "ref_date"
            ]
            mock_schemas.B3_DADOS_MACRO.value = {
                "security_id": str,
                "description": str,
                "data_type": str,
                "value": float,
                "ref_date": 'datetime64[ns]'
            }
            
            # Mock convert to return same dataframe
            mock_convert.side_effect = lambda df, cols: df
            mock_ref_date.return_value = "2025-10-19"
            
            # Execute
            b3_etl.macro_data_full_etl()
            
            # Verify logger calls
            assert b3_etl.config.logger.info.call_count >= 2
            
            # Verify HTTP request was made
            b3_etl.http_exp_backoff_session.request.assert_called_once()
            
            # Verify save was called
            b3_etl.config.save_df_to_gold_export_and_serving.assert_called_once()
            
            # Verify redis cache was called
            b3_etl.config.redis_handler.save_to_cache.assert_called_once()
    
    def test_macro_data_full_etl_http_request_details(self, b3_etl, sample_macro_data):
        """Test that HTTP request is made with correct parameters."""
        mock_response = Mock()
        mock_response.content = json.dumps(sample_macro_data).encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        b3_etl.http_exp_backoff_session.request = Mock(return_value=mock_response)
        
        with patch('formats.DocumentSchemas') as mock_schemas, \
             patch('formats.convert_brazilian_numbers_to_float') as mock_convert, \
             patch('formats.create_ref_date'):
            
            mock_schemas.B3_DADOS_MACRO.get_column_names = [
                "security_id", "description", "data_type", "value", "ref_date"
            ]
            mock_schemas.B3_DADOS_MACRO.value = {}
            mock_convert.side_effect = lambda df, cols: df
            
            b3_etl.macro_data_full_etl()
            
            # Check request parameters
            call_args = b3_etl.http_exp_backoff_session.request.call_args
            assert call_args[0][0] == "GET"
            assert call_args[1]['headers']['accept'] == 'application/json, text/plain, */*'
    
    def test_macro_data_full_etl_handles_http_error(self, b3_etl):
        """Test that HTTP errors are properly raised."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock(side_effect=Exception("HTTP 500 Error"))
        
        b3_etl.http_exp_backoff_session.request = Mock(return_value=mock_response)
        
        with pytest.raises(Exception, match="HTTP 500 Error"):
            b3_etl.macro_data_full_etl()
    
    def test_macro_data_full_etl_data_transformation(self, b3_etl, sample_macro_data):
        """Test that data is correctly transformed into DataFrame."""
        mock_response = Mock()
        mock_response.content = json.dumps(sample_macro_data).encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        b3_etl.http_exp_backoff_session.request = Mock(return_value=mock_response)
        
        captured_df = None
        
        def capture_df(*args, **kwargs):
            nonlocal captured_df
            captured_df = args[0]  # First argument is the dataframe
            return {"gold": "s3://test"}
        
        b3_etl.config.save_df_to_gold_export_and_serving = Mock(side_effect=capture_df)
        
        with patch('formats.DocumentSchemas') as mock_schemas, \
             patch('formats.convert_brazilian_numbers_to_float') as mock_convert, \
             patch('formats.create_ref_date'):
            
            mock_schemas.B3_DADOS_MACRO.get_column_names = [
                "security_id", "description", "data_type", "value", "ref_date"
            ]
            mock_schemas.B3_DADOS_MACRO.value = {}
            mock_convert.side_effect = lambda df, cols: df
            
            b3_etl.macro_data_full_etl()
            
            # Verify DataFrame was created with correct number of rows
            assert captured_df is not None
            assert len(captured_df) == 4
            
            # Verify data types were mapped correctly
            assert "FX" in captured_df["data_type"].values
            assert "DOMESTIC_RATES" in captured_df["data_type"].values
            assert "INTERNATIONAL_RATES" in captured_df["data_type"].values
    
    def test_macro_data_full_etl_value_selection(self, b3_etl):
        """Test that max(value, rate) correctly selects the non-zero value."""
        # Test data where sometimes value is filled, sometimes rate
        test_data = [
            {
                "securityIdentificationCode": "USD",
                "description": "Dólar",
                "groupDescription": "TAXAS DE CÂMBIO",
                "value": 5.25,
                "rate": 0.0,
                "lastUpdate": "2025-10-19"
            },
            {
                "securityIdentificationCode": "SELIC",
                "description": "Taxa Selic",
                "groupDescription": "TAXAS DE JUROS NACIONAL",
                "value": 0.0,
                "rate": 11.25,
                "lastUpdate": "2025-10-19"
            }
        ]
        
        mock_response = Mock()
        mock_response.content = json.dumps(test_data).encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        b3_etl.http_exp_backoff_session.request = Mock(return_value=mock_response)
        
        captured_df = None
        
        def capture_df(*args, **kwargs):
            nonlocal captured_df
            captured_df = args[0]
            return {"gold": "s3://test"}
        
        b3_etl.config.save_df_to_gold_export_and_serving = Mock(side_effect=capture_df)
        
        with patch('formats.DocumentSchemas') as mock_schemas, \
             patch('formats.convert_brazilian_numbers_to_float') as mock_convert, \
             patch('formats.create_ref_date'), \
             patch('formats.GoldServingTableNames'):
            
            mock_schemas.B3_DADOS_MACRO.get_column_names = [
                "security_id", "description", "data_type", "value", "ref_date"
            ]
            # FIX: Add proper datetime type
            mock_schemas.B3_DADOS_MACRO.value = {
                "ref_date": 'datetime64[ns]'
            }
            
            # FIX: Make convert return a DataFrame with datetime column
            def convert_with_datetime(df, cols):
                df['ref_date'] = pd.to_datetime(df['ref_date'])
                return df
            
            mock_convert.side_effect = convert_with_datetime
            
            b3_etl.macro_data_full_etl()
            
            # Verify the correct values were selected
            assert captured_df is not None
            usd_row = captured_df[captured_df["security_id"] == "USD"]
            selic_row = captured_df[captured_df["security_id"] == "SELIC"]
            
            assert usd_row["value"].values[0] == 5.25
            assert selic_row["value"].values[0] == 11.25
    
    def test_macro_data_full_etl_redis_cache(self, b3_etl, sample_macro_data):
        """Test that data is saved to Redis cache."""
        mock_response = Mock()
        mock_response.content = json.dumps(sample_macro_data).encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        b3_etl.http_exp_backoff_session.request = Mock(return_value=mock_response)
        
        with patch('formats.DocumentSchemas') as mock_schemas, \
             patch('formats.convert_brazilian_numbers_to_float') as mock_convert, \
             patch('formats.create_ref_date'), \
             patch('formats.GoldServingTableNames'):
            
            mock_schemas.B3_DADOS_MACRO.get_column_names = [
                "security_id", "description", "data_type", "value", "ref_date"
            ]
            # FIX: Add proper datetime type
            mock_schemas.B3_DADOS_MACRO.value = {
                "ref_date": 'datetime64[ns]'
            }
            
            # FIX: Make convert return a DataFrame with datetime column
            def convert_with_datetime(df, cols):
                df['ref_date'] = pd.to_datetime(df['ref_date'])
                return df
            
            mock_convert.side_effect = convert_with_datetime
            
            b3_etl.macro_data_full_etl()
            
            # Verify redis save_to_cache was called
            b3_etl.config.redis_handler.save_to_cache.assert_called_once()
            
            # Verify it was called with correct trace_id
            call_args = b3_etl.config.redis_handler.save_to_cache.call_args
            assert call_args[0][2] is not None  # datetime
            assert call_args[0][3] == "test-trace-123"  # trace_id
    
    def test_macro_data_full_etl_empty_response(self, b3_etl):
        """Test handling of empty API response."""
        mock_response = Mock()
        mock_response.content = json.dumps([]).encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        b3_etl.http_exp_backoff_session.request = Mock(return_value=mock_response)
        
        captured_df = None
        
        def capture_df(*args, **kwargs):
            nonlocal captured_df
            captured_df = args[0]
            return {"gold": "s3://test"}
        
        b3_etl.config.save_df_to_gold_export_and_serving = Mock(side_effect=capture_df)
        
        with patch('formats.DocumentSchemas') as mock_schemas, \
             patch('formats.convert_brazilian_numbers_to_float') as mock_convert, \
             patch('formats.create_ref_date'):
            
            mock_schemas.B3_DADOS_MACRO.get_column_names = [
                "security_id", "description", "data_type", "value", "ref_date"
            ]
            mock_schemas.B3_DADOS_MACRO.value = {}
            mock_convert.side_effect = lambda df, cols: df
            
            b3_etl.macro_data_full_etl()
            
            # Verify empty DataFrame was created
            assert captured_df is not None
            assert len(captured_df) == 0


class TestIntegrationScenarios:
    """Integration-style tests for common scenarios."""
    
    def test_full_pipeline_with_all_data_types(self, b3_etl):
        """Test full pipeline processes all data types correctly."""
        test_data = [
            {
                "securityIdentificationCode": "USD",
                "description": "Dólar",
                "groupDescription": "TAXAS DE CÂMBIO",
                "value": 5.25,
                "rate": 0.0,
                "lastUpdate": "2025-10-19"
            },
            {
                "securityIdentificationCode": "SELIC",
                "description": "Selic",
                "groupDescription": "TAXAS DE JUROS NACIONAL",
                "value": 0.0,
                "rate": 11.25,
                "lastUpdate": "2025-10-19"
            },
            {
                "securityIdentificationCode": "FED",
                "description": "Fed Funds",
                "groupDescription": "TAXAS DE JUROS INTERNACIONAL",
                "value": 0.0,
                "rate": 5.50,
                "lastUpdate": "2025-10-19"
            }
        ]
        
        mock_response = Mock()
        mock_response.content = json.dumps(test_data).encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        b3_etl.http_exp_backoff_session.request = Mock(return_value=mock_response)
        
        with patch('formats.DocumentSchemas') as mock_schemas, \
             patch('formats.convert_brazilian_numbers_to_float') as mock_convert, \
             patch('formats.create_ref_date'):
            
            mock_schemas.B3_DADOS_MACRO.get_column_names = [
                "security_id", "description", "data_type", "value", "ref_date"
            ]
            mock_schemas.B3_DADOS_MACRO.value = {}
            mock_convert.side_effect = lambda df, cols: df
            
            # Should not raise any exceptions
            b3_etl.macro_data_full_etl()
            
            # Verify all major steps completed
            assert b3_etl.config.logger.info.call_count >= 2
            assert b3_etl.config.save_df_to_gold_export_and_serving.called
            assert b3_etl.config.redis_handler.save_to_cache.called


# Run with: pytest test_b3_etl.py -v
# Debug specific test: pytest test_b3_etl.py::TestMacroDataFullETL::test_macro_data_full_etl_success -v --pdb
# Run with coverage: pytest test_b3_etl.py --cov=etls.b3_etl --cov-report=html
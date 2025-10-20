import pytest
import os
from unittest.mock import Mock, MagicMock, patch, call
from io import BytesIO
from collections import defaultdict

from ragflow_handler import RagflowHandler
from ragflow_sdk.modules.dataset import DataSet
from ragflow_sdk.modules.document import Document
import formats as fmts


@pytest.fixture(scope="session", autouse=True)
def setup_env():
    """Setup test environment variables."""
    os.environ["COMPANY_NAME"] = "Test Company Inc"
    os.environ["COMPANY_FORMATTED_CNPJ"] = "12.345.678/0001-90"


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    return Mock()


@pytest.fixture
def mock_ragflow_client():
    """Create a mock RAGFlow client."""
    return Mock()


@pytest.fixture
def ragflow_handler(mock_logger, mock_ragflow_client):
    """Create RagflowHandler instance with mocked dependencies."""
    with patch('ragflow_handler.RAGFlow') as mock_ragflow_class, \
         patch('ragflow_handler.PDFParser') as mock_pdf_parser:
        
        mock_ragflow_class.return_value = mock_ragflow_client
        mock_pdf_parser.return_value = Mock()
        
        handler = RagflowHandler(
            logger=mock_logger,
            base_url="http://localhost:9380",
            api_key="test-api-key-123",
            embeddings_model_name="nomic-embed-text"
        )
        
        return handler


@pytest.fixture
def sample_file_metadata():
    """Sample file metadata dictionary."""
    return {
        fmts.BucketCustomMetadata.FILE_HASH.value: "abc123hash",
        fmts.BucketCustomMetadata.SOURCE.value: "CVM",
        fmts.BucketCustomMetadata.DOCUMENT_TYPE.value: "ITR",
        fmts.BucketCustomMetadata.AGGREGATION_TYPE.value: "Consolidado"
    }


@pytest.fixture
def sample_file_bytes():
    """Sample file bytes."""
    return BytesIO(b"PDF content here")


class TestRagflowHandlerInit:
    """Tests for RagflowHandler initialization."""
    
    def test_init_creates_correct_attributes(self, mock_logger):
        """Test that RagflowHandler initializes with correct attributes."""
        with patch('ragflow_handler.RAGFlow') as mock_ragflow_class, \
             patch('ragflow_handler.PDFParser') as mock_pdf_parser:
            
            mock_client = Mock()
            mock_ragflow_class.return_value = mock_client
            
            handler = RagflowHandler(
                logger=mock_logger,
                base_url="http://localhost:9380",
                api_key="test-key",
                embeddings_model_name="nomic-embed-text"
            )
            
            assert handler.logger == mock_logger
            assert handler.client == mock_client
            assert handler.embeddings_model_name == "nomic-embed-text"
            assert handler.pdf_parser is not None
    
    def test_init_creates_ragflow_client_with_correct_params(self, mock_logger):
        """Test that RAGFlow client is created with correct parameters."""
        with patch('ragflow_handler.RAGFlow') as mock_ragflow_class, \
             patch('ragflow_handler.PDFParser'):
            
            handler = RagflowHandler(
                logger=mock_logger,
                base_url="http://test.com:9380",
                api_key="my-secret-key",
                embeddings_model_name="test-model"
            )
            
            mock_ragflow_class.assert_called_once_with(
                base_url="http://test.com:9380",
                api_key="my-secret-key"
            )


class TestCreateDataset:
    """Tests for create_dataset method."""
    
    def test_create_dataset_success(self, ragflow_handler):
        """Test successful dataset creation."""
        dataset_name = "test_dataset"
        
        ragflow_handler.create_dataset(dataset_name)
        
        ragflow_handler.client.create_dataset.assert_called_once_with(
            name=dataset_name,
            embedding_model="nomic-embed-text@ollama",
            permission="team",
            chunk_method="naive"
        )
    
    def test_create_dataset_with_different_model(self, mock_logger, mock_ragflow_client):
        """Test dataset creation with different embedding model."""
        with patch('ragflow_handler.RAGFlow') as mock_ragflow_class, \
             patch('ragflow_handler.PDFParser'):
            
            mock_ragflow_class.return_value = mock_ragflow_client
            
            handler = RagflowHandler(
                logger=mock_logger,
                base_url="http://localhost:9380",
                api_key="test-key",
                embeddings_model_name="custom-model"
            )
            
            handler.create_dataset("my_dataset")
            
            call_args = mock_ragflow_client.create_dataset.call_args
            assert call_args[1]['embedding_model'] == "custom-model@ollama"
    
    def test_create_dataset_logs_on_error(self, ragflow_handler):
        """Test that errors during dataset creation are handled."""
        ragflow_handler.client.create_dataset.side_effect = Exception("API Error")
        
        with pytest.raises(Exception, match="API Error"):
            ragflow_handler.create_dataset("test_dataset")


class TestGetFilesInDataset:
    """Tests for get_files_in_dataset method."""
    
    def test_get_files_success(self, ragflow_handler):
        """Test successful retrieval of files from dataset."""
        mock_dataset = Mock(spec=DataSet)
        mock_doc1 = Mock(spec=Document)
        mock_doc1.name = "file1.pdf"
        mock_doc2 = Mock(spec=Document)
        mock_doc2.name = "file2.pdf"
        
        mock_dataset.list_documents.return_value = [mock_doc1, mock_doc2]
        ragflow_handler.client.list_datasets.return_value = [mock_dataset]
        
        dataset, documents = ragflow_handler.get_files_in_dataset(
            file_name="file1.pdf",
            dataset_name="test_dataset"
        )
        
        assert dataset == mock_dataset
        assert len(documents) == 2
        mock_dataset.list_documents.assert_called_once_with(keywords="file1.pdf")
    
    def test_get_files_dataset_not_found(self, ragflow_handler):
        """Test error when dataset doesn't exist."""
        ragflow_handler.client.list_datasets.return_value = []
        
        with pytest.raises(Exception, match="Dataset with name 'nonexistent' doesnt exists"):
            ragflow_handler.get_files_in_dataset(
                file_name="file.pdf",
                dataset_name="nonexistent"
            )
        
        ragflow_handler.logger.error.assert_called_once()
    
    def test_get_files_handles_api_exception(self, ragflow_handler):
        """Test handling of API exceptions."""
        ragflow_handler.client.list_datasets.side_effect = Exception("API Error")
        
        with pytest.raises(Exception):
            ragflow_handler.get_files_in_dataset(
                file_name="file.pdf",
                dataset_name="test_dataset"
            )
    
    def test_get_files_with_empty_results(self, ragflow_handler):
        """Test when no documents match the search."""
        mock_dataset = Mock(spec=DataSet)
        mock_dataset.list_documents.return_value = []
        ragflow_handler.client.list_datasets.return_value = [mock_dataset]
        
        dataset, documents = ragflow_handler.get_files_in_dataset(
            file_name="nonexistent.pdf",
            dataset_name="test_dataset"
        )
        
        assert documents == []


class TestGetAllFilesHashInDataset:
    """Tests for get_all_files_hash_in_dataset method."""
    
    def test_get_all_hashes_success(self, ragflow_handler):
        """Test successful retrieval of all file hashes."""
        mock_dataset = Mock(spec=DataSet)
        
        mock_doc1 = Mock(spec=Document)
        mock_doc1.name = "file1.pdf"
        mock_doc1.meta_fields = Mock()
        mock_doc1.meta_fields.file_hash = "hash123"
        
        mock_doc2 = Mock(spec=Document)
        mock_doc2.name = "file2.pdf"
        mock_doc2.meta_fields = Mock()
        mock_doc2.meta_fields.file_hash = "hash456"
        
        mock_dataset.list_documents.return_value = [mock_doc1, mock_doc2]
        
        result = ragflow_handler.get_all_files_hash_in_dataset(mock_dataset)
        
        assert len(result) == 2
        assert ["file1.pdf", "hash123"] in result
        assert ["file2.pdf", "hash456"] in result
    
    def test_get_all_hashes_missing_hash_attribute(self, ragflow_handler):
        """Test handling documents without file_hash attribute."""
        mock_dataset = Mock(spec=DataSet)
        
        mock_doc = Mock(spec=Document)
        mock_doc.name = "file.pdf"
        mock_doc.meta_fields = Mock(spec=[])  # No file_hash attribute
        
        mock_dataset.list_documents.return_value = [mock_doc]
        
        result = ragflow_handler.get_all_files_hash_in_dataset(mock_dataset)
        
        assert len(result) == 1
        assert result[0] == ["file.pdf", None]
    
    def test_get_all_hashes_empty_dataset(self, ragflow_handler):
        """Test with empty dataset."""
        mock_dataset = Mock(spec=DataSet)
        mock_dataset.list_documents.return_value = []
        
        result = ragflow_handler.get_all_files_hash_in_dataset(mock_dataset)
        
        assert result == []


class TestUploadDocumentAndStartParse:
    """Tests for upload_document_and_start_parse method."""
    
    def test_upload_skips_duplicate_file(self, ragflow_handler, sample_file_bytes, sample_file_metadata):
        """Test that duplicate files are not uploaded."""
        mock_dataset = Mock(spec=DataSet)
        ragflow_handler.client.list_datasets.return_value = [mock_dataset]
        
        # Mock existing file with same hash
        with patch.object(ragflow_handler, 'get_all_files_hash_in_dataset') as mock_get_hashes:
            mock_get_hashes.return_value = [
                ["existing_file.pdf", "abc123hash"]  # Same hash as sample_file_metadata
            ]
            
            ragflow_handler.upload_document_and_start_parse(
                dataset_name="test_dataset",
                file_name="test.pdf",
                file_bytes=sample_file_bytes,
                file_metadata=sample_file_metadata
            )
            
            # Verify upload was NOT called
            mock_dataset.upload_documents.assert_not_called()
            
            # Verify warning was logged
            ragflow_handler.logger.warning.assert_called_once()


class TestNaiveParserConfig:
    """Tests for NAIVE_PARSER_CONFIG constant."""
    
    def test_parser_config_structure(self, ragflow_handler):
        """Test that parser config has correct structure."""
        config = ragflow_handler.NAIVE_PARSER_CONFIG
        
        assert "chunk_count" in config
        assert config["chunk_count"] == 512
        assert "delimiter" in config
        assert config["delimiter"] == "\\n"
        assert "html4excel" in config
        assert config["html4excel"] is False
        assert "layout_recognize" in config
        assert config["layout_recognize"] is True
        assert "raptor" in config
        assert config["raptor"]["use_raptor"] is False


# Run with: pytest test_ragflow_handler.py -v
# Debug: pytest test_ragflow_handler.py::TestUploadDocumentAndStartParse::test_upload_new_document_success -v --pdb
# Coverage: pytest test_ragflow_handler.py --cov=ragflow_handler --cov-report=html
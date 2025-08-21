"""
Tests for eventstore_client context manager. (Generated using Claude.ai)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from main import eventstore_client


class TestEventStoreClient:
    """Test cases for eventstore_client context manager."""

    @patch('main.EventStoreDBClient')
    def test_successful_connection(self, mock_client_class):
        """Test successful EventStore connection."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock the read operation to simulate successful connection
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter([]))
        mock_client.get_stream.return_value = mock_read_response
        
        with eventstore_client("esdb://localhost:2113") as client:
            assert client == mock_client
            mock_client_class.assert_called_once_with(uri="esdb://localhost:2113")
        
        # Check that client was closed
        mock_client.close.assert_called_once()

    @patch('main.EventStoreDBClient')
    def test_connection_error_handling(self, mock_client_class):
        """Test handling of connection errors."""
        mock_client_class.side_effect = ConnectionError("Connection refused")
        
        with pytest.raises(ConnectionError, match="Failed to connect to EventStore"):
            with eventstore_client("esdb://localhost:2113"):
                pass

    @patch('main.EventStoreDBClient')
    def test_os_error_handling(self, mock_client_class):
        """Test handling of OS errors."""
        mock_client_class.side_effect = OSError("Network unreachable")
        
        with pytest.raises(ConnectionError, match="Failed to connect to EventStore"):
            with eventstore_client("esdb://localhost:2113"):
                pass

    @patch('main.EventStoreDBClient')
    def test_value_error_handling(self, mock_client_class):
        """Test handling of value errors."""
        mock_client_class.side_effect = ValueError("Invalid URI")
        
        with pytest.raises(ConnectionError, match="Failed to connect to EventStore"):
            with eventstore_client("invalid-uri"):
                pass

    @patch('main.EventStoreDBClient')
    def test_unexpected_error_handling(self, mock_client_class):
        """Test handling of unexpected errors."""
        mock_client_class.side_effect = RuntimeError("Unexpected error")
        
        with pytest.raises(RuntimeError, match="Unexpected error"):
            with eventstore_client("esdb://localhost:2113"):
                pass

    @patch('main.EventStoreDBClient')
    def test_client_close_on_exception(self, mock_client_class):
        """Test that client is closed even when an exception occurs."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock the read operation
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter([]))
        mock_client.get_stream.return_value = mock_read_response
        
        # Simulate an exception during context execution
        with pytest.raises(RuntimeError):
            with eventstore_client("esdb://localhost:2113") as client:
                raise RuntimeError("Test exception")
        
        # Check that client was still closed
        mock_client.close.assert_called_once()

    @patch('main.EventStoreDBClient')
    def test_client_close_error_handling(self, mock_client_class):
        """Test that client close errors are handled gracefully."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock the read operation
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter([]))
        mock_client.get_stream.return_value = mock_read_response
        
        # Simulate an error during client close
        mock_client.close.side_effect = Exception("Close error")
        
        # Should not raise an error, just log a warning
        with eventstore_client("esdb://localhost:2113") as client:
            pass
        
        # Check that close was attempted
        mock_client.close.assert_called_once()

    @patch('main.EventStoreDBClient')
    def test_connection_test_with_existing_stream(self, mock_client_class):
        """Test connection test with existing stream."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock successful stream read
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter([Mock()]))
        mock_client.get_stream.return_value = mock_read_response
        
        with eventstore_client("esdb://localhost:2113") as client:
            assert client == mock_client
        
        mock_client.close.assert_called_once()

    @patch('main.EventStoreDBClient')
    def test_connection_test_with_stream_error(self, mock_client_class):
        """Test connection test with stream error (should still work)."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock stream read error (this should not prevent connection)
        mock_client.get_stream.side_effect = Exception("Stream error")
        
        with eventstore_client("esdb://localhost:2113") as client:
            assert client == mock_client
        
        mock_client.close.assert_called_once()

    def test_invalid_uri_format(self):
        """Test that invalid URI formats are handled."""
        # This test would depend on how EventStoreDBClient handles invalid URIs
        # For now, we'll test with a clearly invalid URI
        with pytest.raises(Exception):  # Could be various types of errors
            with eventstore_client("not-a-valid-uri"):
                pass

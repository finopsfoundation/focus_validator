"""
Global pytest configuration and fixtures for the focus_validator test suite.
"""
import pytest
import gc
import sys


@pytest.fixture(autouse=True)
def cleanup_resources():
    """Automatically clean up resources after each test to prevent hanging."""
    yield  # Run the test
    
    # Force garbage collection to clean up any lingering resources
    gc.collect()
    
    # Additional cleanup for DuckDB or other resources if needed
    # This helps prevent hanging in CI environments
    

def pytest_sessionfinish(session, exitstatus):
    """Clean up resources at the end of the test session."""
    # Force garbage collection
    gc.collect()
    
    # Ensure any background threads are cleaned up
    import threading
    active_threads = threading.active_count()
    if active_threads > 1:
        # Main thread should be the only one left
        pass  # Could add more aggressive cleanup if needed
"""
Pytest configuration for services tests.

This file sets up environment variables and fixtures that are shared across all tests.
"""
import os

# Set required environment variables before any modules are imported
# This prevents errors during test collection when modules check for env vars at import time
os.environ.setdefault("TABLE_NAME", "test-table")

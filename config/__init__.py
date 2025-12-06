"""
Configuration Package
Application and database configuration settings

This package contains all configuration settings for the application.
Supports environment variables for different deployment environments.

Components:
- DatabaseConfig: MySQL database connection settings
- DevelopmentConfig: Development environment preset
- ProductionConfig: Production environment preset
- TestConfig: Testing environment preset

Usage:
    from config.database import DatabaseConfig

    # Get connection parameters
    params = DatabaseConfig.get_connection_params()

    # Validate configuration
    is_valid, message = DatabaseConfig.validate_config()

    # Print current config (for debugging)
    DatabaseConfig.print_config()
"""

from config.database import (
    DatabaseConfig,
    DevelopmentConfig,
    ProductionConfig,
    TestConfig
)

__all__ = [
    'DatabaseConfig',
    'DevelopmentConfig',
    'ProductionConfig',
    'TestConfig'
]
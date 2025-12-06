"""
Database Configuration Module
Centralized configuration management for database connections
Supports environment variables for production deployment
"""

import os
from typing import Dict

class DatabaseConfig:
    """
    Database configuration settings.
    Supports environment variables for flexible deployment.

    Configuration Priority:
    1. Environment variables (for production)
    2. Default values (for development)

    To use environment variables:
        export DB_HOST=localhost
        export DB_USER=root
        export DB_PASSWORD=your_password
        export DB_NAME=equity_research
    """

    # ========== MYSQL SERVER CONFIGURATION ==========
    # MODIFY THESE VALUES FOR YOUR LOCAL SETUP

    HOST = os.getenv('DB_HOST', 'localhost')
    PORT = int(os.getenv('DB_PORT', 3306))
    USER = os.getenv('DB_USER', 'root')              # ⬅️ CHANGE THIS
    PASSWORD = os.getenv('DB_PASSWORD', 'Prachit@27')          # ⬅️ CHANGE THIS
    DATABASE = os.getenv('DB_NAME', 'equity_research')

    # ========== CONNECTION SETTINGS ==========

    CHARSET = 'utf8mb4'
    AUTOCOMMIT = False  # Use explicit transactions

    # ========== CONNECTION POOL SETTINGS ==========

    POOL_SIZE = 5
    POOL_NAME = 'equity_research_pool'

    # ========== TIMEOUT SETTINGS ==========

    CONNECT_TIMEOUT = 10   # Seconds to wait for connection
    READ_TIMEOUT = 30      # Seconds to wait for read
    WRITE_TIMEOUT = 30     # Seconds to wait for write

    # ========== SSL/SECURITY (Optional) ==========

    USE_SSL = os.getenv('DB_USE_SSL', 'False').lower() == 'true'
    SSL_CA = os.getenv('DB_SSL_CA', None)
    SSL_CERT = os.getenv('DB_SSL_CERT', None)
    SSL_KEY = os.getenv('DB_SSL_KEY', None)

    @classmethod
    def get_connection_params(cls) -> Dict:
        """
        Get connection parameters as dictionary for pymysql.connect().

        Returns:
            dict: Database connection parameters

        Example:
            params = DatabaseConfig.get_connection_params()
            connection = pymysql.connect(**params)
        """
        params = {
            'host': cls.HOST,
            'port': cls.PORT,
            'user': cls.USER,
            'password': cls.PASSWORD,
            'database': cls.DATABASE,
            'charset': cls.CHARSET,
            'autocommit': cls.AUTOCOMMIT,
            'connect_timeout': cls.CONNECT_TIMEOUT,
            'read_timeout': cls.READ_TIMEOUT,
            'write_timeout': cls.WRITE_TIMEOUT
        }

        # Add SSL if configured
        if cls.USE_SSL and cls.SSL_CA:
            params['ssl'] = {
                'ca': cls.SSL_CA,
                'cert': cls.SSL_CERT,
                'key': cls.SSL_KEY
            }

        return params

    @classmethod
    def validate_config(cls) -> tuple:
        """
        Validate configuration parameters.

        Returns:
            tuple: (is_valid: bool, error_message: str)

        Example:
            is_valid, message = DatabaseConfig.validate_config()
            if not is_valid:
                print(f"Config error: {message}")
        """
        if not cls.HOST:
            return False, "Database host is not configured"

        if not cls.USER:
            return False, "Database user is not configured"

        if not cls.DATABASE:
            return False, "Database name is not configured"

        if cls.PORT < 1 or cls.PORT > 65535:
            return False, f"Invalid port number: {cls.PORT}"

        if not cls.PASSWORD:
            # Warning but not error - some setups don't need password
            print("⚠️  Warning: Database password is empty")

        return True, "Configuration is valid"

    @classmethod
    def get_connection_string(cls, hide_password: bool = True) -> str:
        """
        Get connection string for logging (optionally hides password).

        Args:
            hide_password: Whether to hide password in string

        Returns:
            str: Safe connection string

        Example:
            print(DatabaseConfig.get_connection_string())
            # Output: mysql://root@localhost:3306/equity_research
        """
        if hide_password or not cls.PASSWORD:
            return f"mysql://{cls.USER}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}"
        else:
            return f"mysql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}"

    @classmethod
    def print_config(cls):
        """
        Print current configuration (for debugging).
        Does NOT print password for security.
        """
        print("=" * 60)
        print("DATABASE CONFIGURATION")
        print("=" * 60)
        print(f"Host:      {cls.HOST}")
        print(f"Port:      {cls.PORT}")
        print(f"User:      {cls.USER}")
        print(f"Password:  {'*' * len(cls.PASSWORD) if cls.PASSWORD else '(empty)'}")
        print(f"Database:  {cls.DATABASE}")
        print(f"Charset:   {cls.CHARSET}")
        print(f"Timeout:   {cls.CONNECT_TIMEOUT}s")
        print(f"SSL:       {'Enabled' if cls.USE_SSL else 'Disabled'}")
        print("=" * 60)

    @classmethod
    def from_env_file(cls, env_file: str = '.env'):
        """
        Load configuration from .env file.

        Args:
            env_file: Path to .env file

        Example .env file:
            DB_HOST=localhost
            DB_PORT=3306
            DB_USER=root
            DB_PASSWORD=mypassword
            DB_NAME=equity_research
        """
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
            print(f"✅ Loaded configuration from {env_file}")
        except FileNotFoundError:
            print(f"⚠️  {env_file} not found, using default configuration")
        except Exception as e:
            print(f"❌ Error loading {env_file}: {e}")


# ========== CONFIGURATION PRESETS ==========

class DevelopmentConfig(DatabaseConfig):
    """Development environment configuration"""
    HOST = 'localhost'
    USER = 'root'
    PASSWORD = ''
    DATABASE = 'equity_research'
    AUTOCOMMIT = False


class ProductionConfig(DatabaseConfig):
    """Production environment configuration"""
    HOST = os.getenv('DB_HOST', 'production-server.example.com')
    USER = os.getenv('DB_USER', 'prod_user')
    PASSWORD = os.getenv('DB_PASSWORD', '')  # Must be set in environment!
    DATABASE = os.getenv('DB_NAME', 'equity_research_prod')
    AUTOCOMMIT = False
    USE_SSL = True
    CONNECT_TIMEOUT = 5
    READ_TIMEOUT = 10
    WRITE_TIMEOUT = 10


class TestConfig(DatabaseConfig):
    """Test environment configuration"""
    HOST = 'localhost'
    USER = 'test_user'
    PASSWORD = 'test_password'
    DATABASE = 'equity_research_test'
    AUTOCOMMIT = True  # Auto-commit for tests


# ========== USAGE EXAMPLES ==========

"""
Example 1: Basic Usage (Development)
-------------------------------------
from config.database import DatabaseConfig

# Print current config
DatabaseConfig.print_config()

# Validate
is_valid, message = DatabaseConfig.validate_config()
if is_valid:
    print("✅ Configuration is valid")


Example 2: Using Environment Variables
---------------------------------------
# Set environment variables first:
export DB_HOST=localhost
export DB_USER=myuser
export DB_PASSWORD=mypassword

# Then in Python:
from config.database import DatabaseConfig

params = DatabaseConfig.get_connection_params()
# Uses environment variables automatically!


Example 3: Using .env File
---------------------------
# Create .env file:
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=secret123
DB_NAME=equity_research

# Load in Python:
from config.database import DatabaseConfig

DatabaseConfig.from_env_file('.env')
params = DatabaseConfig.get_connection_params()


Example 4: Different Environments
----------------------------------
from config.database import DevelopmentConfig, ProductionConfig

# For development
dev_params = DevelopmentConfig.get_connection_params()

# For production
prod_params = ProductionConfig.get_connection_params()


Example 5: Connection String
-----------------------------
from config.database import DatabaseConfig

# For logging (hides password)
print(DatabaseConfig.get_connection_string())
# Output: mysql://root@localhost:3306/equity_research
"""

# ========== QUICK SETUP GUIDE ==========

"""
🔧 SETUP INSTRUCTIONS:

For Local Development:
1. Open config/database.py
2. Change these lines:
   USER = 'root'              # Your MySQL username
   PASSWORD = 'your_password' # Your MySQL password
3. Save and run!

For Production Deployment:
1. Set environment variables:
   export DB_HOST=your-server.com
   export DB_USER=prod_user
   export DB_PASSWORD=secure_password
   export DB_NAME=equity_research
2. Application will use environment variables automatically!

For Team Development:
1. Copy config/database.py to config/database.local.py
2. Add to .gitignore: config/database.local.py
3. Each team member uses their own database.local.py
4. Never commit passwords to git!
"""
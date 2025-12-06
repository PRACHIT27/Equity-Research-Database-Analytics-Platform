"""
Database Connection Manager - FIXED VERSION
Handles database connections using pymysql driver (NO ORM)
Implements Singleton pattern and provides transaction management
"""

import pymysql
from pymysql import Error
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Tuple

class DatabaseConnection:
    """
    Database connection manager using pymysql driver.
    Implements Singleton pattern for connection management.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Import config here to avoid circular imports
        from config.database import DatabaseConfig
        self.config = DatabaseConfig()
        self.connection = None
        self._initialized = True

    # ========== CONNECTION MANAGEMENT ==========

    def get_connection(self) -> pymysql.Connection:
        """
        Establish and return database connection.

        Returns:
            pymysql.Connection: Active database connection
        """
        try:
            if self.connection is None or not self.connection.open:
                params = self.config.get_connection_params()
                self.connection = pymysql.connect(
                    **params,
                    cursorclass=pymysql.cursors.DictCursor
                )
            return self.connection
        except Error as e:
            raise Exception(f"Database connection error: {e}")

    def close_connection(self):
        """Close database connection if open"""
        if self.connection and self.connection.open:
            self.connection.close()
            self.connection = None

    def reconnect(self):
        """Force reconnection to database"""
        self.close_connection()
        return self.get_connection()

    # ========== CONTEXT MANAGER FOR CURSOR ==========

    @contextmanager
    def get_cursor(self, dictionary: bool = True):
        """
        Context manager for database cursor.

        Args:
            dictionary (bool): Return results as dictionaries

        Yields:
            pymysql.cursors.Cursor: Database cursor
        """
        connection = self.get_connection()
        cursor = None
        try:
            if dictionary:
                cursor = connection.cursor(pymysql.cursors.DictCursor)
            else:
                cursor = connection.cursor()

            yield cursor
            connection.commit()

        except Error as e:
            connection.rollback()
            raise Exception(f"Database operation error: {e}")
        finally:
            if cursor:
                cursor.close()

    # ========== QUERY EXECUTION ==========

    def execute_query(self, query: str, params: Optional[Tuple] = None,
                      fetch: bool = True) -> Optional[List[Dict]]:
        """Execute a SELECT query and return results"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params or ())
                if fetch:
                    return cursor.fetchall()
                return None
        except Exception as e:
            raise Exception(f"Query execution error: {e}")

    def execute_update(self, query: str, params: Optional[Tuple] = None) -> int:
        """Execute INSERT, UPDATE, or DELETE query"""
        try:
            with self.get_cursor() as cursor:
                affected_rows = cursor.execute(query, params or ())
                return affected_rows
        except Exception as e:
            raise Exception(f"Update execution error: {e}")

    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """Execute query with multiple parameter sets"""
        try:
            with self.get_cursor() as cursor:
                affected_rows = cursor.executemany(query, params_list)
                return affected_rows
        except Exception as e:
            raise Exception(f"Batch execution error: {e}")

    # ========== STORED PROCEDURES ==========

    def call_procedure(self, proc_name: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        try:
            with self.get_cursor() as cursor:
                cursor.callproc(proc_name, params or ())

                results = []

            # Start loop to process the first result and all subsequent results/messages
                while True:
                    if cursor.description is not None:
                        results.extend(cursor.fetchall())

                # Try to advance to the next result/status message.
                # If nextset() returns None or False, we're done.
                    if not cursor.nextset():
                        break

                return results
        except Exception as e:
            raise Exception(f"Stored procedure call error ({proc_name}): {e}")

    # ========== USER-DEFINED FUNCTIONS ==========

    def call_function(self, func_name: str, params: Optional[Tuple] = None) -> Any:
        """Call user-defined function"""
        try:
            if params:
                param_placeholders = ', '.join(['%s'] * len(params))
                query = f"SELECT {func_name}({param_placeholders}) as result"
            else:
                query = f"SELECT {func_name}() as result"

            result = self.execute_query(query, params)
            return result[0]['result'] if result else None

        except Exception as e:
            raise Exception(f"Function call error ({func_name}): {e}")

    # ========== TRANSACTION MANAGEMENT ==========

    def begin_transaction(self):
        """Begin explicit transaction"""
        conn = self.get_connection()
        conn.begin()

    def commit(self):
        """Commit current transaction"""
        if self.connection and self.connection.open:
            self.connection.commit()

    def rollback(self):
        """Rollback current transaction"""
        if self.connection and self.connection.open:
            self.connection.rollback()

    # ========== CONNECTION TESTING ==========

    def test_connection(self) -> Dict[str, Any]:
        """Test database connection and return server info"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get MySQL version
                cursor.execute("SELECT VERSION() as version")
                version = cursor.fetchone()

                # Get current database - FIXED: Use backticks
                cursor.execute("SELECT DATABASE() as `database`")
                database = cursor.fetchone()

                # Get table count - FIXED: Properly escape reserved words
                cursor.execute("""
                               SELECT COUNT(*) as table_count
                               FROM information_schema.tables
                               WHERE table_schema = DATABASE()
                               """)
                table_count = cursor.fetchone()

                # Get connection info
                cursor.execute("SELECT CONNECTION_ID() as conn_id")
                conn_id = cursor.fetchone()

                return {
                    'status': 'connected',
                    'version': version['version'] if version else 'Unknown',
                    'database': database['database'] if database else 'Unknown',
                    'table_count': table_count['table_count'] if table_count else 0,
                    'connection_id': conn_id['conn_id'] if conn_id else None,
                    'connection_string': self.config.get_connection_string()
                }
        except Exception as e:
            return {
                'status': 'failed',
                'error': str(e)
            }

    def ping(self) -> bool:
        """Ping database to check if connection is alive"""
        try:
            conn = self.get_connection()
            conn.ping(reconnect=True)
            return True
        except:
            return False

    # ========== UTILITY METHODS ==========

    def get_last_insert_id(self) -> int:
        """Get the last inserted auto-increment ID"""
        try:
            conn = self.get_connection()
            return conn.insert_id()
        except:
            return 0

    def execute_script(self, script: str) -> bool:
        """Execute multi-statement SQL script"""
        try:
            with self.get_cursor() as cursor:
                for statement in script.split(';'):
                    statement = statement.strip()
                    if statement:
                        cursor.execute(statement)
            return True
        except Exception as e:
            print(f"Script execution error: {e}")
            return False

    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a table"""
        query = """
                SELECT
                    COLUMN_NAME as column_name,
                    DATA_TYPE as data_type,
                    IS_NULLABLE as is_nullable,
                    COLUMN_KEY as column_key,
                    COLUMN_DEFAULT as default_value,
                    EXTRA as extra
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION \
                """
        return self.execute_query(query, (table_name,))

    def __del__(self):
        """Destructor - close connection when object is destroyed"""
        self.close_connection()

    def __repr__(self):
        """String representation of connection"""
        status = "connected" if (self.connection and self.connection.open) else "disconnected"
        return f"<DatabaseConnection: {self.config.get_connection_string()} ({status})>"


# ========== GLOBAL SINGLETON INSTANCE ==========

_db_instance = None

def get_db_connection() -> DatabaseConnection:
    """
    Get singleton database connection instance.

    Returns:
        DatabaseConnection: Database connection manager
    """
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseConnection()
    return _db_instance
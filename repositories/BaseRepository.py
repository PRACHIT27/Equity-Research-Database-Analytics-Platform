"""
Base Repository Pattern Implementation
Abstract base class for all repository classes
Provides common database operations using pymysql (NO ORM)
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Tuple
from core.DatabaseConnection import DatabaseConnection

class BaseRepository(ABC):
    """
    Abstract base repository implementing Repository Pattern.
    Provides common CRUD operations and database access methods.
    All specific repositories should inherit from this class.
    """

    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize repository with database connection.

        Args:
            db_connection: DatabaseConnection instance
        """
        self.db = db_connection

    @abstractmethod
    def get_table_name(self) -> str:
        """
        Get the table name for this repository.
        Must be implemented by child classes.

        Returns:
            str: Table name
        """
        pass

    @abstractmethod
    def find_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        """
        Finds a single record by its primary key ID value.
        Implementation must be custom-written in each derived repository.
        """

        pass


    # ========== BASIC CRUD OPERATIONS ==========

    def find_all(self) -> List[Dict[str, Any]]:
        """
        Find all records in the table.

        Returns:
            list: All records as dictionaries
        """
        query = f"SELECT * FROM {self.get_table_name()}"
        return self.db.execute_query(query)

    def insert(self, data: Dict[str, Any]) -> int:
        """
        Insert a new record.

        Args:
            data (dict): Column-value pairs to insert

        Returns:
            int: Number of affected rows
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {self.get_table_name()} ({columns}) VALUES ({placeholders})"

        return self.db.execute_update(query, tuple(data.values()))

    def update(self, id_column: str, id_value: Any, data: Dict[str, Any]) -> int:
        """
        Update an existing record.

        Args:
            id_column (str): Name of ID column
            id_value: Value of ID
            data (dict): Column-value pairs to update

        Returns:
            int: Number of affected rows
        """
        if not data:
            return 0

        set_clause = ', '.join([f"{col} = %s" for col in data.keys()])
        query = f"UPDATE {self.get_table_name()} SET {set_clause} WHERE {id_column} = %s"

        params = tuple(data.values()) + (id_value,)
        return self.db.execute_update(query, params)

    def delete(self, id_column: str, id_value: Any) -> int:
        """
        Delete a record by ID.

        Args:
            id_column (str): Name of ID column
            id_value: Value of ID

        Returns:
            int: Number of affected rows
        """
        query = f"DELETE FROM {self.get_table_name()} WHERE {id_column} = %s"
        return self.db.execute_update(query, (id_value,))

    # ========== CUSTOM QUERY EXECUTION ==========

    def execute_custom_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a custom SELECT query.

        Args:
            query (str): SQL query
            params (tuple): Query parameters

        Returns:
            list: Query results
        """
        return self.db.execute_query(query, params)

    def execute_custom_update(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute a custom INSERT/UPDATE/DELETE query.

        Args:
            query (str): SQL query
            params (tuple): Query parameters

        Returns:
            int: Number of affected rows
        """
        return self.db.execute_update(query, params)

    # ========== BATCH OPERATIONS ==========

    def bulk_insert(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert multiple records in a single query.

        Args:
            records (list): List of dictionaries containing column-value pairs

        Returns:
            int: Number of affected rows
        """
        if not records:
            return 0

        # All records must have the same columns
        columns = ', '.join(records[0].keys())
        placeholders = ', '.join(['%s'] * len(records[0]))
        query = f"INSERT INTO {self.get_table_name()} ({columns}) VALUES ({placeholders})"

        params_list = [tuple(record.values()) for record in records]
        return self.db.execute_many(query, params_list)

    # ========== UTILITY METHODS ==========

    def count(self, where_clause: str = "", params: Optional[Tuple] = None) -> int:
        """
        Count records in the table.

        Args:
            where_clause (str): Optional WHERE clause (without WHERE keyword)
            params (tuple): Query parameters

        Returns:
            int: Count of records
        """
        query = f"SELECT COUNT(*) as count FROM {self.get_table_name()}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = self.db.execute_query(query, params)
        return result[0]['count'] if result else 0

    def exists(self, id_column: str, id_value: Any) -> bool:
        """
        Check if a record exists.

        Args:
            id_column (str): Name of ID column
            id_value: Value of ID

        Returns:
            bool: True if exists, False otherwise
        """
        query = f"""
            SELECT EXISTS(
                SELECT 1 FROM {self.get_table_name()} 
                WHERE {id_column} = %s
            ) as record_exists
        """

        result = self.db.execute_query(query, (id_value,))
        return bool(result[0]['record_exists']) if result else False

    # ========== STORED PROCEDURES & FUNCTIONS ==========

    def call_stored_procedure(self, proc_name: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Call a stored procedure.

        Args:
            proc_name (str): Name of stored procedure
            params (tuple): Procedure parameters

        Returns:
            list: Procedure results

        Example:
            results = self.call_stored_procedure('GetCompanyOverview', (company_id,))
        """
        return self.db.call_procedure(proc_name, params)

    def call_function(self, func_name: str, params: Optional[Tuple] = None) -> Any:
        """
        Call a user-defined function.

        Args:
            func_name (str): Name of function
            params (tuple): Function parameters

        Returns:
            Function result

        Example:
            price = self.call_function('GetLatestPrice', (company_id,))
        """
        return self.db.call_function(func_name, params)

    # ========== TRANSACTION MANAGEMENT ==========

    def begin_transaction(self):
        """Begin explicit transaction"""
        self.db.begin_transaction()

    def commit(self):
        """Commit current transaction"""
        self.db.commit()

    def rollback(self):
        """Rollback current transaction"""
        self.db.rollback()

    # ========== QUERY HELPERS ==========

    def find_where(self, where_clause: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Find records matching WHERE clause.

        Args:
            where_clause: WHERE condition (without WHERE keyword)
            params: Query parameters

        Returns:
            list: Matching records

        Example:
            users = self.find_where("is_active = %s", (True,))
        """
        query = f"SELECT * FROM {self.get_table_name()} WHERE {where_clause}"
        return self.db.execute_query(query, params)

    def find_one_where(self, where_clause: str, params: Optional[Tuple] = None) -> Optional[Dict[str, Any]]:
        """
        Find single record matching WHERE clause.

        Args:
            where_clause: WHERE condition
            params: Query parameters

        Returns:
            dict: First matching record or None
        """
        query = f"SELECT * FROM {self.get_table_name()} WHERE {where_clause} LIMIT 1"
        results = self.db.execute_query(query, params)
        return results[0] if results else None
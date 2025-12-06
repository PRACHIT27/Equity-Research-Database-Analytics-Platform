"""
Core Package
Infrastructure layer for database connectivity

This package provides the foundational database connection infrastructure
using pymysql driver (NO ORM - direct SQL execution).

Components:
- DatabaseConnection: Singleton connection manager with pymysql
- get_db_connection(): Factory function for singleton instance

Design Patterns:
- Singleton Pattern: Single database connection instance
- Factory Pattern: get_db_connection() factory function
- Context Manager: Automatic cursor management with get_cursor()

Usage:
    from core.connection import get_db_connection

    db = get_db_connection()

    # Execute query
    results = db.execute_query("SELECT * FROM Company")

    # Call stored procedure
    overview = db.call_procedure('GetCompanyOverview', (1,))

    # Call function
    price = db.call_function('GetLatestPrice', (1,))
"""

from core.DatabaseConnection import DatabaseConnection, get_db_connection

__all__ = [
    'DatabaseConnection',
    'get_db_connection'
]
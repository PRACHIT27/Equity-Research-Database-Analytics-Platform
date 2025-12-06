"""
Repositories Package
Data access layer using Repository Pattern

All SQL queries, stored procedure calls, and function calls are in this layer.
Services use repositories, repositories use database connection.

Repository Structure:
- BaseRepository: Abstract base class with common CRUD operations
- Domain Repositories: Specific implementations for each table/domain

Design Patterns:
- Repository Pattern: Abstracts data access
- Abstract Base Class: Provides common functionality
- Dependency Injection: Repositories receive DB connection
"""

from repositories.BaseRepository import BaseRepository
from repositories.CompanyRepository import CompanyRepository, SectorRepository
from repositories.PriceRepository import PriceRepository
from repositories.ForecastRepository import ForecastRepository
from repositories.FinancialRepository import FinancialRepository
from repositories.UserRepository import UserRepository, DepartmentRepository

__all__ = [
    # Base
    'BaseRepository',

    # Company domain
    'CompanyRepository',
    'SectorRepository',

    # Price domain
    'PriceRepository',

    # Forecast domain
    'ForecastRepository',

    # Financial domain
    'FinancialRepository',

    # User domain
    'UserRepository',
    'DepartmentRepository',


]

# Repository usage examples:
"""
Example 1: Using CompanyRepository

from repositories.company_repository import CompanyRepository
from core.connection import get_db_connection

db = get_db_connection()
company_repo = CompanyRepository(db)

# CRUD operations
companies = company_repo.find_all_with_sectors()
company = company_repo.find_by_ticker('AAPL')
company_repo.create('NVDA', 'NVIDIA', sector_id=1, ...)
company_repo.update_by_id(company_id, market_cap=1500000)
company_repo.delete_with_dependencies(company_id)

# Stored procedure
overview = company_repo.get_overview(company_id)


Example 2: Using PriceRepository with Stored Procedure

from repositories.price_repository import PriceRepository

price_repo = PriceRepository(db)

# Insert using stored procedure (includes validation)
success = price_repo.create_using_procedure(
    company_id=1, 
    trading_date=date.today(),
    open_price=100.0,
    high_price=105.0,
    low_price=95.0,
    close_price=102.0,
    volume=1000000
)


Example 3: Using UserRepository with Functions

from repositories.user_repository import UserRepository

user_repo = UserRepository(db)

# Authenticate using stored procedure
user = user_repo.authenticate('admin', 'password')

# Check permission using function
has_delete = user_repo.has_permission(user_id, 'DELETE')

# Get permission level using function
level = user_repo.get_permission_level(user_id)


Example 4: Using FinancialRepository (Superclass/Subclass)

from repositories.financial_repository import FinancialRepository

financial_repo = FinancialRepository(db)

# Create income statement (superclass + subclass)
statement_id = financial_repo.create_financial_statement(
    company_id=1, 
    statement_type='Income',
    fiscal_year=2024,
    fiscal_period='Q3',
    reporting_date=date(2024, 9, 30)
)

financial_repo.create_income_statement(
    statement_id=statement_id,
    company_id=1,
    revenue=100000.0,
    net_income=25000.0
)

# Calculate metrics using stored procedure
financial_repo.call_stored_procedure('CalculateValuationMetrics', (company_id, date.today()))
"""
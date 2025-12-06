"""
Company Controller
Handles all company-related operations for UI
FIXED: Now properly uses Service layer instead of direct repository access
"""

from services.CompanyService import CompanyService
from core.DatabaseConnection import get_db_connection
from repositories.CompanyRepository import CompanyRepository, SectorRepository

class CompanyController:
    """
    Controller for company operations.
    Delegates all business logic to CompanyService.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CompanyController, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Initialize repositories
        db = get_db_connection()
        company_repo = CompanyRepository(db)
        sector_repo = SectorRepository(db)

        # Initialize service
        self._service = CompanyService(company_repo, sector_repo)
        self._initialized = True

    # ========== COMPANY OPERATIONS ==========

    def get_all_companies(self):
        """Get all companies with sector information"""
        try:
            return self._service.get_all_companies()
        except Exception as e:
            return {'success': False, 'message': str(e), 'data': []}

    def get_company_by_id(self, company_id):
        """Get company by ID"""
        try:
            return self._service.get_company_by_id(company_id)
        except Exception as e:
            return None

    def get_company_by_ticker(self, ticker):
        """Get company by ticker symbol"""
        try:
            return self._service.get_company_by_ticker(ticker)
        except Exception as e:
            return None

    def search_companies(self, search_term, limit=20):
        """Search companies by ticker or name"""
        try:
            return self._service.search_companies(search_term, limit)
        except Exception as e:
            return []

    def create_company(self, ticker_symbol, company_name, sector_id,
                       market_cap=None, exchange=None,
                       incorporation_country=None,
                       founded_date=None, description=None, currency = None):
        """Create new company with validation through service"""
        try:
            result = self._service.create_company(
                ticker_symbol=ticker_symbol,
                company_name=company_name,
                sector_id=sector_id,
                currency=currency,
                market_cap=market_cap,
                exchange=exchange,
                incorporation_country=incorporation_country,
                founded_date=founded_date,
                description=description,
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def update_company(self, company_id, company_name=None, market_cap=None,
                       employees=None, headquarters=None, description=None):
        """Update company information through service"""
        try:
            result = self._service.update_company(
                company_id=company_id,
                company_name=company_name,
                market_cap=market_cap,
                headquarters=headquarters,
                description=description
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def delete_company(self, company_id, confirm=False):
        """Delete company with confirmation through service"""
        try:
            result = self._service.delete_company(company_id, confirm)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    # ========== SECTOR OPERATIONS ==========

    def get_all_sectors(self):
        """Get all sectors - through service"""
        try:
            return self._service.get_all_sectors()
        except Exception as e:
            return []

    def get_sector_by_id(self, sector_id):
        """Get sector by ID - through service"""
        try:
            return self._service.get_sector_by_id(sector_id)
        except Exception as e:
            return None

    def get_companies_by_sector(self, sector_id):
        """Get all companies in a sector through service"""
        try:
            return self._service.get_companies_by_sector(sector_id)
        except Exception as e:
            return []

    def get_sector_statistics(self):
        """Get sector statistics with aggregations through service"""
        try:
            return self._service.get_sector_statistics()
        except Exception as e:
            return []


def get_company_controller():
    """Get singleton instance"""
    return CompanyController()
"""
Price Controller
Handles all stock price operations for UI
FIXED: Now properly uses Service layer instead of direct repository access
"""

from services.PriceService import PriceService
from core.DatabaseConnection import get_db_connection
from repositories.PriceRepository import PriceRepository
from repositories.CompanyRepository import CompanyRepository

class PriceController:
    """
    Controller for stock price operations.
    Delegates all business logic to PriceService.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PriceController, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        db = get_db_connection()
        price_repo = PriceRepository(db)
        company_repo = CompanyRepository(db)

        self._service = PriceService(price_repo, company_repo)
        self._initialized = True

    # ========== PRICE OPERATIONS ==========

    def get_latest_prices(self):
        """Get latest prices for all companies through service"""
        try:
            return self._service.get_latest_prices()
        except Exception as e:
            return []

    def get_price_history(self, company_id, start_date, end_date):
        """Get price history for date range through service"""
        try:
            return self._service.get_price_history(company_id, start_date, end_date)
        except Exception as e:
            return []

    def get_price_by_company_and_date(self, company_id, trading_date):
        """Get specific price record through service"""
        try:
            # Service doesn't have this method, add it or use repo
            return self._service.price_repo.find_by_company_and_date(company_id, trading_date)
        except Exception as e:
            return None

    def add_stock_price(self, company_id, trading_date, open_price, high_price,
                        low_price, close_price, volume):
        """
        Add stock price through service with validation.
        Service handles all validation and business logic.
        """
        try:
            result = self._service.add_stock_price(
                company_id=company_id,
                trading_date=trading_date,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume=volume
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def update_stock_price(self, company_id, trading_date, open_price=None,
                           high_price=None, low_price=None, close_price=None,
                           volume=None):
        """Update stock price through service"""
        try:
            result = self._service.update_stock_price(
                company_id=company_id,
                trading_date=trading_date,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume=volume
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def delete_stock_price(self, company_id, trading_date):
        """Delete stock price record through service"""
        try:
            result = self._service.delete_stock_price(company_id, trading_date)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    # ========== ANALYTICS ==========

    def get_price_statistics(self, company_id, days=30):
        """Get price statistics for last N days through service"""
        try:
            return self._service.get_price_statistics(company_id, days)
        except Exception as e:
            return None

    def get_daily_returns(self, company_id, limit=30):
        """Get daily returns through repository (read-only analytics)"""
        try:
            return self._service.price_repo.get_daily_returns(company_id, limit)
        except Exception as e:
            return []

    def get_moving_average(self, company_id, window=20):
        """Calculate moving average through repository (read-only analytics)"""
        try:
            return self._service.price_repo.get_moving_average(company_id, window)
        except Exception as e:
            return []


def get_price_controller():
    """Get singleton instance"""
    return PriceController()
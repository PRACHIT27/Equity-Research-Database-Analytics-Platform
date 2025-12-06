"""
Analytics Controller
Handles analytics, reports, and stored procedures
FIXED: Now properly uses Service layer instead of direct repository access
"""

from services.AnalyticsService import AnalyticsService
from core.DatabaseConnection import get_db_connection
from repositories.CompanyRepository import CompanyRepository
from repositories.PriceRepository import PriceRepository
from repositories.ForecastRepository import ForecastRepository
from repositories.FinancialRepository import FinancialRepository

class AnalyticsController:
    """
    Controller for analytics and reporting.
    Delegates all business logic to AnalyticsService.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AnalyticsController, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Initialize repositories
        db = get_db_connection()
        company_repo = CompanyRepository(db)
        price_repo = PriceRepository(db)
        forecast_repo = ForecastRepository(db)
        financial_repo = FinancialRepository(db)

        # Initialize service
        self._service = AnalyticsService(
            company_repo,
            price_repo,
            forecast_repo,
            financial_repo
        )
        self._initialized = True

    # ========== STORED PROCEDURE REPORTS ==========

    def get_top_performer(self, days, limit=10):
        """
        Get top performing stocks through service.
        Uses stored procedure: GetTopPerformers
        """
        try:
            return self._service.get_top_performers(days, limit)
        except Exception as e:
            return []

    def get_company_overview(self, company_id):
        """
        Get comprehensive company overview through service.
        Uses stored procedure: GetCompanyOverview
        """
        try:
            return self._service.get_company_overview(company_id)
        except Exception as e:
            return None

    # ========== SECTOR ANALYSIS ==========

    def get_sector_statistic(self):
        """Get sector statistics with aggregations through service"""
        try:
            return self._service.get_sector_statistics()
        except Exception as e:
            return []


    def get_sector_performance(self):
        """Get sector performance analysis through service"""
        try:
            return self._service.get_sector_performance()
        except Exception as e:
            return []

    # ========== PERFORMANCE ANALYSIS ==========

    def get_price_performance(self, company_id, days=30):
        """Get price performance for a company (read-only analytics)"""
        try:
            stats = self._service.price_repo.get_price_statistics(company_id, days)
            returns = self._service.price_repo.get_daily_returns(company_id, days)

            return {
                'statistics': stats,
                'daily_returns': returns
            }
        except Exception as e:
            return {
                'statistics': None,
                'daily_returns': []
            }

    def get_market_overview(self):
        """Get overall market overview (read-only analytics)"""
        try:
            query = """
                    SELECT
                        COUNT(DISTINCT c.company_id) as total_companies,
                        SUM(c.market_cap) as total_market_cap,
                        AVG(c.market_cap) as avg_market_cap,
                        COUNT(DISTINCT sp.trading_date) as trading_days,
                        (SELECT COUNT(*) FROM AnalystForecast) as total_forecasts
                    FROM Company c
                             LEFT JOIN StockPrice sp ON c.company_id = sp.company_id \
                    """
            results = self._service.company_repo.execute_custom_query(query)
            return results[0] if results else {}
        except Exception as e:
            return {}

    # ========== FORECAST ANALYSIS ==========

    def get_recommendation_distribution(self):
        """Get distribution of analyst recommendations (read-only)"""
        try:
            return self._service.forecast_repo.get_recommendation_distribution()
        except Exception as e:
            return []

    def get_forecasts_by_sector(self):
        """Get forecasts grouped by sector (read-only)"""
        try:
            return self._service.forecast_repo.get_forecasts_by_sector()
        except Exception as e:
            return []

    def get_recommendation_summary(self):
        """Get summary of all analyst recommendations through service"""
        try:
            return self._service.get_recommendation_summary()
        except Exception as e:
            return {
                'distribution': [],
                'by_sector': []
            }

    # ========== CUSTOM QUERIES (Admin Only) ==========

    def execute_custom_query(self, query):
        """
        Execute custom SQL query through service.
        Service enforces READ-ONLY (SELECT queries only).
        """
        try:
            return self._service.execute_custom_query(query)
        except Exception as e:
            raise Exception(f"Query execution error: {e}")

    # ========== DATABASE FUNCTIONS ==========

    def calculate_daily_return(self, company_id, trading_date):
        """Call CalculateDailyReturn function through service"""
        try:
            return self._service.calculate_daily_return(company_id, trading_date)
        except Exception as e:
            return None

    def get_latest_price(self, company_id):
        """Call GetLatestPrice function through service"""
        try:
            return self._service.get_latest_price(company_id)
        except Exception as e:
            return None

    def calculate_average_volume(self, company_id, days):
        """Call CalculateAverageVolume function through service"""
        try:
            return self._service.calculate_average_volume(company_id, days)
        except Exception as e:
            return None

    # ========== DATABASE STATISTICS ==========

    def get_database_stats(self):
        """Get overall database statistics through service"""
        try:
            return self._service.get_database_stats()
        except Exception as e:
            return {}


def get_analytics_controller():
    """Get singleton instance"""
    return AnalyticsController()
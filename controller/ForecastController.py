"""
Forecast Controller
Handles all forecast operations for UI
FIXED: Now properly uses Service layer instead of direct repository access
"""

from services.ForecastService import ForecastService
from core.DatabaseConnection import get_db_connection
from repositories.ForecastRepository import ForecastRepository
from repositories.CompanyRepository import CompanyRepository

class ForecastController:
    """
    Controller for forecast operations.
    Delegates all business logic to ForecastService.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ForecastController, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Initialize repositories
        db = get_db_connection()
        forecast_repo = ForecastRepository(db)
        company_repo = CompanyRepository(db)

        # Initialize service
        self._service = ForecastService(forecast_repo, company_repo)
        self._initialized = True

    # ========== FORECAST OPERATIONS ==========

    def get_all_forecasts(self):
        """Get all forecasts with company info through service"""
        try:
            return self._service.get_all_forecasts()
        except Exception as e:
            return []

    def get_latest_forecasts(self, limit=50):
        """Get latest forecasts through repository (read-only)"""
        try:
            return self._service.forecast_repo.get_latest_forecasts(limit)
        except Exception as e:
            return []

    def get_forecast_by_id(self, forecast_id):
        """Get forecast by ID through repository (read-only)"""
        try:
            return self._service.forecast_repo.find_by_id(forecast_id)
        except Exception as e:
            return None

    def get_forecasts_by_company(self, company_id):
        """Get all forecasts for a company through repository (read-only)"""
        try:
            return self._service.forecast_repo.find_by_company(company_id)
        except Exception as e:
            return []

    def create_forecast(self, company_id, forecast_date, target_date,
                        target_price, recommendation, confidence_score,
                        revenue_estimate=None, eps_estimate=None,
                        price_target=None, upside_potential_percent=None,
                        model_version=None):
        """Create new forecast with validation through service"""
        try:
            result = self._service.create_forecast(
                company_id=company_id,
                forecast_date=forecast_date,
                target_date=target_date,
                target_price=target_price,
                recommendation=recommendation,
                confidence_score=confidence_score,
                revenue_estimate=revenue_estimate,
                eps_estimate=eps_estimate,
                price_target=price_target,
                upside_potential_percent=upside_potential_percent,
                model_version=model_version
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def update_forecast(self, forecast_id, target_price=None, recommendation=None,
                        confidence_score=None, revenue_estimate=None, eps_estimate=None):
        """Update forecast through service"""
        try:
            result = self._service.update_forecast(
                forecast_id=forecast_id,
                target_price=target_price,
                recommendation=recommendation,
                confidence_score=confidence_score,
                revenue_estimate=revenue_estimate,
                eps_estimate=eps_estimate
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def delete_forecast(self, forecast_id):
        """Delete forecast through service"""
        try:
            result = self._service.delete_forecast(forecast_id)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    # ========== ANALYTICS ==========

    def get_recommendation_distribution(self):
        """Get distribution of recommendations (read-only analytics)"""
        try:
            return self._service.forecast_repo.get_recommendation_distribution()
        except Exception as e:
            return []

    def get_forecasts_by_sector(self):
        """Get forecasts grouped by sector (read-only analytics)"""
        try:
            return self._service.forecast_repo.get_forecasts_by_sector()
        except Exception as e:
            return []


def get_forecast_controller():
    """Get singleton instance"""
    return ForecastController()
"""
Forecast Service Layer
Business logic for analyst forecast operations
"""

from typing import List, Optional, Dict, Any
from datetime import date
from repositories.ForecastRepository import ForecastRepository
from repositories.CompanyRepository import CompanyRepository
from utils.validators import ForecastValidator
from utils.exceptions import ValidationError, BusinessLogicError

class ForecastService:
    """Service for forecast business logic"""

    def __init__(self, forecast_repo: ForecastRepository, company_repo: CompanyRepository):
        self.forecast_repo = forecast_repo
        self.company_repo = company_repo
        self.validator = ForecastValidator()

    def create_forecast(self, company_id: int, forecast_date: date, target_date: date,
                        target_price: float, recommendation: str, confidence_score: float,
                        **kwargs) -> Dict[str, Any]:
        """Create forecast with validation"""

        # Validate company exists
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        # Validate inputs
        self.validator.validate_dates(forecast_date, target_date)
        self.validator.validate_target_price(target_price)
        self.validator.validate_confidence(confidence_score)

        # Create forecast
        rows = self.forecast_repo.create(
            company_id=company_id,
            forecast_date=forecast_date,
            target_date=target_date,
            target_price=target_price,
            recommendation=recommendation,
            confidence_score=confidence_score,
            **kwargs
        )

        return {
            'success': True,
            'message': f"Forecast created for {company['ticker_symbol']}"
        }

    def get_all_forecasts(self) -> List[Dict[str, Any]]:
        """Get all forecasts"""
        return self.forecast_repo.find_all()

    def update_forecast(self, forecast_id: int, **kwargs) -> Dict[str, Any]:
        """Update forecast"""
        existing = self.forecast_repo.find_by_id(forecast_id)
        if not existing:
            raise BusinessLogicError("Forecast not found")

        rows = self.forecast_repo.update_by_id(forecast_id, **kwargs)
        return {'success': True, 'message': "Forecast updated"}

    def delete_forecast(self, forecast_id: int) -> Dict[str, Any]:
        """Delete forecast"""
        existing = self.forecast_repo.find_by_id(forecast_id)
        if not existing:
            raise BusinessLogicError("Forecast not found")

        self.forecast_repo.delete_by_id(forecast_id)
        return {'success': True, 'message': "Forecast deleted"}
"""
Price Service Layer
Business logic for stock price operations
"""

from typing import List, Optional, Dict, Any
from datetime import date
from repositories.PriceRepository import PriceRepository
from repositories.CompanyRepository import CompanyRepository
from utils.validators import PriceValidator
from utils.exceptions import ValidationError, BusinessLogicError

class PriceService:
    """Service for stock price business logic"""

    def __init__(self, price_repo: PriceRepository, company_repo: CompanyRepository):
        self.price_repo = price_repo
        self.company_repo = company_repo
        self.validator = PriceValidator()

    def add_stock_price(self, company_id: int, trading_date: date,
                        open_price: float, high_price: float, low_price: float,
                        close_price: float, volume: int) -> Dict[str, Any]:
        """Add stock price with validation"""

        # Validate company exists
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        # Validate prices using validator
        self.validator.validate_price(open_price, "Open price")
        self.validator.validate_price(high_price, "High price")
        self.validator.validate_price(low_price, "Low price")
        self.validator.validate_price(close_price, "Close price")

        # Validate OHLC relationships
        self.validator.validate_ohlc(open_price, high_price, low_price, close_price)

        # Validate volume
        self.validator.validate_volume(volume)

        # Validate date
        self.validator.validate_date(trading_date)

        # Use stored procedure for insertion (includes trigger validation)
        success = self.price_repo.create_using_procedure(
            company_id, trading_date, open_price, high_price,
            low_price, close_price, volume
        )

        if not success:
            raise BusinessLogicError("Failed to add stock price")

        return {
            'success': True,
            'message': f"Stock price added for {company['ticker_symbol']} on {trading_date}"
        }

    def get_price_history(self, company_id: int, start_date: date,
                          end_date: date) -> List[Dict[str, Any]]:
        """Get price history for date range"""

        # Validate company exists
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        # Validate dates
        if start_date > end_date:
            raise ValidationError("Start date must be before end date")

        return self.price_repo.find_by_date_range(company_id, start_date, end_date)

    def get_latest_prices(self) -> List[Dict[str, Any]]:
        """Get latest prices for all companies"""
        return self.price_repo.get_latest_prices_all()

    def update_stock_price(self, company_id: int, trading_date: date,
                           open_price: Optional[float] = None,
                           high_price: Optional[float] = None,
                           low_price: Optional[float] = None,
                           close_price: Optional[float] = None,
                           volume: Optional[int] = None) -> Dict[str, Any]:
        """Update stock price with validation"""

        # Check if price exists
        existing = self.price_repo.find_by_company_and_date(company_id, trading_date)
        if not existing:
            raise BusinessLogicError("Price record not found")

        # Validate new values if provided
        if open_price is not None:
            self.validator.validate_price(open_price, "Open price")
        if high_price is not None:
            self.validator.validate_price(high_price, "High price")
        if low_price is not None:
            self.validator.validate_price(low_price, "Low price")
        if close_price is not None:
            self.validator.validate_price(close_price, "Close price")
        if volume is not None:
            self.validator.validate_volume(volume)

        # Build update dict
        updates = {}
        if open_price is not None:
            updates['open_price'] = open_price
        if high_price is not None:
            updates['high_price'] = high_price
        if low_price is not None:
            updates['low_price'] = low_price
        if close_price is not None:
            updates['close_price'] = close_price
        if volume is not None:
            updates['volume'] = volume

        if not updates:
            raise ValidationError("No fields to update")

        # Update
        rows = self.price_repo.update_by_company_and_date(company_id, trading_date, **updates)

        if rows == 0:
            raise BusinessLogicError("No changes made")

        return {
            'success': True,
            'message': "Stock price updated successfully"
        }

    def delete_stock_price(self, company_id: int, trading_date: date) -> Dict[str, Any]:
        """Delete stock price"""

        # Check if exists
        existing = self.price_repo.find_by_company_and_date(company_id, trading_date)
        if not existing:
            raise BusinessLogicError("Price record not found")

        # Delete
        rows = self.price_repo.delete_by_company_and_date(company_id, trading_date)

        return {
            'success': True,
            'message': "Stock price deleted successfully"
        }

    def get_price_statistics(self, company_id: int, days: int = 30) -> Optional[Dict[str, Any]]:
        """Get price statistics"""

        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        return self.price_repo.get_price_statistics(company_id, days)
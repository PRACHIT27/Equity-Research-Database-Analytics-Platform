"""
Input Validation Utilities
Validates user inputs and business rules
"""

import re
from datetime import datetime, date
from typing import Optional
from utils.exceptions import ValidationError


class CompanyValidator:
    """Validator for company-related inputs"""

    @staticmethod
    def validate_ticker(ticker: str):
        """Validate ticker symbol"""
        if not ticker:
            raise ValidationError("Ticker symbol is required")

        ticker = ticker.strip().upper()

        if len(ticker) < 1:
            raise ValidationError("Ticker symbol must be at least 1 character")

        if len(ticker) > 10:
            raise ValidationError("Ticker symbol must be 10 characters or less")

        if not re.match(r'^[A-Z0-9]+$', ticker):
            raise ValidationError("Ticker symbol must contain only letters and numbers")

    @staticmethod
    def validate_company_name(name: str):
        """Validate company name"""
        if not name:
            raise ValidationError("Company name is required")

        name = name.strip()

        if len(name) < 2:
            raise ValidationError("Company name must be at least 2 characters")

        if len(name) > 200:
            raise ValidationError("Company name must be 200 characters or less")

    @staticmethod
    def validate_market_cap(market_cap: float):
        """Validate market capitalization (in millions)"""
        if market_cap < 0:
            raise ValidationError("Market cap cannot be negative")

        if market_cap > 10000000:  # 10 trillion
            raise ValidationError("Market cap seems unrealistically high (max 10 trillion)")

    @staticmethod
    def validate_employees(employees: int):
        """Validate employee count"""
        if employees < 0:
            raise ValidationError("Employee count cannot be negative")

        if employees > 20000000:  # 20 million
            raise ValidationError("Employee count seems unrealistically high")

    @staticmethod
    def validate_fiscal_year_end(month: int):
        """Validate fiscal year end month"""
        if month < 1 or month > 12:
            raise ValidationError("Fiscal year end must be between 1 (Jan) and 12 (Dec)")

    @staticmethod
    def validate_founded_date(founded_date: date):
        """Validate founded date"""
        if founded_date.year < 1800:
            raise ValidationError("Founded year must be 1800 or later")

        if founded_date > date.today():
            raise ValidationError("Founded date cannot be in the future")


class PriceValidator:
    """Validator for stock price inputs"""

    @staticmethod
    def validate_price(price: float, price_type: str = "Price"):
        """Validate stock price"""
        if price is None:
            raise ValidationError(f"{price_type} is required")

        if price <= 0:
            raise ValidationError(f"{price_type} must be positive")

        if price > 1000000:  # $1M per share
            raise ValidationError(f"{price_type} seems unrealistically high (max $1M)")

    @staticmethod
    def validate_ohlc(open_price: float, high: float, low: float, close: float):
        """Validate OHLC price relationships"""
        # This is the most critical validation for stock prices
        if high < low:
            raise ValidationError("High price must be >= Low price")

        if high < open_price:
            raise ValidationError("High price must be >= Open price")

        if high < close:
            raise ValidationError("High price must be >= Close price")

        if low > open_price:
            raise ValidationError("Low price must be <= Open price")

        if low > close:
            raise ValidationError("Low price must be <= Close price")

    @staticmethod
    def validate_volume(volume: int):
        """Validate trading volume"""
        if volume is None:
            raise ValidationError("Volume is required")

        if volume < 0:
            raise ValidationError("Volume cannot be negative")

        if volume > 1000000000000:  # 1 trillion shares
            raise ValidationError("Volume seems unrealistically high")

    @staticmethod
    def validate_date(trading_date: date):
        """Validate trading date"""
        if trading_date is None:
            raise ValidationError("Trading date is required")

        if trading_date > date.today():
            raise ValidationError("Trading date cannot be in the future")

        if trading_date.year < 1900:
            raise ValidationError("Trading date must be 1900 or later")


class UserValidator:
    """Validator for user inputs"""

    @staticmethod
    def validate_username(username: str):
        """Validate username"""
        if not username:
            raise ValidationError("Username is required")

        username = username.strip()

        if len(username) < 3:
            raise ValidationError("Username must be at least 3 characters")

        if len(username) > 50:
            raise ValidationError("Username must be 50 characters or less")

        if not re.match(r'^[a-zA-Z0-9_]+$', username):
            raise ValidationError("Username can only contain letters, numbers, and underscores")

    @staticmethod
    def validate_email(email: str):
        """Validate email address"""
        if not email:
            raise ValidationError("Email is required")

        email = email.strip().lower()

        # RFC 5322 simplified email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

        if not re.match(email_pattern, email):
            raise ValidationError("Invalid email format (example: user@example.com)")

        if len(email) > 255:
            raise ValidationError("Email must be 255 characters or less")

    @staticmethod
    def validate_password(password: str):
        """Validate password strength"""
        if not password:
            raise ValidationError("Password is required")

        if len(password) < 6:
            raise ValidationError("Password must be at least 6 characters")

        if len(password) > 255:
            raise ValidationError("Password must be 255 characters or less")

        # Optional: Add more strength requirements
        # if not re.search(r'[A-Z]', password):
        #     raise ValidationError("Password must contain at least one uppercase letter")
        # if not re.search(r'[a-z]', password):
        #     raise ValidationError("Password must contain at least one lowercase letter")
        # if not re.search(r'[0-9]', password):
        #     raise ValidationError("Password must contain at least one number")

    @staticmethod
    def validate_phone(phone: Optional[str]):
        """Validate phone number (optional)"""
        if phone is None or phone.strip() == "":
            return  # Phone is optional

        phone = phone.strip()

        # Allow various formats: +1-555-0100, (555) 555-0100, 555.555.0100, etc.
        phone_pattern = r'^[\+]?[(]?[0-9]{1,4}[)]?[-\s\.]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,9}$'

        if not re.match(phone_pattern, phone):
            raise ValidationError("Invalid phone number format")

    @staticmethod
    def validate_full_name(full_name: str):
        """Validate full name"""
        if not full_name:
            raise ValidationError("Full name is required")

        full_name = full_name.strip()

        if len(full_name) < 2:
            raise ValidationError("Full name must be at least 2 characters")

        if len(full_name) > 100:
            raise ValidationError("Full name must be 100 characters or less")


class ForecastValidator:
    """Validator for forecast inputs"""

    @staticmethod
    def validate_dates(forecast_date: date, target_date: date):
        """Validate forecast dates"""
        if forecast_date is None:
            raise ValidationError("Forecast date is required")

        if target_date is None:
            raise ValidationError("Target date is required")

        if target_date <= forecast_date:
            raise ValidationError("Target date must be after forecast date")

        if forecast_date > date.today():
            raise ValidationError("Forecast date cannot be in the future")

        # Reasonable forecast horizon (e.g., within 5 years)
        days_difference = (target_date - forecast_date).days
        if days_difference > 1825:  # 5 years
            raise ValidationError("Target date cannot be more than 5 years from forecast date")

    @staticmethod
    def validate_confidence(confidence: float):
        """Validate confidence score (0.0 to 1.0)"""
        if confidence is None:
            raise ValidationError("Confidence score is required")

        if confidence < 0 or confidence > 1:
            raise ValidationError("Confidence score must be between 0 and 1")

    @staticmethod
    def validate_target_price(target_price: float):
        """Validate target price"""
        if target_price is None:
            raise ValidationError("Target price is required")

        if target_price <= 0:
            raise ValidationError("Target price must be positive")

        if target_price > 1000000:
            raise ValidationError("Target price seems unrealistically high")

    @staticmethod
    def validate_recommendation(recommendation: str):
        """Validate recommendation"""
        valid_recommendations = ['Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell']

        if recommendation not in valid_recommendations:
            raise ValidationError(
                f"Invalid recommendation. Must be one of: {', '.join(valid_recommendations)}"
            )

    @staticmethod
    def validate_revenue_estimate(revenue: Optional[float]):
        """Validate revenue estimate"""
        if revenue is not None and revenue < 0:
            raise ValidationError("Revenue estimate cannot be negative")

    @staticmethod
    def validate_eps_estimate(eps: Optional[float]):
        """Validate EPS estimate"""
        if eps is not None and abs(eps) > 1000:
            raise ValidationError("EPS estimate seems unrealistic")


class FinancialValidator:
    """Validator for financial statement inputs"""

    @staticmethod
    def validate_fiscal_year(fiscal_year: int):
        """Validate fiscal year"""
        current_year = datetime.now().year

        if fiscal_year < 1900:
            raise ValidationError("Fiscal year must be 1900 or later")

        if fiscal_year > current_year + 1:
            raise ValidationError("Fiscal year cannot be more than 1 year in the future")

    @staticmethod
    def validate_fiscal_period(fiscal_period: str):
        """Validate fiscal period"""
        valid_periods = ['Q1', 'Q2', 'Q3', 'Q4', 'FY']

        if fiscal_period not in valid_periods:
            raise ValidationError(
                f"Invalid fiscal period. Must be one of: {', '.join(valid_periods)}"
            )

    @staticmethod
    def validate_reporting_date(reporting_date: date, fiscal_year: int):
        """Validate reporting date is reasonable for fiscal year"""
        if reporting_date.year < fiscal_year - 1:
            raise ValidationError("Reporting date too early for fiscal year")

        if reporting_date.year > fiscal_year + 1:
            raise ValidationError("Reporting date too late for fiscal year")

    @staticmethod
    def validate_amount(amount: Optional[float], field_name: str, allow_negative: bool = False):
        """Validate financial amount"""
        if amount is None:
            return  # Most amounts are optional

        if not allow_negative and amount < 0:
            raise ValidationError(f"{field_name} cannot be negative")

        if abs(amount) > 1000000000:  # 1 trillion
            raise ValidationError(f"{field_name} seems unrealistically high")

    @staticmethod
    def validate_ratio(ratio: Optional[float], ratio_name: str, min_val: float = -10, max_val: float = 1000):
        """Validate financial ratio"""
        if ratio is None:
            return  # Most ratios are optional

        if ratio < min_val or ratio > max_val:
            raise ValidationError(f"{ratio_name} must be between {min_val} and {max_val}")

    @staticmethod
    def validate_percentage(percentage: Optional[float], field_name: str):
        """Validate percentage (stored as decimal, e.g., 0.15 = 15%)"""
        if percentage is None:
            return  # Most percentages are optional

        if percentage < -1 or percentage > 1:
            raise ValidationError(f"{field_name} must be between -100% and 100% (-1 to 1)")


class ValuationValidator:
    """Validator for valuation metrics"""

    @staticmethod
    def validate_pe_ratio(pe_ratio: Optional[float]):
        """Validate P/E ratio"""
        if pe_ratio is None:
            return

        if pe_ratio < -100 or pe_ratio > 10000:
            raise ValidationError("P/E ratio must be between -100 and 10,000")

    @staticmethod
    def validate_pb_ratio(pb_ratio: Optional[float]):
        """Validate P/B ratio"""
        if pb_ratio is None:
            return

        if pb_ratio < 0 or pb_ratio > 1000:
            raise ValidationError("P/B ratio must be between 0 and 1,000")

    @staticmethod
    def validate_roe(roe: Optional[float]):
        """Validate ROE (Return on Equity)"""
        if roe is None:
            return

        if roe < -2 or roe > 5:  # -200% to 500%
            raise ValidationError("ROE must be between -200% and 500% (-2 to 5)")

    @staticmethod
    def validate_roa(roa: Optional[float]):
        """Validate ROA (Return on Assets)"""
        if roa is None:
            return

        if roa < -1 or roa > 1:  # -100% to 100%
            raise ValidationError("ROA must be between -100% and 100% (-1 to 1)")

    @staticmethod
    def validate_debt_to_equity(debt_to_equity: Optional[float]):
        """Validate Debt-to-Equity ratio"""
        if debt_to_equity is None:
            return

        if debt_to_equity < 0 or debt_to_equity > 100:
            raise ValidationError("Debt-to-Equity ratio must be between 0 and 100")

    @staticmethod
    def validate_current_ratio(current_ratio: Optional[float]):
        """Validate Current ratio"""
        if current_ratio is None:
            return

        if current_ratio < 0 or current_ratio > 100:
            raise ValidationError("Current ratio must be between 0 and 100")


# ========== GENERAL VALIDATORS ==========

def validate_positive_integer(value: int, field_name: str):
    """Validate positive integer"""
    if value < 0:
        raise ValidationError(f"{field_name} must be positive")


def validate_positive_float(value: float, field_name: str):
    """Validate positive float"""
    if value <= 0:
        raise ValidationError(f"{field_name} must be positive")


def validate_required_field(value, field_name: str):
    """Validate required field is not None or empty"""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        raise ValidationError(f"{field_name} is required")


def validate_string_length(value: str, field_name: str, min_length: int = 0, max_length: int = 255):
    """Validate string length"""
    if value is None:
        return

    length = len(value.strip())

    if length < min_length:
        raise ValidationError(f"{field_name} must be at least {min_length} characters")

    if length > max_length:
        raise ValidationError(f"{field_name} must be {max_length} characters or less")
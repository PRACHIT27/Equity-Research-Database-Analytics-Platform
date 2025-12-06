"""
Utils Package
Utility modules for the application

Modules:
- validators: Input validation classes for all entities
- exceptions: Custom exception classes
- formatters: Display formatting utilities
- decorators: Reusable decorators (future)
"""

# Import validators
from utils.validators import (
    CompanyValidator,
    PriceValidator,
    UserValidator,
    ForecastValidator,
    FinancialValidator,
    ValuationValidator,
    validate_positive_integer,
    validate_positive_float,
    validate_required_field,
    validate_string_length
)

# Import exceptions
from utils.exceptions import (
    ValidationError,
    BusinessLogicError,
    AuthenticationError,
    AuthorizationError,
    DatabaseError,
    NotFoundError,
    DuplicateError
)

# Import formatters
from utils.formatters import (
    format_currency,
    format_number,
    format_percentage,
    format_date,
    format_datetime,
    format_ratio,
    format_market_cap_tier,
    format_recommendation_color,
    truncate_text
)

__all__ = [
    # Validators
    'CompanyValidator',
    'PriceValidator',
    'UserValidator',
    'ForecastValidator',
    'FinancialValidator',
    'ValuationValidator',
    'validate_positive_integer',
    'validate_positive_float',
    'validate_required_field',
    'validate_string_length',

    # Exceptions
    'ValidationError',
    'BusinessLogicError',
    'AuthenticationError',
    'AuthorizationError',
    'DatabaseError',
    'NotFoundError',
    'DuplicateError',

    # Formatters
    'format_currency',
    'format_number',
    'format_percentage',
    'format_date',
    'format_datetime',
    'format_ratio',
    'format_market_cap_tier',
    'format_recommendation_color',
    'truncate_text'
]
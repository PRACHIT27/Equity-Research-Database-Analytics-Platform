"""
UI Pages Package
Individual page modules for the application
"""

from ui.pages.dashboard import show_dashboard
from ui.pages.companies import show_companies
from ui.pages.stock_prices import show_stock_prices
from ui.pages.forecasts import show_forecasts
from ui.pages.financial_statements import show_financial_statements
from ui.pages.valuation_metrics import show_valuation_metrics
from ui.pages.users import show_user_management
from ui.pages.analytics import show_analytics

__all__ = [
    'show_dashboard',
    'show_companies',
    'show_stock_prices',
    'show_forecasts',
    'show_financial_statements',
    'show_valuation_metrics',
    'show_user_management',
    'show_analytics'
]
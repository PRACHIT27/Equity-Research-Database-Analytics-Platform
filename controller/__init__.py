"""
Controllers Package
Facade pattern - UI layer only imports from here

Each controller handles a specific domain:
- CompanyController: Companies and sectors
- PriceController: Stock prices
- ForecastController: Analyst forecasts
- FinancialController: Financial statements and valuation metrics
- UserController: Authentication and user management
- WatchlistController: User watchlists
- AnalyticsController: Reports and analytics

UI pages import specific controllers they need.
"""

from controller.CompanyController import CompanyController, get_company_controller
from controller.PriceController import PriceController, get_price_controller
from controller.ForecastController import ForecastController, get_forecast_controller
from controller.FinancialController import FinancialController, get_financial_controller
from controller.UserController import UserController, get_user_controller
from controller.AnalyticsController import AnalyticsController, get_analytics_controller

__all__ = [
    'CompanyController',
    'PriceController',
    'ForecastController',
    'FinancialController',
    'UserController',
    'AnalyticsController',
    'get_company_controller',
    'get_price_controller',
    'get_forecast_controller',
    'get_financial_controller',
    'get_user_controller',
    'get_analytics_controller'
]
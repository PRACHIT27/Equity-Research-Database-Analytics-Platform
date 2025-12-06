"""
Analytics Service Layer
Business logic for analytics, reports, and stored procedures
"""

from typing import List, Optional, Dict, Any
from repositories.CompanyRepository import CompanyRepository
from repositories.PriceRepository import PriceRepository
from repositories.ForecastRepository import ForecastRepository
from repositories.FinancialRepository import FinancialRepository
from utils.exceptions import ValidationError, BusinessLogicError

class AnalyticsService:
    """
    Service for analytics and reporting operations.
    Orchestrates data from multiple repositories for complex analysis.
    """

    def __init__(self, company_repo: CompanyRepository, price_repo: PriceRepository,
                 forecast_repo: ForecastRepository, financial_repo: FinancialRepository):
        """
        Initialize analytics service.

        Args:
            company_repo: CompanyRepository instance
            price_repo: PriceRepository instance
            forecast_repo: ForecastRepository instance
            financial_repo: FinancialRepository instance
        """
        self.company_repo = company_repo
        self.price_repo = price_repo
        self.forecast_repo = forecast_repo
        self.financial_repo = financial_repo

    # ========== STORED PROCEDURE REPORTS ==========

    def get_top_performers(self, days: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get top performing stocks using stored procedure.

        Args:
            days: Number of days to analyze
            limit: Number of top performers to return

        Returns:
            list: Top performing stocks with return percentages
        """
        if days < 1 or days > 365:
            raise ValidationError("Days must be between 1 and 365")

        if limit < 1 or limit > 100:
            raise ValidationError("Limit must be between 1 and 100")

        # Call stored procedure
        results = self.company_repo.call_stored_procedure('GetTopPerformers', (days, limit))

        return results if results else []

    def get_company_overview(self, company_id: int) -> Dict[str, Any]:
        """
        Get comprehensive company overview using stored procedure.

        Args:
            company_id: Company ID

        Returns:
            dict: Complete company overview with latest metrics
        """
        # Validate company exists
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        # Call stored procedure
        results = self.company_repo.call_stored_procedure('GetCompanyOverview', (company_id,))

        return results[0] if results else None

    # ========== SECTOR ANALYSIS ==========

    def get_sector_statistics(self) -> List[Dict[str, Any]]:
        """
        Get comprehensive sector statistics.
        Aggregates company count, market cap, etc. by sector.

        Returns:
            list: Sector statistics with aggregations
        """
        return self.company_repo.get_count_by_sector()

    def get_sector_performance(self) -> List[Dict[str, Any]]:
        """
        Get sector performance analysis.
        Combines company data, prices, and valuations by sector.

        Returns:
            list: Sector performance metrics
        """
        query = """
                SELECT
                    s.sector_name,
                    COUNT(DISTINCT c.company_id) as company_count,
                    AVG(c.market_cap) as avg_market_cap,
                    SUM(c.market_cap) as total_market_cap,
                    AVG(vm.pe_ratio) as avg_pe_ratio,
                    AVG(vm.roe) as avg_roe,
                    AVG(vm.debt_to_equity) as avg_debt_to_equity,
                    COUNT(DISTINCT af.forecast_id) as forecast_count
                FROM Sector s
                         LEFT JOIN Company c ON s.sector_id = c.sector_id
                         LEFT JOIN ValuationMetrics vm ON c.company_id = vm.company_id
                         LEFT JOIN AnalystForecast af ON c.company_id = af.company_id
                GROUP BY s.sector_id, s.sector_name
                ORDER BY total_market_cap DESC \
                """

        return self.company_repo.execute_custom_query(query)

    # ========== PRICE ANALYSIS ==========

    def get_price_volatility_analysis(self, company_id: int, days: int = 30) -> Dict[str, Any]:
        """
        Analyze price volatility using database function.

        Args:
            company_id: Company ID
            days: Number of days to analyze

        Returns:
            dict: Volatility statistics
        """
        # Get price statistics
        stats = self.price_repo.get_price_statistics(company_id, days)

        # Calculate volatility using database function
        volatility = self.company_repo.call_function('CalculateVolatility', (company_id, days))

        return {
            'statistics': stats,
            'volatility': volatility,
            'company_id': company_id,
            'period_days': days
        }

    def get_moving_averages(self, company_id: int, windows: List[int] = [20, 50, 200]) -> Dict[str, Any]:
        """
        Calculate moving averages for multiple windows.

        Args:
            company_id: Company ID
            windows: List of window sizes (e.g., [20, 50, 200] for 20-day, 50-day, 200-day MA)

        Returns:
            dict: Moving averages for each window
        """
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        moving_averages = {}

        for window in windows:
            ma_data = self.price_repo.get_moving_average(company_id, window)
            moving_averages[f'ma_{window}'] = ma_data

        return {
            'company_id': company_id,
            'ticker': company['ticker_symbol'],
            'moving_averages': moving_averages
        }

    # ========== FORECAST ANALYSIS ==========

    def get_recommendation_summary(self) -> Dict[str, Any]:
        """
        Get summary of all analyst recommendations.

        Returns:
            dict: Recommendation distribution and statistics
        """
        distribution = self.forecast_repo.get_recommendation_distribution()
        by_sector = self.forecast_repo.get_forecasts_by_sector()

        return {
            'distribution': distribution,
            'by_sector': by_sector
        }

    def get_forecast_accuracy(self, company_id: int) -> Dict[str, Any]:
        """
        Analyze forecast accuracy (compare predictions vs actual).

        Args:
            company_id: Company ID

        Returns:
            dict: Forecast accuracy metrics
        """
        # Get historical forecasts
        forecasts = self.forecast_repo.find_by_company(company_id)

        # Get actual prices
        # Compare target prices vs actual prices on target dates
        # This would require more complex logic

        return {
            'company_id': company_id,
            'forecast_count': len(forecasts) if forecasts else 0,
            'message': 'Detailed accuracy analysis would compare predictions vs actuals'
        }

    # ========== VALUATION ANALYSIS ==========

    def calculate_valuation_metrics(self, company_id: int, calculation_date: date) -> Dict[str, Any]:
        """
        Calculate valuation metrics using stored procedure.

        Args:
            company_id: Company ID
            calculation_date: Date for calculation

        Returns:
            dict: Success message
        """
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        # Call stored procedure
        self.company_repo.call_stored_procedure('CalculateValuationMetrics', (company_id, calculation_date))

        return {
            'success': True,
            'message': f"Valuation metrics calculated for {company['ticker_symbol']}"
        }

    def get_valuation_comparison(self, company_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Compare valuation metrics across multiple companies.

        Args:
            company_ids: List of company IDs to compare

        Returns:
            list: Valuation metrics for comparison
        """
        if not company_ids:
            raise ValidationError("At least one company ID required")

        placeholders = ','.join(['%s'] * len(company_ids))

        query = f"""
            SELECT 
                c.ticker_symbol,
                c.company_name,
                s.sector_name,
                vm.pe_ratio,
                vm.pb_ratio,
                vm.ps_ratio,
                vm.roe,
                vm.roa,
                vm.debt_to_equity,
                vm.current_ratio
            FROM ValuationMetrics vm
            INNER JOIN Company c ON vm.company_id = c.company_id
            INNER JOIN Sector s ON c.sector_id = s.sector_id
            WHERE c.company_id IN ({placeholders})
            AND vm.calculation_date = (
                SELECT MAX(calculation_date)
                FROM ValuationMetrics
                WHERE company_id = vm.company_id
            )
            ORDER BY c.ticker_symbol
        """

        return self.company_repo.execute_custom_query(query, tuple(company_ids))

    # ========== DATABASE STATISTICS ==========

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get overall database statistics.

        Returns:
            dict: Database statistics
        """
        query = """
                SELECT
                        (SELECT COUNT(*) FROM Company) as total_companies,
                        (SELECT COUNT(*) FROM Sector) as total_sectors,
                        (SELECT COUNT(*) FROM StockPrice) as total_prices,
                        (SELECT COUNT(*) FROM AnalystForecast) as total_forecasts,
                        (SELECT COUNT(*) FROM FinancialStatement) as total_statements,
                        (SELECT COUNT(*) FROM User WHERE is_active = TRUE) as active_users,
                        (SELECT SUM(market_cap) FROM Company) as total_market_cap,
                        (SELECT AVG(market_cap) FROM Company) as avg_market_cap \
                """

        results = self.company_repo.execute_custom_query(query)
        return results[0] if results else {}

    # ========== CUSTOM QUERIES (Admin Only) ==========

    def execute_custom_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute custom SQL query.
        SECURITY: Only SELECT queries allowed!

        Args:
            query: SQL query string

        Returns:
            list: Query results

        Raises:
            ValidationError: If query is not SELECT
        """
        # Security validation
        if not query.strip().upper().startswith('SELECT'):
            raise ValidationError("Only SELECT queries are allowed for security")

        # Execute query
        try:
            results = self.company_repo.execute_custom_query(query)
            return results if results else []
        except Exception as e:
            raise BusinessLogicError(f"Query execution failed: {e}")

    # ========== USER-DEFINED FUNCTIONS ==========

    def calculate_daily_return(self, company_id: int, trading_date: date) -> float:
        """Call CalculateDailyReturn database function"""
        return self.company_repo.call_function('CalculateDailyReturn', (company_id, trading_date))

    def get_latest_price(self, company_id: int) -> float:
        """Call GetLatestPrice database function"""
        return self.company_repo.call_function('GetLatestPrice', (company_id,))

    def calculate_average_volume(self, company_id: int, days: int) -> int:
        """Call CalculateAverageVolume database function"""
        return self.company_repo.call_function('CalculateAverageVolume', (company_id, days))
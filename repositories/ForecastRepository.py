"""
Forecast Repository
Data access layer for AnalystForecast table
ALL SQL queries here
"""

from typing import List, Optional, Dict, Any
from datetime import date
from repositories.BaseRepository import BaseRepository

class ForecastRepository(BaseRepository):
    """Repository for analyst forecast operations"""

    def get_table_name(self) -> str:
        return "AnalystForecast"

    # ========== CREATE ==========

    def create(self, company_id: int, forecast_date: date, target_date: date,
               target_price: float, revenue_estimate: Optional[float] = None,
               eps_estimate: Optional[float] = None, price_target: Optional[float] = None,
               recommendation: str = 'Hold', upside_potential_percent: Optional[float] = None,
               confidence_score: Optional[float] = None, model_version: Optional[str] = None) -> int:
        """Create new forecast"""

        query = """
                INSERT INTO AnalystForecast (
                    company_id, forecast_date, target_date, target_price,
                    revenue_estimate, eps_estimate, price_target, recommendation,
                    upside_potential_percent, confidence_score, model_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                """

        return self.execute_custom_update(query, (
            company_id, forecast_date, target_date, target_price,
            revenue_estimate, eps_estimate, price_target or target_price,
            recommendation, upside_potential_percent, confidence_score, model_version
        ))

    # ========== READ ==========

    def find_by_id(self, forecast_id: int) -> Optional[Dict[str, Any]]:
        """Find forecast by ID with company info"""

        query = """
                SELECT
                    af.forecast_id,
                    af.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    af.forecast_date,
                    af.target_date,
                    af.target_price,
                    af.revenue_estimate,
                    af.eps_estimate,
                    af.price_target,
                    af.recommendation,
                    af.upside_potential_percent,
                    af.confidence_score,
                    af.model_version
                FROM AnalystForecast af
                         INNER JOIN Company c ON af.company_id = c.company_id
                WHERE af.forecast_id = %s \
                """

        results = self.execute_custom_query(query, (forecast_id,))
        return results[0] if results else None

    def find_by_company(self, company_id: int) -> List[Dict[str, Any]]:
        """Get all forecasts for a company"""

        query = """
                SELECT *
                FROM AnalystForecast
                WHERE company_id = %s
                ORDER BY forecast_date DESC \
                """

        return self.execute_custom_query(query, (company_id,))

    def find_all(self) -> List[Dict[str, Any]]:
        """Get all forecasts with company info"""

        query = """
                SELECT
                    af.forecast_id,
                    af.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    s.sector_name,
                    af.forecast_date,
                    af.target_date,
                    af.target_price,
                    af.revenue_forecast,
                    af.eps_forecast,
                    af.recommendation,
                    af.confidence_score,
                    af.model_version
                FROM Forecasts af
                         INNER JOIN Companies c ON af.company_id = c.company_id
                         INNER JOIN Sectors s ON c.sector_id = s.sector_id
                ORDER BY af.forecast_date DESC \
                """

        return self.execute_custom_query(query)

    def get_latest_forecasts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get latest forecasts"""

        query = """
                SELECT
                    af.forecast_id,
                    af.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    s.sector_name,
                    af.forecast_date,
                    af.target_date,
                    af.target_price,
                    af.recommendation,
                    af.confidence_score
                FROM AnalystForecast af
                         INNER JOIN Company c ON af.company_id = c.company_id
                         INNER JOIN Sector s ON c.sector_id = s.sector_id
                ORDER BY af.forecast_date DESC
                    LIMIT %s \
                """

        return self.execute_custom_query(query, (limit,))

    def get_latest_by_company(self, company_id: int) -> Optional[Dict[str, Any]]:
        """Get most recent forecast for a company"""

        query = """
                SELECT *
                FROM AnalystForecast
                WHERE company_id = %s
                ORDER BY forecast_date DESC
                    LIMIT 1 \
                """

        results = self.execute_custom_query(query, (company_id,))
        return results[0] if results else None

    # ========== UPDATE ==========

    def update_by_id(self, forecast_id: int, **kwargs) -> int:
        """Update forecast by ID"""

        data = {k: v for k, v in kwargs.items() if v is not None}

        if not data:
            return 0

        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"UPDATE AnalystForecast SET {set_clause} WHERE forecast_id = %s"

        params = tuple(data.values()) + (forecast_id,)
        return self.execute_custom_update(query, params)

    # ========== DELETE ==========

    def delete_by_id(self, forecast_id: int) -> int:
        """Delete forecast by ID"""

        query = "DELETE FROM AnalystForecast WHERE forecast_id = %s"
        return self.execute_custom_update(query, (forecast_id,))

    def delete_by_company(self, company_id: int) -> int:
        """Delete all forecasts for a company"""

        query = "DELETE FROM AnalystForecast WHERE company_id = %s"
        return self.execute_custom_update(query, (company_id,))

    # ========== ANALYTICS ==========

    def get_recommendation_distribution(self) -> List[Dict[str, Any]]:
        """Get distribution of recommendations"""

        query = """
                SELECT
                    recommendation,
                    COUNT(*) as count,
                AVG(confidence_score) as avg_confidence,
                AVG(target_price) as avg_target_price
                FROM AnalystForecast
                GROUP BY recommendation
                ORDER BY count DESC \
                """

        return self.execute_custom_query(query)

    def get_forecasts_by_sector(self) -> List[Dict[str, Any]]:
        """Get forecasts grouped by sector"""

        query = """
                SELECT
                    s.sector_name,
                    COUNT(af.forecast_id) as forecast_count,
                    AVG(af.confidence_score) as avg_confidence,
                    AVG(af.target_price) as avg_target_price,
                    COUNT(CASE WHEN af.recommendation IN ('Strong Buy', 'Buy') THEN 1 END) as buy_count,
                    COUNT(CASE WHEN af.recommendation = 'Hold' THEN 1 END) as hold_count,
                    COUNT(CASE WHEN af.recommendation IN ('Sell', 'Strong Sell') THEN 1 END) as sell_count
                FROM AnalystForecast af
                         INNER JOIN Company c ON af.company_id = c.company_id
                         INNER JOIN Sector s ON c.sector_id = s.sector_id
                GROUP BY s.sector_id, s.sector_name
                ORDER BY forecast_count DESC \
                """

        return self.execute_custom_query(query)
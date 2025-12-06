"""
Price Repository
Data access layer for StockPrices table
ALL SQL queries and stored procedure calls here
"""

from typing import List, Optional, Dict, Any
from datetime import date
from repositories.BaseRepository import BaseRepository

class PriceRepository(BaseRepository):
    """Repository for stock price operations"""

    def find_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        pass

    def get_table_name(self) -> str:
        return "StockPrice"

    # ========== CREATE ==========

    def create(self, company_id: int, trading_date: date, open_price: float,
               high_price: float, low_price: float, close_price: float,
               volume: int, adjusted_close: Optional[float] = None) -> int:
        """Create new stock price record"""

        query = """
                INSERT INTO StockPrice (
                    company_id, trading_date, open_price, high_price, low_price,
                    close_price, volume, adjusted_close
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                """

        return self.execute_custom_update(query, (
            company_id, trading_date, open_price, high_price, low_price,
            close_price, volume, adjusted_close or close_price
        ))

    def create_using_procedure(self, company_id: int, trading_date: date,
                               open_price: float, high_price: float, low_price: float,
                               close_price: float, volume: int) -> bool:
        """
        Create using stored procedure with validation.
        Uses: InsertStockPrice procedure
        Includes trigger validation automatically.
        """
        try:
            self.call_stored_procedure('InsertStockPrice',
                                       (company_id, trading_date, open_price, high_price, low_price, close_price, volume))
            return True
        except Exception as e:
            print(f"Stored procedure error: {e}")
            return False

    # ========== READ ==========

    def find_by_company_and_date(self, company_id: int, trading_date: date) -> Optional[Dict[str, Any]]:
        """Find price for specific company and date"""

        query = """
                SELECT
                    price_id,
                    company_id,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    adjusted_close,
                    volume
                FROM StockPrices
                WHERE company_id = %s AND trade_date = %s \
                """

        results = self.execute_custom_query(query, (company_id, trading_date))
        return results[0] if results else None

    def find_by_company(self, company_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Get price history for a company"""

        query = """
                SELECT *
                FROM StockPrice
                WHERE company_id = %s
                ORDER BY trading_date DESC
                    LIMIT %s \
                """

        return self.execute_custom_query(query, (company_id, limit))

    def find_by_date_range(self, company_id: int, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Get prices for date range"""

        query = """
                SELECT
                    price_id,
                    company_id,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    adjusted_close,
                    volume
                FROM StockPrices
                WHERE company_id = %s
                  AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date ASC \
                """

        return self.execute_custom_query(query, (company_id, start_date, end_date))

    def get_latest_price(self, company_id: int) -> Optional[Dict[str, Any]]:
        """Get most recent price for a company"""

        query = """
                SELECT *
                FROM StockPrice
                WHERE company_id = %s
                ORDER BY trading_date DESC
                    LIMIT 1 \
                """

        results = self.execute_custom_query(query, (company_id,))
        return results[0] if results else None

    def get_latest_prices_all(self) -> List[Dict[str, Any]]:
        """Get latest prices for all companies"""

        query = """
                SELECT
                    sp.price_id,
                    sp.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    s.sector_name,
                    sp.trade_date,
                    sp.close_price,
                    sp.volume
                FROM StockPrices sp
                         INNER JOIN Companies c ON sp.company_id = c.company_id
                         INNER JOIN Sectors s ON c.sector_id = s.sector_id
                WHERE sp.trade_date = (
                    SELECT MAX(trade_date)
                    FROM StockPrices
                    WHERE company_id = sp.company_id
                )
                ORDER BY c.company_name \
                """

        return self.execute_custom_query(query)

    # ========== UPDATE ==========

    def update_by_company_and_date(self, company_id: int, trading_date: date, **kwargs) -> int:
        """Update price data for specific company and date"""

        data = {k: v for k, v in kwargs.items() if v is not None}

        if not data:
            return 0

        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"""
            UPDATE StockPrice
            SET {set_clause}
            WHERE company_id = %s AND trading_date = %s
        """

        params = tuple(data.values()) + (company_id, trading_date)
        return self.execute_custom_update(query, params)

    # ========== DELETE ==========

    def delete_by_company_and_date(self, company_id: int, trading_date: date) -> int:
        """Delete specific price record"""

        query = "DELETE FROM StockPrice WHERE company_id = %s AND trading_date = %s"
        return self.execute_custom_update(query, (company_id, trading_date))

    def delete_by_company(self, company_id: int) -> int:
        """Delete all prices for a company"""

        query = "DELETE FROM StockPrice WHERE company_id = %s"
        return self.execute_custom_update(query, (company_id,))

    # ========== ANALYTICS QUERIES ==========

    def get_price_statistics(self, company_id: int, days: int = 30) -> Optional[Dict[str, Any]]:
        """Get price statistics for last N days"""

        query = """
                SELECT
                    company_id,
                    COUNT(*) as trading_days,
                    AVG(close_price) as avg_price,
                    MAX(high_price) as highest_price,
                    MIN(low_price) as lowest_price,
                    AVG(volume) as avg_volume,
                    MAX(volume) as max_volume,
                    STDDEV(close_price) as price_std_dev
                FROM StockPrice
                WHERE company_id = %s
                  AND trading_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                GROUP BY company_id \
                """

        results = self.execute_custom_query(query, (company_id, days))
        return results[0] if results else None

    def get_daily_returns(self, company_id: int, limit: int = 30) -> List[Dict[str, Any]]:
        """Get daily returns using window function"""

        query = """
                SELECT
                    trading_date,
                    close_price,
                    LAG(close_price) OVER (ORDER BY trading_date) as prev_close,
                    ((close_price - LAG(close_price) OVER (ORDER BY trading_date)) /
                     LAG(close_price) OVER (ORDER BY trading_date) * 100) as daily_return_pct
                FROM StockPrice
                WHERE company_id = %s
                ORDER BY trading_date DESC
                    LIMIT %s \
                """

        return self.execute_custom_query(query, (company_id, limit))

    def get_moving_average(self, company_id: int, window: int = 20) -> List[Dict[str, Any]]:
        """Calculate moving average"""

        query = """
                SELECT
                    trading_date,
                    close_price,
                    AVG(close_price) OVER (
                    ORDER BY trading_date 
                    ROWS BETWEEN %s PRECEDING AND CURRENT ROW
                ) as moving_avg
                FROM StockPrice
                WHERE company_id = %s
                ORDER BY trading_date DESC
                    LIMIT 100 \
                """

        return self.execute_custom_query(query, (window - 1, company_id))
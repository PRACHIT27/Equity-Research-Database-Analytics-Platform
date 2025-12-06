"""
Company Repository
Data access layer for Companies and Sectors tables
ALL SQL queries are here - NO SQL in service layer
"""
import datetime
from typing import List, Optional, Dict, Any
from repositories.BaseRepository import BaseRepository

class CompanyRepository(BaseRepository):
    """Repository for Company operations"""

    def get_table_name(self) -> str:
        return "Company"

    # ========== CREATE ==========

    def create(self, ticker_symbol: str, company_name: str, sector_id: int,
               market_cap: Optional[float] = None,
               exchange: Optional[str] = None, incorporation_country: Optional[str] = None,
               founded_date = Optional[datetime],
               description: Optional[str] = None,
               currency : Optional[str]= None) -> int:
        """Create new company"""

        query = """
                INSERT INTO Companies (
                    ticker_symbol, company_name, sector_id, market_cap,
                    exchange, country, incorporation_date,
                    description, currency, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                """

        return self.execute_custom_update(query, (
            ticker_symbol, company_name, sector_id, market_cap,
            exchange, incorporation_country, founded_date,
            description, currency, datetime.datetime.now()
        ))

    # ========== READ ==========

    def find_by_id(self, company_id: int) -> Optional[Dict[str, Any]]:
        """Find company by ID with sector info"""

        query = """
                SELECT
                    c.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    c.sector_id,
                    s.sector_name,
                    c.market_cap,
                    c.exchange,
                    c.currency,
                    c.country,
                    c.incorporation_date,
                    c.description,
                    c.created_at,
                    c.updated_at
                FROM Companies c
                         INNER JOIN Sectors s ON c.sector_id = s.sector_id
                WHERE c.company_id = %s \
                """

        results = self.execute_custom_query(query, (company_id,))
        return results[0] if results else None

    def find_by_ticker(self, ticker_symbol: str) -> Optional[Dict[str, Any]]:
        """Find company by ticker symbol"""

        query = """
                SELECT
                    c.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    c.sector_id,
                    s.sector_name,
                    c.market_cap,
                    c.exchange,
                    c.description
                FROM Companies c
                         INNER JOIN Sectors s ON c.sector_id = s.sector_id
                WHERE c.ticker_symbol = %s \
                """

        results = self.execute_custom_query(query, (ticker_symbol,))
        return results[0] if results else None

    def find_all_with_sectors(self) -> List[Dict[str, Any]]:
        """Get all companies with sector information"""

        query = """
                SELECT
                    c.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    c.market_cap,
                    c.exchange,
                    c.country,
                    c.description,
                    c.incorporation_date,
                    c.currency,
                    s.sector_id,
                    s.sector_name,
                    c.created_at,
                    c.updated_at
                FROM Companies c
                         INNER JOIN Sectors s ON c.sector_id = s.sector_id
                ORDER BY c.company_name \
                """

        return self.execute_custom_query(query)

    def find_by_sector(self, sector_id: int) -> List[Dict[str, Any]]:
        """Find all companies in a sector"""

        query = """
                SELECT
                    company_id,
                    ticker_symbol,
                    company_name,
                    market_cap,
                FROM Companies
                WHERE sector_id = %s
                ORDER BY market_cap DESC \
                """

        return self.execute_custom_query(query, (sector_id,))

    def search(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search companies by ticker or name"""

        query = """
                SELECT
                    c.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    s.sector_name,
                    c.market_cap,
                    c.exchange
                FROM Companies c
                         INNER JOIN Sectors s ON c.sector_id = s.sector_id
                WHERE c.ticker_symbol LIKE %s
                   OR c.company_name LIKE %s
                ORDER BY c.market_cap DESC
                    LIMIT %s \
                """

        search_pattern = f"%{search_term}%"
        return self.execute_custom_query(query, (search_pattern, search_pattern, limit))

    def get_with_latest_price(self) -> List[Dict[str, Any]]:
        """Get companies with their latest stock price"""

        query = """
                SELECT
                    c.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    s.sector_name,
                    c.market_cap,
                    sp.close_price as latest_price,
                    sp.trading_date as latest_price_date,
                    sp.volume as latest_volume
                FROM Company c
                         INNER JOIN Sector s ON c.sector_id = s.sector_id
                         LEFT JOIN (
                    SELECT company_id, close_price, trading_date, volume
                    FROM StockPrice sp1
                    WHERE trading_date = (
                        SELECT MAX(trading_date)
                        FROM StockPrice sp2
                        WHERE sp2.company_id = sp1.company_id
                    )
                ) sp ON c.company_id = sp.company_id
                ORDER BY c.market_cap DESC \
                """

        return self.execute_custom_query(query)

    # ========== STORED PROCEDURE: Get Company Overview ==========

    def get_overview(self, company_id: int) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive company overview.
        Uses stored procedure: GetCompanyOverview
        """
        results = self.call_stored_procedure('GetCompanyOverview', (company_id,))
        return results[0] if results else None

    # ========== UPDATE ==========

    def update_by_id(self, company_id: int, **kwargs) -> int:
        """Update company by ID"""

        # Filter out None values
        data = {k: v for k, v in kwargs.items() if v is not None}

        if not data:
            return 0

        # Build dynamic update query
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"UPDATE Companies SET {set_clause} WHERE company_id = %s"

        params = tuple(data.values()) + (company_id,)
        return self.execute_custom_update(query, params)

    def update_sector(self, company_id: int, sector_id: int) -> int:
        """Update company's sector"""

        query = "UPDATE Company SET sector_id = %s WHERE company_id = %s"
        return self.execute_custom_update(query, (sector_id, company_id))

    # ========== DELETE ==========

    def delete_by_id(self, company_id: int) -> int:
        """Delete company by ID (direct delete)"""

        query = "DELETE FROM Companies WHERE company_id = %s"
        return self.execute_custom_update(query, (company_id,))

    def delete_with_dependencies(self, company_id: int) -> bool:
        """
        Delete company using stored procedure.
        Uses: DeleteCompanyWithDependencies
        Safely handles all foreign key dependencies.
        """
        try:
            self.call_stored_procedure('DeleteCompanyWithDependencies', (company_id,))
            return True
        except Exception:
            return False

    # ========== ANALYTICS QUERIES ==========

    def get_count_by_sector(self) -> List[Dict[str, Any]]:
        """Get company count and stats per sector"""

        query = """
                SELECT
                    s.sector_id,
                    s.sector_name,
                    COUNT(c.company_id) as company_count,
                    AVG(c.market_cap) as avg_market_cap,
                    SUM(c.market_cap) as total_market_cap,
                    MAX(c.market_cap) as max_market_cap,
                    MIN(c.market_cap) as min_market_cap
                FROM Sectors s
                         LEFT JOIN Companies c ON s.sector_id = c.sector_id
                GROUP BY s.sector_id, s.sector_name
                ORDER BY company_count DESC \
                """

        return self.execute_custom_query(query)

    def get_top_by_market_cap(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top companies by market capitalization"""

        query = """
                SELECT
                    c.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    s.sector_name,
                    c.market_cap,
                    c.employees,
                    c.exchange
                FROM Company c
                         INNER JOIN Sector s ON c.sector_id = s.sector_id
                WHERE c.market_cap IS NOT NULL
                ORDER BY c.market_cap DESC
                    LIMIT %s \
                """

        return self.execute_custom_query(query, (limit,))


class SectorRepository(BaseRepository):
    """Repository for Sector operations"""

    def get_table_name(self) -> str:
        return "Sector"

    def create(self, sector_name: str, description: Optional[str] = None,
               industry_category: Optional[str] = None, sector_index_ticker: Optional[str] = None) -> int:
        """Create new sector"""

        query = """
                INSERT INTO Sector (sector_name, description, industry_category, sector_index_ticker)
                VALUES (%s, %s, %s, %s) \
                """

        return self.execute_custom_update(query, (sector_name, description, industry_category, sector_index_ticker))

    def find_by_id(self, sector_id: int) -> Optional[Dict[str, Any]]:
        """Find sector by ID"""

        query = "SELECT * FROM Sectors WHERE sector_id = %s"
        results = self.execute_custom_query(query, (sector_id,))
        return results[0] if results else None

    def find_by_name(self, sector_name: str) -> Optional[Dict[str, Any]]:
        """Find sector by name"""

        query = "SELECT * FROM Sector WHERE sector_name = %s"
        results = self.execute_custom_query(query, (sector_name,))
        return results[0] if results else None

    def find_all(self) -> List[Dict[str, Any]]:
        """Get all sectors"""

        query = """
                SELECT
                    sector_id,
                    sector_name,
                    industry_category,
                    sector_index_ticker
                FROM Sectors
                ORDER BY sector_name \
                """

        return self.execute_custom_query(query)

    def get_all_with_counts(self) -> List[Dict[str, Any]]:
        """Get all sectors with company counts"""

        query = """
                SELECT
                    s.sector_id,
                    s.sector_name,
                    s.description,
                    COUNT(c.company_id) as company_count
                FROM Sector s
                         LEFT JOIN Company c ON s.sector_id = c.sector_id
                GROUP BY s.sector_id, s.sector_name, s.description
                ORDER BY s.sector_name \
                """

        return self.execute_custom_query(query)

    def update_by_id(self, sector_id: int, **kwargs) -> int:
        """Update sector"""

        data = {k: v for k, v in kwargs.items() if v is not None}
        if not data:
            return 0

        return self.update('sector_id', sector_id, data)

    def delete_by_id(self, sector_id: int) -> int:
        """Delete sector"""

        return self.delete('sector_id', sector_id)
"""
Financial Repository Module
Data access layer for Financial Statements (superclass/subclass) and Valuation Metrics
Handles FinancialStatement, IncomeStatement, BalanceSheet, CashFlowStatement, ValuationMetrics
ALL SQL queries here
"""

from typing import List, Optional, Dict, Any
from datetime import date
from repositories.BaseRepository import BaseRepository

class FinancialRepository(BaseRepository):
    """Repository for financial statement and valuation operations"""

    def get_table_name(self) -> str:
        return "FinancialStatement"

    # ========== CREATE FINANCIAL STATEMENT (Superclass) ==========

    def create_financial_statement(self, company_id: int, statement_type: str,
                                   fiscal_year: int, fiscal_period: str,
                                   reporting_date: date, filing_type: Optional[str] = None) -> int:
        """
        Create financial statement (superclass).
        Returns statement_id for creating subclass records.
        """
        query = """
                INSERT INTO FinancialStatement (
                    company_id, statement_type, fiscal_year, fiscal_period,
                    reporting_date, filing_type
                ) VALUES (%s, %s, %s, %s, %s, %s) \
                """

        self.execute_custom_update(query, (
            company_id, statement_type, fiscal_year, fiscal_period,
            reporting_date, filing_type
        ))

        # Get the inserted statement_id
        query_id = """
                   SELECT statement_id FROM FinancialStatement
                   WHERE company_id = %s AND statement_type = %s
                     AND fiscal_year = %s AND fiscal_period = %s
                   ORDER BY statement_id DESC LIMIT 1 \
                   """

        result = self.execute_custom_query(query_id, (company_id, statement_type, fiscal_year, fiscal_period))
        return result[0]['statement_id'] if result else None

    # ========== CREATE SUBCLASS RECORDS ==========

    def create_income_statement(self, statement_id: int, company_id: int,
                                revenue: Optional[float] = None, cost_of_revenue: Optional[float] = None,
                                gross_profit: Optional[float] = None, operating_expenses: Optional[float] = None,
                                operating_income: Optional[float] = None, net_income: Optional[float] = None,
                                ebitda: Optional[float] = None, gross_margin: Optional[float] = None,
                                operating_margin: Optional[float] = None, net_margin: Optional[float] = None) -> int:
        """Create income statement (subclass)"""

        query = """
                INSERT INTO IncomeStatement (
                    statement_id, company_id, revenue, cost_of_revenue, gross_profit,
                    operating_expenses, operating_income, net_income, ebitda,
                    gross_margin, operating_margin, net_margin
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                """

        return self.execute_custom_update(query, (
            statement_id, company_id, revenue, cost_of_revenue, gross_profit,
            operating_expenses, operating_income, net_income, ebitda,
            gross_margin, operating_margin, net_margin
        ))

    def create_balance_sheet(self, statement_id: int, company_id: int,
                             total_assets: Optional[float] = None, current_assets: Optional[float] = None,
                             total_liabilities: Optional[float] = None, current_liabilities: Optional[float] = None,
                             shareholders_equity: Optional[float] = None, cash_and_equivalents: Optional[float] = None,
                             inventory: Optional[float] = None, retained_earnings: Optional[float] = None) -> int:
        """Create balance sheet (subclass)"""

        query = """
                INSERT INTO BalanceSheet (
                    statement_id, company_id, total_assets, current_assets,
                    total_liabilities, current_liabilities, shareholders_equity,
                    cash_and_equivalents, inventory, retained_earnings
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                """

        return self.execute_custom_update(query, (
            statement_id, company_id, total_assets, current_assets,
            total_liabilities, current_liabilities, shareholders_equity,
            cash_and_equivalents, inventory, retained_earnings
        ))

    def create_cashflow_statement(self, statement_id: int, company_id: int,
                                  operating_cash_flow: Optional[float] = None,
                                  investing_cash_flow: Optional[float] = None,
                                  financing_cash_flow: Optional[float] = None,
                                  free_cash_flow: Optional[float] = None,
                                  capital_expenditure: Optional[float] = None,
                                  dividend_paid: Optional[float] = None,
                                  net_cash_flow: Optional[float] = None) -> int:
        """Create cash flow statement (subclass)"""

        query = """
                INSERT INTO CashFlowStatement (
                    statement_id, company_id, operating_cash_flow, investing_cash_flow,
                    financing_cash_flow, free_cash_flow, capital_expenditure,
                    dividend_paid, net_cash_flow
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
                """

        return self.execute_custom_update(query, (
            statement_id, company_id, operating_cash_flow, investing_cash_flow,
            financing_cash_flow, free_cash_flow, capital_expenditure,
            dividend_paid, net_cash_flow
        ))

    # ========== READ FINANCIAL STATEMENTS ==========

    def find_all(self) -> List[Dict[str, Any]]:
        """Get all financial statements with company info"""

        query = """
                SELECT
                    fs.statement_id,
                    fs.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    fs.statement_type,
                    fs.fiscal_year,
                    fs.fiscal_period,
                    fs.reporting_date,
                    fs.filing_type
                FROM FinancialStatement fs
                         INNER JOIN Company c ON fs.company_id = c.company_id
                ORDER BY fs.reporting_date DESC \
                """

        return self.execute_custom_query(query)

    def find_by_id(self, statement_id: int) -> Optional[Dict[str, Any]]:
        """Find financial statement by ID"""

        query = """
                SELECT
                    fs.*,
                    c.ticker_symbol,
                    c.company_name
                FROM FinancialStatement fs
                         INNER JOIN Company c ON fs.company_id = c.company_id
                WHERE fs.statement_id = %s \
                """

        results = self.execute_custom_query(query, (statement_id,))
        return results[0] if results else None

    def find_by_company(self, company_id: int) -> List[Dict[str, Any]]:
        """Get all statements for a company"""

        query = """
                SELECT
                    fs.statement_id,
                    fs.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    fs.statement_type,
                    fs.fiscal_year,
                    fs.fiscal_quarter,
                    fs.filing_date
                FROM FinancialStatements fs
                         INNER JOIN Companies c ON fs.company_id = c.company_id
                WHERE fs.company_id = %s
                ORDER BY fs.filing_date DESC \
                """

        return self.execute_custom_query(query, (company_id,))

    def get_income_statements(self, company_id: int) -> List[Dict[str, Any]]:
        """Get income statements for a company"""

        query = """
                SELECT
                    fs.statement_id,
                    fs.fiscal_year,
                    fs.fiscal_quarter,
                    fs.filing_date,
                    i.revenue,
                    i.net_income,
                    i.gross_profit,
                    i.operating_income,
                    i.eps_diluted
                FROM FinancialStatements fs
                         INNER JOIN IncomeStatements i ON fs.statement_id = i.statement_id
                WHERE fs.company_id = %s
                ORDER BY fs.filing_date DESC \
                """

        return self.execute_custom_query(query, (company_id,))

    def get_balance_sheets(self, company_id: int) -> List[Dict[str, Any]]:
        """Get balance sheets for a company"""

        query = """
                SELECT
                    fs.statement_id,
                    fs.fiscal_year,
                    fs.fiscal_period,
                    fs.reporting_date,
                    b.balance_id,
                    b.total_assets,
                    b.total_liabilities,
                    b.shareholders_equity,
                    b.working_capital
                FROM FinancialStatement fs
                         INNER JOIN BalanceSheet b ON fs.statement_id = b.statement_id
                WHERE fs.company_id = %s
                ORDER BY fs.reporting_date DESC \
                """

        return self.execute_custom_query(query, (company_id,))

    def get_cashflow_statements(self, company_id: int) -> List[Dict[str, Any]]:
        """Get cash flow statements for a company"""

        query = """
                SELECT
                    fs.statement_id,
                    fs.fiscal_year,
                    fs.fiscal_period,
                    fs.reporting_date,
                    cf.cashflow_id,
                    cf.operating_cash_flow,
                    cf.free_cash_flow,
                    cf.capital_expenditure
                FROM FinancialStatement fs
                         INNER JOIN CashFlowStatement cf ON fs.statement_id = cf.statement_id
                WHERE fs.company_id = %s
                ORDER BY fs.reporting_date DESC \
                """

        return self.execute_custom_query(query, (company_id,))

    # ========== UPDATE ==========

    def update_income_statement(self, statement_id: int, **kwargs) -> int:
        """Update income statement"""

        data = {k: v for k, v in kwargs.items() if v is not None}

        if not data:
            return 0

        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"UPDATE IncomeStatement SET {set_clause} WHERE statement_id = %s"

        params = tuple(data.values()) + (statement_id,)
        return self.execute_custom_update(query, params)

    def update_balance_sheet(self, statement_id: int, **kwargs) -> int:
        """Update balance sheet"""

        data = {k: v for k, v in kwargs.items() if v is not None}

        if not data:
            return 0

        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"UPDATE BalanceSheet SET {set_clause} WHERE statement_id = %s"

        params = tuple(data.values()) + (statement_id,)
        return self.execute_custom_update(query, params)

    # ========== DELETE ==========

    def delete_by_id(self, statement_id: int) -> int:
        """Delete financial statement (cascade deletes subclass)"""

        query = "DELETE FROM FinancialStatement WHERE statement_id = %s"
        return self.execute_custom_update(query, (statement_id,))

    # ========== VALUATION METRICS OPERATIONS ==========

    def create_valuation_metrics_manual(self, company_id: int, calculation_date: date,
                                        pe_ratio=None, pb_ratio=None, ps_ratio=None,
                                        ev_ebitda=None, roe=None, roa=None,
                                        debt_to_equity=None, current_ratio=None,
                                        quick_ratio=None, gross_margin=None,
                                        operating_margin=None, net_margin=None) -> int:
        """Insert valuation metrics manually"""

        query = """
                INSERT INTO ValuationMetrics (
                    company_id, calculation_date, pe_ratio, pb_ratio, ps_ratio,
                    ev_ebitda, roe, roa, debt_to_equity, current_ratio,
                    quick_ratio, gross_margin, operating_margin, net_margin
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                                         pe_ratio = VALUES(pe_ratio),
                                         pb_ratio = VALUES(pb_ratio),
                                         ps_ratio = VALUES(ps_ratio),
                                         ev_ebitda = VALUES(ev_ebitda),
                                         roe = VALUES(roe),
                                         roa = VALUES(roa),
                                         debt_to_equity = VALUES(debt_to_equity),
                                         current_ratio = VALUES(current_ratio),
                                         quick_ratio = VALUES(quick_ratio),
                                         gross_margin = VALUES(gross_margin),
                                         operating_margin = VALUES(operating_margin),
                                         net_margin = VALUES(net_margin) \
                """

        return self.execute_custom_update(query, (
            company_id, calculation_date, pe_ratio, pb_ratio, ps_ratio,
            ev_ebitda, roe, roa, debt_to_equity, current_ratio,
            quick_ratio, gross_margin, operating_margin, net_margin
        ))

    def get_all_valuation_metrics(self) -> List[Dict[str, Any]]:
        """Get all valuation metrics with company info"""

        query = """
                SELECT
                    vm.metric_id,
                    vm.company_id,
                    c.ticker_symbol,
                    c.company_name,
                    s.sector_name,
                    vm.calculation_date,
                    vm.pe_ratio,
                    vm.pb_ratio,
                    vm.ps_ratio,
                    vm.roe,
                    vm.roa,
                    vm.debt_to_equity,
                    vm.current_ratio,
                    vm.quick_ratio,
                    vm.gross_margin,
                    vm.operating_margin,
                    vm.net_margin
                FROM ValuationMetrics vm
                         INNER JOIN Companies c ON vm.company_id = c.company_id
                         INNER JOIN Sectors s ON c.sector_id = s.sector_id
                ORDER BY vm.calculation_date DESC \
                """

        return self.execute_custom_query(query)

    def get_valuation_metrics_by_company(self, company_id: int) -> List[Dict[str, Any]]:
        """Get all metrics for a company"""

        query = """
                SELECT *
                FROM ValuationMetrics
                WHERE company_id = %s
                ORDER BY calculation_date DESC \
                """

        return self.execute_custom_query(query, (company_id,))

    def get_latest_valuation_metrics(self, company_id: int) -> Optional[Dict[str, Any]]:
        """Get latest metrics for a company"""

        query = """
                SELECT *
                FROM ValuationMetrics
                WHERE company_id = %s
                ORDER BY calculation_date DESC
                    LIMIT 1 \
                """

        results = self.execute_custom_query(query, (company_id,))
        return results[0] if results else None

    def find_valuation_metric_by_id(self, metric_id: int) -> Optional[Dict[str, Any]]:
        """Find valuation metric by ID"""

        query = """
                SELECT
                    vm.*,
                    c.ticker_symbol,
                    c.company_name
                FROM ValuationMetrics vm
                         INNER JOIN Company c ON vm.company_id = c.company_id
                WHERE vm.metric_id = %s \
                """

        results = self.execute_custom_query(query, (metric_id,))
        return results[0] if results else None

    def delete_valuation_metric(self, metric_id: int) -> int:
        """Delete valuation metric by ID"""

        query = "DELETE FROM ValuationMetrics WHERE metric_id = %s"
        return self.execute_custom_update(query, (metric_id,))

    def compare_valuation_metrics(self, company_ids: List[int]) -> List[Dict[str, Any]]:
        """Compare metrics across companies"""

        placeholders = ','.join(['%s'] * len(company_ids))

        query = f"""
            SELECT 
                c.ticker_symbol,
                c.company_name,
                s.sector_name,
                vm.calculation_date,
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

        return self.execute_custom_query(query, tuple(company_ids))

    def get_sector_valuation_averages(self) -> List[Dict[str, Any]]:
        """Get average valuation metrics by sector"""

        query = """
                SELECT
                    s.sector_name,
                    COUNT(DISTINCT c.company_id) as company_count,
                    AVG(vm.pe_ratio) as avg_pe_ratio,
                    AVG(vm.pb_ratio) as avg_pb_ratio,
                    AVG(vm.ps_ratio) as avg_ps_ratio,
                    AVG(vm.roe) as avg_roe,
                    AVG(vm.roa) as avg_roa,
                    AVG(vm.debt_to_equity) as avg_debt_to_equity,
                    AVG(vm.current_ratio) as avg_current_ratio
                FROM Sectors s
                         LEFT JOIN Companies c ON s.sector_id = c.sector_id
                         LEFT JOIN ValuationMetrics vm ON c.company_id = vm.company_id
                WHERE vm.calculation_date = (
                    SELECT MAX(calculation_date)
                    FROM ValuationMetrics
                    WHERE company_id = vm.company_id
                )
                GROUP BY s.sector_id, s.sector_name
                ORDER BY s.sector_name \
                """

        return self.execute_custom_query(query)
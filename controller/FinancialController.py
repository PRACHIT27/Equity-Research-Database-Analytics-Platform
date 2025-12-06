"""
Financial Controller
Handles financial statements and valuation metrics
FIXED: Now properly uses Service layer instead of direct repository access
"""

from services.FinancialService import FinancialService
from services.ValuationService import ValuationService
from core.DatabaseConnection import get_db_connection
from repositories.FinancialRepository import FinancialRepository
from repositories.CompanyRepository import CompanyRepository

class FinancialController:
    """
    Controller for financial statements and metrics.
    Delegates all business logic to FinancialService and ValuationService.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FinancialController, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Initialize repositories
        db = get_db_connection()
        financial_repo = FinancialRepository(db)
        company_repo = CompanyRepository(db)

        # Initialize services
        self._financial_service = FinancialService(financial_repo, company_repo)
        self._valuation_service = ValuationService(financial_repo, company_repo)
        self._initialized = True

    # ========== FINANCIAL STATEMENTS ==========

    def get_all_statements(self):
        """Get all financial statements through repository (read-only)"""
        try:
            return self._financial_service.financial_repo.find_all()
        except Exception as e:
            return []

    def get_sector_valuation_averages(self):
        """Get all financial statements through repository (read-only)"""
        try:
            return self._valuation_service.get_sector_valuation_averages()
        except Exception as e:
            return []

    def get_statements_by_company(self, company_id):
        """Get all statements for a company through repository (read-only)"""
        try:
            return self._financial_service.financial_repo.find_by_company(company_id)
        except Exception as e:
            return []

    def get_income_statements_by_company(self, company_id):
        """Get all statements for a company through repository (read-only)"""
        try:
            return self._financial_service.financial_repo.get_income_statements(company_id)
        except Exception as e:
            return []

    def get_statement_by_id(self, statement_id):
        """Get statement by ID through repository (read-only)"""
        try:
            return self._financial_service.financial_repo.find_by_id(statement_id)
        except Exception as e:
            return None

    def create_income_statement(self, company_id, fiscal_year, fiscal_period,
                                reporting_date, revenue=None, cost_of_revenue=None,
                                gross_profit=None, operating_expenses=None,
                                operating_income=None, net_income=None, ebitda=None):
        """Create income statement through service"""
        try:
            result = self._financial_service.create_income_statement(
                company_id=company_id,
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                reporting_date=reporting_date,
                revenue=revenue,
                cost_of_revenue=cost_of_revenue,
                gross_profit=gross_profit,
                operating_expenses=operating_expenses,
                operating_income=operating_income,
                net_income=net_income,
                ebitda=ebitda
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def create_balance_sheet(self, company_id, fiscal_year, fiscal_period,
                             reporting_date, total_assets=None, current_assets=None,
                             total_liabilities=None, current_liabilities=None,
                             shareholders_equity=None, cash_and_equivalents=None,
                             inventory=None, retained_earnings=None):
        """Create balance sheet through service"""
        try:
            result = self._financial_service.create_balance_sheet(
                company_id=company_id,
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                reporting_date=reporting_date,
                total_assets=total_assets,
                current_assets=current_assets,
                total_liabilities=total_liabilities,
                current_liabilities=current_liabilities,
                shareholders_equity=shareholders_equity,
                cash_and_equivalents=cash_and_equivalents,
                inventory=inventory,
                retained_earnings=retained_earnings
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def create_cashflow_statement(self, company_id, fiscal_year, fiscal_period,
                                  reporting_date, operating_cash_flow=None,
                                  investing_cash_flow=None, financing_cash_flow=None,
                                  free_cash_flow=None, capital_expenditure=None,
                                  dividend_paid=None, net_cash_flow=None):
        """Create cash flow statement through service"""
        try:
            result = self._financial_service.create_cashflow_statement(
                company_id=company_id,
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                reporting_date=reporting_date,
                operating_cash_flow=operating_cash_flow,
                investing_cash_flow=investing_cash_flow,
                financing_cash_flow=financing_cash_flow,
                free_cash_flow=free_cash_flow,
                capital_expenditure=capital_expenditure,
                dividend_paid=dividend_paid,
                net_cash_flow=net_cash_flow
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def delete_statement(self, statement_id):
        """Delete financial statement through service"""
        try:
            result = self._financial_service.delete_statement(statement_id)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    # ========== VALUATION METRICS ==========

    def get_all_valuation_metrics(self):
        """Get all valuation metrics through service"""
        try:
            return self._valuation_service.get_all_valuation_metrics()
        except Exception as e:
            return []

    def calculate_valuation_metrics(self, company_id, calculation_date):
        """Calculate metrics using stored procedure through service"""
        try:
            result = self._valuation_service.calculate_valuation_metrics(
                company_id=company_id,
                calculation_date=calculation_date
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def add_valuation_metrics_manual(self, company_id, calculation_date, pe_ratio=None,
                                     pb_ratio=None, ps_ratio=None, ev_ebitda=None,
                                     roe=None, roa=None, debt_to_equity=None,
                                     current_ratio=None, quick_ratio=None,
                                     gross_margin=None, operating_margin=None,
                                     net_margin=None):
        """Manually add valuation metrics through service"""
        try:
            result = self._valuation_service.add_valuation_metrics_manual(
                company_id=company_id,
                calculation_date=calculation_date,
                pe_ratio=pe_ratio,
                pb_ratio=pb_ratio,
                ps_ratio=ps_ratio,
                ev_ebitda=ev_ebitda,
                roe=roe,
                roa=roa,
                debt_to_equity=debt_to_equity,
                current_ratio=current_ratio,
                quick_ratio=quick_ratio,
                gross_margin=gross_margin,
                operating_margin=operating_margin,
                net_margin=net_margin
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def delete_valuation_metrics(self, metric_id):
        """Delete valuation metrics through service"""
        try:
            result = self._valuation_service.delete_valuation_metrics(metric_id)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def get_valuation_metrics_by_company(self, company_id):
        """Get valuation metrics for a company through service"""
        try:
            return self._valuation_service.get_valuation_metrics_by_company(company_id)
        except Exception as e:
            return []

    def compare_valuations(self, company_ids):
        """Compare valuation metrics across companies through service"""
        try:
            return self._valuation_service.compare_valuations(company_ids)
        except Exception as e:
            return []


def get_financial_controller():
    """Get singleton instance"""
    return FinancialController()
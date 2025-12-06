"""
Valuation Service Layer
Business logic for valuation metrics and financial analysis
NO SQL QUERIES - All database access through repositories
"""

from typing import List, Optional, Dict, Any
from datetime import date
from repositories.FinancialRepository import FinancialRepository
from repositories.CompanyRepository import CompanyRepository
from utils.exceptions import ValidationError, BusinessLogicError

class ValuationService:
    """
    Service for valuation metrics operations.
    Handles P/E ratio, P/B ratio, ROE, ROA, and other financial ratios.
    """

    def __init__(self, financial_repo: FinancialRepository, company_repo: CompanyRepository):
        """
        Initialize valuation service.

        Args:
            financial_repo: FinancialRepository instance
            company_repo: CompanyRepository instance
        """
        self.financial_repo = financial_repo
        self.company_repo = company_repo

    # ========== AUTOMATIC CALCULATION (Using Stored Procedure) ==========

    def calculate_valuation_metrics(self, company_id: int, calculation_date: date) -> Dict[str, Any]:
        """
        Calculate valuation metrics automatically using stored procedure.
        Uses data from IncomeStatement and BalanceSheet.

        Args:
            company_id: Company ID
            calculation_date: Date for calculation

        Returns:
            dict: Success message

        Raises:
            BusinessLogicError: If company not found or calculation fails
        """
        # Validate company exists (call repository - NO SQL!)
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        # Check if company has financial statements (call repository - NO SQL!)
        income_statements = self.financial_repo.get_income_statements(company_id)
        balance_sheets = self.financial_repo.get_balance_sheets(company_id)

        if not income_statements:
            raise BusinessLogicError(
                f"Cannot calculate metrics: {company['ticker_symbol']} has no income statements. "
                "Please add financial statements first."
            )

        if not balance_sheets:
            raise BusinessLogicError(
                f"Cannot calculate metrics: {company['ticker_symbol']} has no balance sheets. "
                "Please add financial statements first."
            )

        # Call stored procedure through repository (NO SQL!)
        try:
            self.company_repo.call_stored_procedure('CalculateValuationMetrics', (company_id, calculation_date))
        except Exception as e:
            raise BusinessLogicError(f"Metric calculation failed: {e}")

        return {
            'success': True,
            'message': f"Valuation metrics calculated for {company['ticker_symbol']}",
            'company_ticker': company['ticker_symbol'],
            'calculation_date': calculation_date
        }

    # ========== MANUAL ENTRY ==========

    def add_valuation_metrics_manual(self, company_id: int, calculation_date: date,
                                     pe_ratio: Optional[float] = None,
                                     pb_ratio: Optional[float] = None,
                                     ps_ratio: Optional[float] = None,
                                     ev_ebitda: Optional[float] = None,
                                     roe: Optional[float] = None,
                                     roa: Optional[float] = None,
                                     debt_to_equity: Optional[float] = None,
                                     current_ratio: Optional[float] = None,
                                     quick_ratio: Optional[float] = None,
                                     gross_margin: Optional[float] = None,
                                     operating_margin: Optional[float] = None,
                                     net_margin: Optional[float] = None) -> Dict[str, Any]:
        """
        Manually add valuation metrics.

        Args:
            company_id: Company ID
            calculation_date: Date of calculation
            pe_ratio: Price-to-Earnings ratio
            pb_ratio: Price-to-Book ratio
            ps_ratio: Price-to-Sales ratio
            ev_ebitda: Enterprise Value to EBITDA
            roe: Return on Equity
            roa: Return on Assets
            debt_to_equity: Debt-to-Equity ratio
            current_ratio: Current ratio
            quick_ratio: Quick ratio
            gross_margin: Gross margin
            operating_margin: Operating margin
            net_margin: Net margin

        Returns:
            dict: Success message
        """
        # Validate company exists (call repository - NO SQL!)
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        # Validate ratios are reasonable (business logic - OK here!)
        if pe_ratio is not None and (pe_ratio < 0 or pe_ratio > 1000):
            raise ValidationError("P/E ratio seems unrealistic (must be 0-1000)")

        if roe is not None and (roe < -1 or roe > 2):
            raise ValidationError("ROE must be between -1 and 2 (-100% to 200%)")

        if roa is not None and (roa < -1 or roa > 1):
            raise ValidationError("ROA must be between -1 and 1 (-100% to 100%)")

        if debt_to_equity is not None and debt_to_equity < 0:
            raise ValidationError("Debt-to-Equity ratio cannot be negative")

        # Insert metrics through repository (NO SQL!)
        rows = self.financial_repo.create_valuation_metrics_manual(
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

        return {
            'success': True,
            'message': f"Valuation metrics added for {company['ticker_symbol']}"
        }

    # ========== RETRIEVE METRICS ==========

    def get_all_valuation_metrics(self) -> List[Dict[str, Any]]:
        """Get all valuation metrics (call repository - NO SQL!)"""
        return self.financial_repo.get_all_valuation_metrics()

    def get_valuation_metrics_by_company(self, company_id: int) -> List[Dict[str, Any]]:
        """
        Get all valuation metrics for a specific company.

        Args:
            company_id: Company ID

        Returns:
            list: Historical valuation metrics
        """
        # Validate company (call repository - NO SQL!)
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        # Get metrics through repository (NO SQL!)
        return self.financial_repo.get_valuation_metrics_by_company(company_id)

    def get_latest_valuation_metrics(self, company_id: int) -> Optional[Dict[str, Any]]:
        """
        Get most recent valuation metrics for a company.

        Args:
            company_id: Company ID

        Returns:
            dict: Latest valuation metrics or None
        """
        # Call repository method (NO SQL!)
        return self.financial_repo.get_latest_valuation_metrics(company_id)

    # ========== COMPARATIVE ANALYSIS ==========

    def compare_valuations(self, company_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Compare valuation metrics across multiple companies.

        Args:
            company_ids: List of company IDs to compare

        Returns:
            list: Valuation comparison data
        """
        # Validate input (business logic - OK here!)
        if not company_ids:
            raise ValidationError("At least one company required for comparison")

        if len(company_ids) > 20:
            raise ValidationError("Cannot compare more than 20 companies at once")

        # Call repository method (NO SQL!)
        return self.financial_repo.compare_valuation_metrics(company_ids)

    def get_sector_valuation_averages(self) -> List[Dict[str, Any]]:
        """
        Get average valuation metrics by sector.

        Returns:
            list: Average metrics per sector
        """
        # Call repository method (NO SQL!)
        return self.financial_repo.get_sector_valuation_averages()

    # ========== DELETE OPERATIONS ==========

    def delete_valuation_metrics(self, metric_id: int) -> Dict[str, Any]:
        """
        Delete valuation metrics record.

        Args:
            metric_id: Metric ID

        Returns:
            dict: Success message
        """
        # Check if exists (call repository - NO SQL!)
        existing = self.financial_repo.find_valuation_metric_by_id(metric_id)

        if not existing:
            raise BusinessLogicError("Valuation metric not found")

        # Delete through repository (NO SQL!)
        rows = self.financial_repo.delete_valuation_metric(metric_id)

        if rows == 0:
            raise BusinessLogicError("Failed to delete metric")

        return {
            'success': True,
            'message': "Valuation metric deleted successfully"
        }

    # ========== ANALYSIS UTILITIES ==========

    def get_valuation_category(self, pe_ratio: Optional[float]) -> str:
        """
        Categorize company valuation based on P/E ratio.
        Pure business logic - no database access needed.

        Args:
            pe_ratio: Price-to-Earnings ratio

        Returns:
            str: Valuation category
        """
        if pe_ratio is None:
            return "Unknown"

        if pe_ratio < 0:
            return "Negative Earnings"
        elif pe_ratio < 15:
            return "Undervalued"
        elif pe_ratio < 25:
            return "Fairly Valued"
        elif pe_ratio < 40:
            return "Overvalued"
        else:
            return "Highly Overvalued"

    def calculate_derived_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate derived metrics from base metrics.
        Pure calculation - no database access.

        Args:
            metrics: Dictionary of base valuation metrics

        Returns:
            dict: Enhanced metrics with derived values
        """
        enhanced = metrics.copy()

        # Add valuation category
        if 'pe_ratio' in metrics:
            enhanced['valuation_category'] = self.get_valuation_category(metrics['pe_ratio'])

        # Add health score (simple calculation)
        if metrics.get('current_ratio'):
            if metrics['current_ratio'] >= 2:
                enhanced['liquidity_health'] = 'Strong'
            elif metrics['current_ratio'] >= 1:
                enhanced['liquidity_health'] = 'Adequate'
            else:
                enhanced['liquidity_health'] = 'Weak'

        return enhanced
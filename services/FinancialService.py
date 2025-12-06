"""
Financial Service Layer
Business logic for financial statements and valuation metrics
"""

from typing import Dict, Any
from datetime import date
from repositories.FinancialRepository import FinancialRepository
from repositories.CompanyRepository import CompanyRepository
from utils.exceptions import ValidationError, BusinessLogicError

class FinancialService:
    """Service for financial statement business logic"""

    def __init__(self, financial_repo: FinancialRepository, company_repo: CompanyRepository):
        self.financial_repo = financial_repo
        self.company_repo = company_repo

    def create_income_statement(self, company_id: int, fiscal_year: int,
                                fiscal_period: str, reporting_date: date,
                                **kwargs) -> Dict[str, Any]:
        """Create income statement with validation"""

        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        if fiscal_year < 2000 or fiscal_year > 2030:
            raise ValidationError("Invalid fiscal year")

        statement_id = self.financial_repo.create_financial_statement(
            company_id, 'Income', fiscal_year, fiscal_period, reporting_date
        )

        self.financial_repo.create_income_statement(statement_id, company_id, **kwargs)

        return {'success': True, 'statement_id': statement_id}

    def create_balance_sheet(self, company_id: int, fiscal_year: int,
                             fiscal_period: str, reporting_date: date,
                             **kwargs) -> Dict[str, Any]:
        """Create balance sheet"""

        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        statement_id = self.financial_repo.create_financial_statement(
            company_id, 'Balance', fiscal_year, fiscal_period, reporting_date
        )

        self.financial_repo.create_balance_sheet(statement_id, company_id, **kwargs)

        return {'success': True, 'statement_id': statement_id}

    def create_cashflow_statement(self, company_id: int, fiscal_year: int,
                                  fiscal_period: str, reporting_date: date,
                                  **kwargs) -> Dict[str, Any]:
        """Create cash flow statement"""

        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        statement_id = self.financial_repo.create_financial_statement(
            company_id, 'CashFlow', fiscal_year, fiscal_period, reporting_date
        )

        self.financial_repo.create_cashflow_statement(statement_id, company_id, **kwargs)

        return {'success': True, 'statement_id': statement_id}

    def delete_statement(self, statement_id: int) -> Dict[str, Any]:
        """Delete financial statement"""
        existing = self.financial_repo.find_by_id(statement_id)
        if not existing:
            raise BusinessLogicError("Statement not found")

        self.financial_repo.delete_by_id(statement_id)
        return {'success': True, 'message': "Statement deleted"}
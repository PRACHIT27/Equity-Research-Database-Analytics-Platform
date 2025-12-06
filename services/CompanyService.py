"""
Company Service Layer
Business logic for company and sector operations
"""

from typing import List, Optional, Dict, Any
from repositories.CompanyRepository import CompanyRepository, SectorRepository
from utils.validators import CompanyValidator
from utils.exceptions import ValidationError, BusinessLogicError

class CompanyService:
    """
    Service layer for company business logic.
    Handles validation, business rules, and orchestrates repository calls.
    """

    def __init__(self, company_repo: CompanyRepository, sector_repo: SectorRepository):
        """
        Initialize service with repositories.

        Args:
            company_repo: CompanyRepository instance
            sector_repo: SectorRepository instance
        """
        self.company_repo = company_repo
        self.sector_repo = sector_repo
        self.validator = CompanyValidator()


    def create_company(self, ticker_symbol: str, company_name: str, sector_id: int, currency: str,
                       market_cap: Optional[float] = None,
                       exchange: Optional[str] = None, incorporation_country: Optional[str] = None,
                       founded_date = None,
                       description: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new company with validation.

        Args:
            ticker_symbol: Stock ticker
            company_name: Company name
            sector_id: Sector ID
            market_cap: Market capitalization (millions)
            exchange: Stock exchange
            incorporation_country: Country of incorporation
            founded_date: Date founded
            description: Company description

        Returns:
            dict: Success message and created company data

        Raises:
            ValidationError: If validation fails
            BusinessLogicError: If business rules violated
        """
        # Validate inputs
        self.validator.validate_ticker(ticker_symbol)
        self.validator.validate_company_name(company_name)

        if market_cap is not None:
            self.validator.validate_market_cap(market_cap)

        # Check if ticker already exists
        existing = self.company_repo.find_by_ticker(ticker_symbol)
        if existing:
            raise BusinessLogicError(f"Company with ticker '{ticker_symbol}' already exists")

        # Check if sector exists
        sector = self.sector_repo.find_by_id(sector_id)
        if not sector:
            raise BusinessLogicError(f"Sector with ID {sector_id} does not exist")

        # Create company
        rows_affected = self.company_repo.create(
            ticker_symbol=ticker_symbol,
            company_name=company_name,
            sector_id=sector_id,
            market_cap=market_cap,
            exchange=exchange,
            incorporation_country=incorporation_country,
            founded_date=founded_date,
            description=description,
            currency=currency
        )

        if rows_affected == 0:
            raise BusinessLogicError("Failed to create company")

        # Retrieve created company
        created_company = self.company_repo.find_by_ticker(ticker_symbol)

        return {
            'success': True,
            'message': f"Company '{company_name}' created successfully",
            'data': created_company
        }

    # ========== READ OPERATIONS ==========

    def get_all_companies(self) -> List[Dict[str, Any]]:
        """Get all companies with sector information"""
        return self.company_repo.find_all_with_sectors()

    def get_company_by_id(self, company_id: int) -> Optional[Dict[str, Any]]:
        """Get company by ID"""
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError(f"Company with ID {company_id} not found")
        return company

    def get_company_by_ticker(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get company by ticker"""
        self.validator.validate_ticker(ticker)
        company = self.company_repo.find_by_ticker(ticker)
        if not company:
            raise BusinessLogicError(f"Company '{ticker}' not found")
        return company

    def search_companies(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search companies"""
        if not search_term or len(search_term) < 1:
            raise ValidationError("Search term must be at least 1 character")
        return self.company_repo.search(search_term, limit)

    # ========== UPDATE OPERATIONS ==========

    def update_company(self, company_id: int, company_name: Optional[str] = None,
                       market_cap: Optional[float] = None,
                       headquarters: Optional[str] = None, description: Optional[str] = None) -> Dict[str, Any]:
        """Update company with validation"""

        # Check if company exists
        existing = self.company_repo.find_by_id(company_id)
        if not existing:
            raise BusinessLogicError(f"Company with ID {company_id} not found")

        # Validate inputs if provided
        if company_name is not None:
            self.validator.validate_company_name(company_name)

        if market_cap is not None:
            self.validator.validate_market_cap(market_cap)


        # Update company
        rows_affected = self.company_repo.update_by_id(
            company_id=company_id,
            company_name=company_name,
            market_cap=market_cap,
            country=headquarters,
            description=description
        )

        if rows_affected == 0:
            raise BusinessLogicError("No changes made or company not found")

        # Get updated company
        updated_company = self.company_repo.find_by_id(company_id)

        return {
            'success': True,
            'message': f"Company updated successfully",
            'data': updated_company
        }

    # ========== DELETE OPERATIONS ==========

    def delete_company(self, company_id: int, confirm: bool = False) -> Dict[str, Any]:
        """Delete company with confirmation"""

        if not confirm:
            raise ValidationError("Deletion must be confirmed")

        # Check if company exists
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError(f"Company with ID {company_id} not found")

        # Delete using stored procedure (handles dependencies)
        success = self.company_repo.delete_with_dependencies(company_id)

        print(success)

        if not success:
            raise BusinessLogicError(f"Failed to delete company")

        return {
            'success': True,
            'message': f"Company '{company['ticker_symbol']}' and all related data deleted"
        }


    # ========== SECTOR OPERATIONS ==========

    def get_all_sectors(self) -> List[Dict[str, Any]]:
        """Get all sectors"""
        return self.sector_repo.find_all()

    def get_sector_by_id(self, sector_id: int) -> Optional[Dict[str, Any]]:
        """Get sector by ID"""
        sector = self.sector_repo.find_by_id(sector_id)
        if not sector:
            raise BusinessLogicError(f"Sector with ID {sector_id} not found")
        return sector

    def get_companies_by_sector(self, sector_id: int) -> List[Dict[str, Any]]:
        """Get companies in a sector"""
        sector = self.sector_repo.find_by_id(sector_id)
        if not sector:
            raise BusinessLogicError(f"Sector with ID {sector_id} not found")
        return self.company_repo.find_by_sector(sector_id)


    # ========== ANALYTICS ==========


    def get_sector_statistics(self) -> List[Dict[str, Any]]:
        """Get sector statistics with aggregations"""
        return self.company_repo.get_count_by_sector()

    def get_top_companies_by_market_cap(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top companies by market cap"""
        if limit < 1 or limit > 100:
            raise ValidationError("Limit must be between 1 and 100")
        return self.company_repo.get_top_by_market_cap(limit)

    def get_top_performers(self, days: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top performing stocks using stored procedure"""
        if days < 1 or days > 365:
            raise ValidationError("Days must be between 1 and 365")
        if limit < 1 or limit > 100:
            raise ValidationError("Limit must be between 1 and 100")

        return self.company_repo.call_stored_procedure('GetTopPerformers', (days, limit))

    def get_company_overview(self, company_id: int) -> Optional[Dict[str, Any]]:
        """Get comprehensive company overview using stored procedure"""
        company = self.company_repo.find_by_id(company_id)
        if not company:
            raise BusinessLogicError("Company not found")

        results = self.company_repo.call_stored_procedure('GetCompanyOverview', (company_id,))
        return results[0] if results else None

    def execute_custom_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute custom SELECT query (admin only, read-only)"""
        if not query.strip().upper().startswith('SELECT'):
            raise ValidationError("Only SELECT queries are allowed for security")

        return self.company_repo.execute_custom_query(query)
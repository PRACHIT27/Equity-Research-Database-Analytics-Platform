"""
ETL Pipeline for Equity Research Database
Fetches financial data from Yahoo Finance and Alpha Vantage APIs
"""

import yfinance as yf
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import time
from decimal import Decimal
import requests
import json

class EquityETLPipeline:
    """ETL Pipeline to fetch and load financial data into MySQL database"""
    
    def __init__(self, db_config):
        """
        Initialize ETL Pipeline
        
        Args:
            db_config: Dictionary with MySQL connection parameters
                {host, user, password, database}
        """
        self.db_config = db_config
        self.connection = None
        
        # Target companies for data collection
        self.target_companies = [
            {'ticker': 'AAPL', 'name': 'Apple Inc.', 'sector': 'Technology'},
            {'ticker': 'MSFT', 'name': 'Microsoft Corporation', 'sector': 'Technology'},
            {'ticker': 'JPM', 'name': 'JPMorgan Chase & Co.', 'sector': 'Financial Services'},
            {'ticker': 'JNJ', 'name': 'Johnson & Johnson', 'sector': 'Healthcare'},
            {'ticker': 'TSLA', 'name': 'Tesla Inc.', 'sector': 'Consumer Discretionary'},
            {'ticker': 'XOM', 'name': 'Exxon Mobil Corporation', 'sector': 'Energy'}
        ]
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            print("✓ Database connection established")
            return True
        except Error as e:
            print(f"✗ Error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("✓ Database connection closed")
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute SQL query with error handling"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            
            if fetch:
                result = cursor.fetchall()
                cursor.close()
                return result
            else:
                self.connection.commit()
                last_id = cursor.lastrowid
                cursor.close()
                return last_id
        except Error as e:
            print(f"✗ Query error: {e}")
            self.connection.rollback()
            return None
    
    def get_or_create_sector(self, sector_name):
        """Get sector_id or create new sector"""
        query = "SELECT sector_id FROM Sectors WHERE sector_name = %s"
        result = self.execute_query(query, (sector_name,), fetch=True)
        
        if result:
            return result[0]['sector_id']
        else:
            query = "INSERT INTO Sectors (sector_name) VALUES (%s)"
            return self.execute_query(query, (sector_name,))
    
    def extract_company_data(self, ticker):
        """Extract company information from Yahoo Finance"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            return {
                'ticker': ticker,
                'name': info.get('longName', ticker),
                'sector': info.get('sector', 'Unknown'),
                'market_cap': info.get('marketCap'),
                'country': info.get('country', 'USA'),
                'exchange': info.get('exchange', 'NASDAQ'),
                'currency': info.get('currency', 'USD'),
                'description': info.get('longBusinessSummary', '')
            }
        except Exception as e:
            print(f"✗ Error extracting {ticker} company data: {e}")
            return None
    
    def load_company(self, company_data, sector_id):
        """Load company data into database"""
        query = """
        INSERT INTO Companies 
        (ticker_symbol, company_name, sector_id, market_cap, country, 
         exchange, currency, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        company_name = VALUES(company_name),
        market_cap = VALUES(market_cap),
        description = VALUES(description)
        """
        
        params = (
            company_data['ticker'],
            company_data['name'],
            sector_id,
            company_data['market_cap'],
            company_data['country'],
            company_data['exchange'],
            company_data['currency'],
            company_data['description']
        )
        
        company_id = self.execute_query(query, params)
        
        if not company_id:
            # Get existing company_id
            query = "SELECT company_id FROM Companies WHERE ticker_symbol = %s"
            result = self.execute_query(query, (company_data['ticker'],), fetch=True)
            if result:
                company_id = result[0]['company_id']
        
        return company_id
    
    def extract_stock_prices(self, ticker, period='2y'):
        """Extract historical stock prices"""
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period=period)
            
            if hist.empty:
                print(f"✗ No stock price data for {ticker}")
                return None
            
            hist.reset_index(inplace=True)
            return hist
        except Exception as e:
            print(f"✗ Error extracting {ticker} stock prices: {e}")
            return None
    
    def load_stock_prices(self, company_id, price_data):
        """
        Load stock price data into database with safe handling
        
        Args:
            company_id: Company ID in database
            price_data: DataFrame with stock price data
            
        Returns:
            Number of records successfully loaded
        """
        query = """
        INSERT INTO StockPrices 
        (company_id, trade_date, open_price, high_price, low_price, 
         close_price, adjusted_close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        open_price = VALUES(open_price),
        high_price = VALUES(high_price),
        low_price = VALUES(low_price),
        close_price = VALUES(close_price),
        adjusted_close = VALUES(adjusted_close),
        volume = VALUES(volume)
        """
        
        count = 0
        errors = 0
        
        for _, row in price_data.iterrows():
            try:
                # Safely extract values with validation
                open_price = self.safe_get_price(row, 'Open')
                high_price = self.safe_get_price(row, 'High')
                low_price = self.safe_get_price(row, 'Low')
                close_price = self.safe_get_price(row, 'Close')
                volume = self.safe_get_volume(row, 'Volume')
                
                # Skip row if critical data is missing
                if close_price is None or pd.isna(close_price):
                    errors += 1
                    continue
                
                # Use close as adjusted_close if not available
                adjusted_close = close_price
                
                params = (
                    company_id,
                    row['Date'].date(),
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    adjusted_close,
                    volume
                )
                
                if self.execute_query(query, params):
                    count += 1
                else:
                    errors += 1
                    
            except Exception as e:
                print(f"Error loading price record: {e}")
                errors += 1
                continue
        
        if errors > 0:
            print(f"⚠ Skipped {errors} invalid price records")
        
        return count
    
    def safe_get_price(self, row, column):
        """Safely extract price value"""
        try:
            value = row.get(column)
            if value is None or pd.isna(value):
                return None
            
            price = float(value)
            # Validate reasonable price range (positive, not extremely large)
            if price <= 0 or price > 1000000:
                return None
            
            return price
        except:
            return None
    
    def safe_get_volume(self, row, column):
        """Safely extract volume value"""
        try:
            value = row.get(column)
            if value is None or pd.isna(value):
                return 0
            
            volume = int(value)
            # Validate reasonable volume (non-negative)
            if volume < 0:
                return 0
            
            return volume
        except:
            return 0
    
    def extract_financial_statements(self, ticker):
        """Extract financial statements (quarterly)"""
        try:
            stock = yf.Ticker(ticker)
            
            # Income Statement
            income_stmt = stock.quarterly_income_stmt
            balance_sheet = stock.quarterly_balance_sheet
            cash_flow = stock.quarterly_cashflow
            
            return {
                'income': income_stmt,
                'balance': balance_sheet,
                'cashflow': cash_flow
            }
        except Exception as e:
            print(f"✗ Error extracting {ticker} financial statements: {e}")
            return None
    
    def load_income_statement(self, company_id, statement_data, period_date):
        """
        Load income statement data with comprehensive field mapping
        
        Args:
            company_id: Company ID in database
            statement_data: Series containing income statement data
            period_date: Date of the financial statement
            
        Returns:
            Boolean indicating success
        """
        # Create financial statement record
        fiscal_year = period_date.year
        fiscal_quarter = f"Q{(period_date.month - 1) // 3 + 1}"
        
        query = """
        INSERT INTO FinancialStatements 
        (company_id, fiscal_year, fiscal_quarter, filing_date, statement_type)
        VALUES (%s, %s, %s, %s, 'IncomeStatement')
        ON DUPLICATE KEY UPDATE statement_id=LAST_INSERT_ID(statement_id)
        """
        
        statement_id = self.execute_query(query, (
            company_id, fiscal_year, fiscal_quarter, period_date
        ))
        
        if not statement_id:
            query = """
            SELECT statement_id FROM FinancialStatements 
            WHERE company_id=%s AND fiscal_year=%s AND fiscal_quarter=%s 
            AND statement_type='IncomeStatement'
            """
            result = self.execute_query(query, (company_id, fiscal_year, fiscal_quarter), fetch=True)
            if result:
                statement_id = result[0]['statement_id']
        
        if statement_id:
            # Extract all income statement fields with multiple possible field names
            revenue = self.safe_get(statement_data, 'Total Revenue') or \
                     self.safe_get(statement_data, 'Revenue')
            
            cost_of_revenue = self.safe_get(statement_data, 'Cost Of Revenue') or \
                             self.safe_get(statement_data, 'Total Cost Of Revenue')
            
            gross_profit = self.safe_get(statement_data, 'Gross Profit')
            
            operating_expenses = self.safe_get(statement_data, 'Operating Expense') or \
                                self.safe_get(statement_data, 'Total Operating Expenses') or \
                                self.safe_get(statement_data, 'Operating Expenses')
            
            operating_income = self.safe_get(statement_data, 'Operating Income') or \
                              self.safe_get(statement_data, 'EBIT')
            
            # Interest expense (may be negative in the data)
            interest_expense = self.safe_get(statement_data, 'Interest Expense') or \
                              self.safe_get(statement_data, 'Interest Expense Non Operating') or \
                              self.safe_get(statement_data, 'Net Interest Income')
            
            # Income before tax
            income_before_tax = self.safe_get(statement_data, 'Pretax Income') or \
                               self.safe_get(statement_data, 'Income Before Tax') or \
                               self.safe_get(statement_data, 'Earnings Before Tax')
            
            # Tax expense
            income_tax_expense = self.safe_get(statement_data, 'Tax Provision') or \
                                self.safe_get(statement_data, 'Income Tax Expense') or \
                                self.safe_get(statement_data, 'Tax Effect Of Unusual Items')
            
            # Net income
            net_income = self.safe_get(statement_data, 'Net Income') or \
                        self.safe_get(statement_data, 'Net Income Common Stockholders')
            
            # Shares outstanding
            shares_outstanding = self.safe_get(statement_data, 'Diluted Average Shares') or \
                                self.safe_get(statement_data, 'Average Diluted Shares Outstanding') or \
                                self.safe_get(statement_data, 'Diluted NI Availto Com Stockholders')
            
            # Basic shares (for EPS calculation)
            basic_shares = self.safe_get(statement_data, 'Basic Average Shares') or \
                          self.safe_get(statement_data, 'Average Basic Shares Outstanding') or \
                          shares_outstanding  # Fallback to diluted
            
            # Calculate EPS if we have the data
            eps_basic = None
            eps_diluted = None
            
            if net_income is not None and basic_shares is not None and basic_shares > 0:
                eps_basic = net_income / basic_shares
            else:
                # Try to get directly from data
                eps_basic = self.safe_get(statement_data, 'Basic EPS') or \
                           self.safe_get(statement_data, 'Earnings Per Share Basic')
            
            if net_income is not None and shares_outstanding is not None and shares_outstanding > 0:
                eps_diluted = net_income / shares_outstanding
            else:
                # Try to get directly from data
                eps_diluted = self.safe_get(statement_data, 'Diluted EPS') or \
                             self.safe_get(statement_data, 'Earnings Per Share Diluted') or \
                             eps_basic  # Fallback to basic EPS
            
            # Insert income statement details with all fields
            query = """
            INSERT INTO IncomeStatements 
            (statement_id, revenue, cost_of_revenue, gross_profit, 
             operating_expenses, operating_income, interest_expense,
             income_before_tax, income_tax_expense, net_income, 
             eps_basic, eps_diluted, shares_outstanding)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            revenue = VALUES(revenue),
            cost_of_revenue = VALUES(cost_of_revenue),
            gross_profit = VALUES(gross_profit),
            operating_expenses = VALUES(operating_expenses),
            operating_income = VALUES(operating_income),
            interest_expense = VALUES(interest_expense),
            income_before_tax = VALUES(income_before_tax),
            income_tax_expense = VALUES(income_tax_expense),
            net_income = VALUES(net_income),
            eps_basic = VALUES(eps_basic),
            eps_diluted = VALUES(eps_diluted),
            shares_outstanding = VALUES(shares_outstanding)
            """
            
            params = (
                statement_id,
                revenue,
                cost_of_revenue,
                gross_profit,
                operating_expenses,
                operating_income,
                interest_expense,
                income_before_tax,
                income_tax_expense,
                net_income,
                eps_basic,
                eps_diluted,
                shares_outstanding
            )
            
            self.execute_query(query, params)
            return True
        
        return False
    
    def load_balance_sheet(self, company_id, statement_data, period_date):
        """
        Load balance sheet data with comprehensive field mapping
        
        Args:
            company_id: Company ID in database
            statement_data: Series containing balance sheet data
            period_date: Date of the financial statement
            
        Returns:
            Boolean indicating success
        """
        fiscal_year = period_date.year
        fiscal_quarter = f"Q{(period_date.month - 1) // 3 + 1}"
        
        query = """
        INSERT INTO FinancialStatements 
        (company_id, fiscal_year, fiscal_quarter, filing_date, statement_type)
        VALUES (%s, %s, %s, %s, 'BalanceSheet')
        ON DUPLICATE KEY UPDATE statement_id=LAST_INSERT_ID(statement_id)
        """
        
        statement_id = self.execute_query(query, (
            company_id, fiscal_year, fiscal_quarter, period_date
        ))
        
        if not statement_id:
            query = """
            SELECT statement_id FROM FinancialStatements 
            WHERE company_id=%s AND fiscal_year=%s AND fiscal_quarter=%s 
            AND statement_type='BalanceSheet'
            """
            result = self.execute_query(query, (company_id, fiscal_year, fiscal_quarter), fetch=True)
            if result:
                statement_id = result[0]['statement_id']
        
        if statement_id:
            # Extract balance sheet fields with multiple possible names
            total_assets = self.safe_get(statement_data, 'Total Assets') or \
                          self.safe_get(statement_data, 'TotalAssets')
            
            current_assets = self.safe_get(statement_data, 'Current Assets') or \
                            self.safe_get(statement_data, 'TotalCurrentAssets')
            
            cash_and_equivalents = self.safe_get(statement_data, 'Cash And Cash Equivalents') or \
                                  self.safe_get(statement_data, 'Cash Cash Equivalents And Short Term Investments') or \
                                  self.safe_get(statement_data, 'CashAndCashEquivalents')
            
            accounts_receivable = self.safe_get(statement_data, 'Accounts Receivable') or \
                                 self.safe_get(statement_data, 'Receivables') or \
                                 self.safe_get(statement_data, 'AccountsReceivable')
            
            inventory = self.safe_get(statement_data, 'Inventory') or \
                       self.safe_get(statement_data, 'Inventories')
            
            non_current_assets = self.safe_get(statement_data, 'Total Non Current Assets') or \
                                self.safe_get(statement_data, 'Non Current Assets') or \
                                self.safe_get(statement_data, 'TotalNonCurrentAssets')
            
            property_plant_equipment = self.safe_get(statement_data, 'Net PPE') or \
                                      self.safe_get(statement_data, 'Property Plant Equipment Net') or \
                                      self.safe_get(statement_data, 'Gross PPE')
            
            total_liabilities = self.safe_get(statement_data, 'Total Liabilities Net Minority Interest') or \
                               self.safe_get(statement_data, 'Total Liabilities') or \
                               self.safe_get(statement_data, 'TotalLiabilities')
            
            current_liabilities = self.safe_get(statement_data, 'Current Liabilities') or \
                                 self.safe_get(statement_data, 'TotalCurrentLiabilities')
            
            accounts_payable = self.safe_get(statement_data, 'Accounts Payable') or \
                              self.safe_get(statement_data, 'Payables') or \
                              self.safe_get(statement_data, 'AccountsPayable')
            
            short_term_debt = self.safe_get(statement_data, 'Current Debt') or \
                             self.safe_get(statement_data, 'Short Term Debt') or \
                             self.safe_get(statement_data, 'Current Debt And Capital Lease Obligation')
            
            long_term_debt = self.safe_get(statement_data, 'Long Term Debt') or \
                            self.safe_get(statement_data, 'Long Term Debt And Capital Lease Obligation') or \
                            self.safe_get(statement_data, 'LongTermDebt')
            
            total_equity = self.safe_get(statement_data, 'Total Equity Gross Minority Interest') or \
                          self.safe_get(statement_data, 'Stockholders Equity') or \
                          self.safe_get(statement_data, 'Total Equity') or \
                          self.safe_get(statement_data, 'TotalEquity')
            
            retained_earnings = self.safe_get(statement_data, 'Retained Earnings') or \
                               self.safe_get(statement_data, 'RetainedEarnings')
            
            query = """
            INSERT INTO BalanceSheets 
            (statement_id, total_assets, current_assets, cash_and_equivalents,
             accounts_receivable, inventory, non_current_assets, property_plant_equipment,
             total_liabilities, current_liabilities, accounts_payable,
             short_term_debt, long_term_debt, total_equity, retained_earnings)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            total_assets = VALUES(total_assets),
            current_assets = VALUES(current_assets),
            cash_and_equivalents = VALUES(cash_and_equivalents),
            accounts_receivable = VALUES(accounts_receivable),
            inventory = VALUES(inventory),
            non_current_assets = VALUES(non_current_assets),
            property_plant_equipment = VALUES(property_plant_equipment),
            total_liabilities = VALUES(total_liabilities),
            current_liabilities = VALUES(current_liabilities),
            accounts_payable = VALUES(accounts_payable),
            short_term_debt = VALUES(short_term_debt),
            long_term_debt = VALUES(long_term_debt),
            total_equity = VALUES(total_equity),
            retained_earnings = VALUES(retained_earnings)
            """
            
            params = (
                statement_id,
                total_assets,
                current_assets,
                cash_and_equivalents,
                accounts_receivable,
                inventory,
                non_current_assets,
                property_plant_equipment,
                total_liabilities,
                current_liabilities,
                accounts_payable,
                short_term_debt,
                long_term_debt,
                total_equity,
                retained_earnings
            )
            
            self.execute_query(query, params)
            return True
        
        return False
    
    def load_cashflow_statement(self, company_id, statement_data, period_date):
        """
        Load cash flow statement data with comprehensive field mapping
        
        Args:
            company_id: Company ID in database
            statement_data: Series containing cash flow statement data
            period_date: Date of the financial statement
            
        Returns:
            Boolean indicating success
        """
        try:
            fiscal_year = period_date.year
            fiscal_quarter = f"Q{(period_date.month - 1) // 3 + 1}"
            
            query = """
            INSERT INTO FinancialStatements 
            (company_id, fiscal_year, fiscal_quarter, filing_date, statement_type)
            VALUES (%s, %s, %s, %s, 'CashFlowStatement')
            ON DUPLICATE KEY UPDATE statement_id=LAST_INSERT_ID(statement_id)
            """
            
            statement_id = self.execute_query(query, (
                company_id, fiscal_year, fiscal_quarter, period_date
            ))
            
            if not statement_id:
                query = """
                SELECT statement_id FROM FinancialStatements 
                WHERE company_id=%s AND fiscal_year=%s AND fiscal_quarter=%s 
                AND statement_type='CashFlowStatement'
                """
                result = self.execute_query(query, (company_id, fiscal_year, fiscal_quarter), fetch=True)
                if result:
                    statement_id = result[0]['statement_id']
                else:
                    print(f"    ⚠ Could not create/find cash flow statement record for {fiscal_year} {fiscal_quarter}")
                    return False
            
            if statement_id:
                # Extract cash flow fields with multiple possible names
                operating_cash_flow = self.safe_get(statement_data, 'Operating Cash Flow') or \
                                     self.safe_get(statement_data, 'Cash Flow From Operating Activities') or \
                                     self.safe_get(statement_data, 'Total Cash From Operating Activities') or \
                                     self.safe_get(statement_data, 'Net Cash From Operating Activities')
                
                investing_cash_flow = self.safe_get(statement_data, 'Investing Cash Flow') or \
                                     self.safe_get(statement_data, 'Cash Flow From Investing Activities') or \
                                     self.safe_get(statement_data, 'Total Cash From Investing Activities') or \
                                     self.safe_get(statement_data, 'Net Cash From Investing Activities')
                
                financing_cash_flow = self.safe_get(statement_data, 'Financing Cash Flow') or \
                                     self.safe_get(statement_data, 'Cash Flow From Financing Activities') or \
                                     self.safe_get(statement_data, 'Total Cash From Financing Activities') or \
                                     self.safe_get(statement_data, 'Net Cash From Financing Activities')
                
                net_change_in_cash = self.safe_get(statement_data, 'Changes In Cash') or \
                                    self.safe_get(statement_data, 'Net Change In Cash') or \
                                    self.safe_get(statement_data, 'Change In Cash And Cash Equivalents') or \
                                    self.safe_get(statement_data, 'Net Change In Cash And Cash Equivalents')
                
                capital_expenditure = self.safe_get(statement_data, 'Capital Expenditure') or \
                                     self.safe_get(statement_data, 'Capital Expenditures') or \
                                     self.safe_get(statement_data, 'Purchase Of PPE') or \
                                     self.safe_get(statement_data, 'Purchases Of Property Plant And Equipment')
                
                # Calculate free cash flow if not directly available
                free_cash_flow = self.safe_get(statement_data, 'Free Cash Flow')
                if free_cash_flow is None and operating_cash_flow is not None and capital_expenditure is not None:
                    # FCF = Operating CF - CapEx (note: CapEx is usually negative in the data)
                    free_cash_flow = operating_cash_flow + capital_expenditure
                
                dividends_paid = self.safe_get(statement_data, 'Cash Dividends Paid') or \
                                self.safe_get(statement_data, 'Common Stock Dividend Paid') or \
                                self.safe_get(statement_data, 'Dividends Paid') or \
                                self.safe_get(statement_data, 'Payment Of Dividends')
                
                stock_repurchases = self.safe_get(statement_data, 'Repurchase Of Capital Stock') or \
                                   self.safe_get(statement_data, 'Common Stock Repurchased') or \
                                   self.safe_get(statement_data, 'Stock Repurchased') or \
                                   self.safe_get(statement_data, 'Repurchase Of Common Stock')
                
                # Debug: Log what we found
                fields_found = sum([
                    operating_cash_flow is not None,
                    investing_cash_flow is not None,
                    financing_cash_flow is not None,
                    net_change_in_cash is not None,
                    capital_expenditure is not None,
                    free_cash_flow is not None,
                    dividends_paid is not None,
                    stock_repurchases is not None
                ])
                
                print(f"    Found {fields_found}/8 cash flow fields for {fiscal_year} {fiscal_quarter}")
                
                query = """
                INSERT INTO CashFlowStatements 
                (statement_id, operating_cash_flow, investing_cash_flow,
                 financing_cash_flow, net_change_in_cash, capital_expenditure,
                 free_cash_flow, dividends_paid, stock_repurchases)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                operating_cash_flow = VALUES(operating_cash_flow),
                investing_cash_flow = VALUES(investing_cash_flow),
                financing_cash_flow = VALUES(financing_cash_flow),
                net_change_in_cash = VALUES(net_change_in_cash),
                capital_expenditure = VALUES(capital_expenditure),
                free_cash_flow = VALUES(free_cash_flow),
                dividends_paid = VALUES(dividends_paid),
                stock_repurchases = VALUES(stock_repurchases)
                """
                
                params = (
                    statement_id,
                    operating_cash_flow,
                    investing_cash_flow,
                    financing_cash_flow,
                    net_change_in_cash,
                    capital_expenditure,
                    free_cash_flow,
                    dividends_paid,
                    stock_repurchases
                )
                
                result = self.execute_query(query, params)
                if result is not None:
                    return True
                else:
                    print(f"    ⚠ Failed to insert cash flow data into database")
                    return False
            
            return False
            
        except Exception as e:
            print(f"    ✗ Error loading cash flow statement: {e}")
            import traceback
            traceback.print_exc()
            return False
        """
        Load balance sheet data with comprehensive field mapping
        
        Args:
            company_id: Company ID in database
            statement_data: Series containing balance sheet data
            period_date: Date of the financial statement
            
        Returns:
            Boolean indicating success
        """
        fiscal_year = period_date.year
        fiscal_quarter = f"Q{(period_date.month - 1) // 3 + 1}"
        
        query = """
        INSERT INTO FinancialStatements 
        (company_id, fiscal_year, fiscal_quarter, filing_date, statement_type)
        VALUES (%s, %s, %s, %s, 'BalanceSheet')
        ON DUPLICATE KEY UPDATE statement_id=LAST_INSERT_ID(statement_id)
        """
        
        statement_id = self.execute_query(query, (
            company_id, fiscal_year, fiscal_quarter, period_date
        ))
        
        if not statement_id:
            query = """
            SELECT statement_id FROM FinancialStatements 
            WHERE company_id=%s AND fiscal_year=%s AND fiscal_quarter=%s 
            AND statement_type='BalanceSheet'
            """
            result = self.execute_query(query, (company_id, fiscal_year, fiscal_quarter), fetch=True)
            if result:
                statement_id = result[0]['statement_id']
        
        if statement_id:
            # Extract balance sheet fields with multiple possible names
            total_assets = self.safe_get(statement_data, 'Total Assets') or \
                          self.safe_get(statement_data, 'TotalAssets')
            
            current_assets = self.safe_get(statement_data, 'Current Assets') or \
                            self.safe_get(statement_data, 'TotalCurrentAssets')
            
            cash_and_equivalents = self.safe_get(statement_data, 'Cash And Cash Equivalents') or \
                                  self.safe_get(statement_data, 'Cash Cash Equivalents And Short Term Investments') or \
                                  self.safe_get(statement_data, 'CashAndCashEquivalents')
            
            accounts_receivable = self.safe_get(statement_data, 'Accounts Receivable') or \
                                 self.safe_get(statement_data, 'Receivables') or \
                                 self.safe_get(statement_data, 'AccountsReceivable')
            
            inventory = self.safe_get(statement_data, 'Inventory') or \
                       self.safe_get(statement_data, 'Inventories')
            
            non_current_assets = self.safe_get(statement_data, 'Total Non Current Assets') or \
                                self.safe_get(statement_data, 'Non Current Assets') or \
                                self.safe_get(statement_data, 'TotalNonCurrentAssets')
            
            property_plant_equipment = self.safe_get(statement_data, 'Net PPE') or \
                                      self.safe_get(statement_data, 'Property Plant Equipment Net') or \
                                      self.safe_get(statement_data, 'Gross PPE')
            
            total_liabilities = self.safe_get(statement_data, 'Total Liabilities Net Minority Interest') or \
                               self.safe_get(statement_data, 'Total Liabilities') or \
                               self.safe_get(statement_data, 'TotalLiabilities')
            
            current_liabilities = self.safe_get(statement_data, 'Current Liabilities') or \
                                 self.safe_get(statement_data, 'TotalCurrentLiabilities')
            
            accounts_payable = self.safe_get(statement_data, 'Accounts Payable') or \
                              self.safe_get(statement_data, 'Payables') or \
                              self.safe_get(statement_data, 'AccountsPayable')
            
            short_term_debt = self.safe_get(statement_data, 'Current Debt') or \
                             self.safe_get(statement_data, 'Short Term Debt') or \
                             self.safe_get(statement_data, 'Current Debt And Capital Lease Obligation')
            
            long_term_debt = self.safe_get(statement_data, 'Long Term Debt') or \
                            self.safe_get(statement_data, 'Long Term Debt And Capital Lease Obligation') or \
                            self.safe_get(statement_data, 'LongTermDebt')
            
            total_equity = self.safe_get(statement_data, 'Total Equity Gross Minority Interest') or \
                          self.safe_get(statement_data, 'Stockholders Equity') or \
                          self.safe_get(statement_data, 'Total Equity') or \
                          self.safe_get(statement_data, 'TotalEquity')
            
            retained_earnings = self.safe_get(statement_data, 'Retained Earnings') or \
                               self.safe_get(statement_data, 'RetainedEarnings')
            
            query = """
            INSERT INTO BalanceSheets 
            (statement_id, total_assets, current_assets, cash_and_equivalents,
             accounts_receivable, inventory, non_current_assets, property_plant_equipment,
             total_liabilities, current_liabilities, accounts_payable,
             short_term_debt, long_term_debt, total_equity, retained_earnings)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            total_assets = VALUES(total_assets),
            current_assets = VALUES(current_assets),
            cash_and_equivalents = VALUES(cash_and_equivalents),
            accounts_receivable = VALUES(accounts_receivable),
            inventory = VALUES(inventory),
            non_current_assets = VALUES(non_current_assets),
            property_plant_equipment = VALUES(property_plant_equipment),
            total_liabilities = VALUES(total_liabilities),
            current_liabilities = VALUES(current_liabilities),
            accounts_payable = VALUES(accounts_payable),
            short_term_debt = VALUES(short_term_debt),
            long_term_debt = VALUES(long_term_debt),
            total_equity = VALUES(total_equity),
            retained_earnings = VALUES(retained_earnings)
            """
            
            params = (
                statement_id,
                total_assets,
                current_assets,
                cash_and_equivalents,
                accounts_receivable,
                inventory,
                non_current_assets,
                property_plant_equipment,
                total_liabilities,
                current_liabilities,
                accounts_payable,
                short_term_debt,
                long_term_debt,
                total_equity,
                retained_earnings
            )
            
            self.execute_query(query, params)
            return True
        
        return False
    
    def calculate_and_load_metrics(self, company_id, ticker):
        """
        Calculate and load comprehensive valuation metrics
        
        Args:
            company_id: Company ID in database
            ticker: Stock ticker symbol
            
        Returns:
            Boolean indicating success
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Get latest stock price
            query = """
            SELECT close_price, trade_date 
            FROM StockPrices 
            WHERE company_id = %s 
            ORDER BY trade_date DESC LIMIT 1
            """
            price_result = self.execute_query(query, (company_id,), fetch=True)
            
            if not price_result:
                print(f"  ⚠ No stock price data found for metric calculations")
                return False
            
            current_price = float(price_result[0]['close_price'])
            calc_date = price_result[0]['trade_date']
            
            # Get latest financial statement data for calculations
            query = """
            SELECT 
                ins.revenue,
                ins.net_income,
                ins.shares_outstanding,
                bs.total_assets,
                bs.current_assets,
                bs.current_liabilities,
                bs.cash_and_equivalents,
                bs.inventory,
                bs.total_equity,
                bs.long_term_debt,
                bs.short_term_debt
            FROM FinancialStatements fs
            LEFT JOIN IncomeStatements ins ON fs.statement_id = ins.statement_id
            LEFT JOIN BalanceSheets bs ON fs.statement_id = bs.statement_id
            WHERE fs.company_id = %s 
                AND fs.statement_type IN ('IncomeStatement', 'BalanceSheet')
            ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter DESC
            LIMIT 1
            """
            
            financial_result = self.execute_query(query, (company_id,), fetch=True)
            
            if not financial_result or not financial_result[0]:
                print(f"  ⚠ No financial statement data found for metric calculations")
                return False
            
            fin_data = financial_result[0]
            
            # Extract financial values
            revenue = self.safe_get_metric(fin_data, 'revenue')
            net_income = self.safe_get_metric(fin_data, 'net_income')
            shares_outstanding = self.safe_get_metric(fin_data, 'shares_outstanding')
            total_assets = self.safe_get_metric(fin_data, 'total_assets')
            current_assets = self.safe_get_metric(fin_data, 'current_assets')
            current_liabilities = self.safe_get_metric(fin_data, 'current_liabilities')
            cash_and_equivalents = self.safe_get_metric(fin_data, 'cash_and_equivalents')
            inventory = self.safe_get_metric(fin_data, 'inventory')
            total_equity = self.safe_get_metric(fin_data, 'total_equity')
            long_term_debt = self.safe_get_metric(fin_data, 'long_term_debt') or 0
            short_term_debt = self.safe_get_metric(fin_data, 'short_term_debt') or 0
            
            print(f"  Calculating metrics from financial data...")
            
            # Calculate Market Cap
            market_cap = None
            if shares_outstanding:
                market_cap = current_price * shares_outstanding
            
            # 1. P/E Ratio (Price-to-Earnings)
            pe_ratio = None
            earnings_per_share = None
            if net_income and shares_outstanding and shares_outstanding > 0:
                earnings_per_share = net_income / shares_outstanding
                if earnings_per_share > 0:
                    pe_ratio = current_price / earnings_per_share
            
            # Try from API if calculation failed
            if pe_ratio is None:
                pe_ratio = info.get('trailingPE')
            
            # 2. P/B Ratio (Price-to-Book)
            pb_ratio = None
            book_value_per_share = None
            if total_equity and shares_outstanding and shares_outstanding > 0:
                book_value_per_share = total_equity / shares_outstanding
                if book_value_per_share > 0:
                    pb_ratio = current_price / book_value_per_share
            
            # Try from API if calculation failed
            if pb_ratio is None:
                pb_ratio = info.get('priceToBook')
            
            # 3. P/S Ratio (Price-to-Sales)
            ps_ratio = None
            sales_per_share = None
            if revenue and shares_outstanding and shares_outstanding > 0:
                sales_per_share = revenue / shares_outstanding
                if sales_per_share > 0:
                    ps_ratio = current_price / sales_per_share
            
            # Try from API if calculation failed
            if ps_ratio is None:
                ps_ratio = info.get('priceToSalesTrailing12Months')
            
            # 4. ROE (Return on Equity)
            roe = None
            if net_income and total_equity and total_equity > 0:
                roe = (net_income / total_equity) * 100  # Convert to percentage
            
            # Try from API if calculation failed
            if roe is None:
                api_roe = info.get('returnOnEquity')
                if api_roe:
                    roe = api_roe * 100  # API returns as decimal
            
            # 5. ROA (Return on Assets)
            roa = None
            if net_income and total_assets and total_assets > 0:
                roa = (net_income / total_assets) * 100  # Convert to percentage
            
            # Try from API if calculation failed
            if roa is None:
                api_roa = info.get('returnOnAssets')
                if api_roa:
                    roa = api_roa * 100  # API returns as decimal
            
            # 6. Debt-to-Equity Ratio
            debt_to_equity = None
            total_debt = long_term_debt + short_term_debt
            if total_debt and total_equity and total_equity > 0:
                debt_to_equity = total_debt / total_equity
            
            # Try from API if calculation failed
            if debt_to_equity is None:
                debt_to_equity = info.get('debtToEquity')
                if debt_to_equity:
                    debt_to_equity = debt_to_equity / 100  # API returns as percentage
            
            # 7. Current Ratio
            current_ratio = None
            if current_assets and current_liabilities and current_liabilities > 0:
                current_ratio = current_assets / current_liabilities
            
            # Try from API if calculation failed
            if current_ratio is None:
                current_ratio = info.get('currentRatio')
            
            # 8. Quick Ratio (Acid Test Ratio)
            quick_ratio = None
            if current_assets and inventory is not None and current_liabilities and current_liabilities > 0:
                quick_assets = current_assets - inventory
                quick_ratio = quick_assets / current_liabilities
            elif cash_and_equivalents and current_liabilities and current_liabilities > 0:
                # Simplified quick ratio using cash only
                quick_ratio = cash_and_equivalents / current_liabilities
            
            # Try from API if calculation failed
            if quick_ratio is None:
                quick_ratio = info.get('quickRatio')
            
            # 9. Gross Margin
            gross_margin = None
            api_gross_margin = info.get('grossMargins')
            if api_gross_margin:
                gross_margin = api_gross_margin * 100  # Convert to percentage
            
            # 10. Operating Margin
            operating_margin = None
            api_operating_margin = info.get('operatingMargins')
            if api_operating_margin:
                operating_margin = api_operating_margin * 100  # Convert to percentage
            
            # 11. Net Margin (Profit Margin)
            net_margin = None
            if net_income and revenue and revenue > 0:
                net_margin = (net_income / revenue) * 100  # Convert to percentage
            
            # Try from API if calculation failed
            if net_margin is None:
                api_net_margin = info.get('profitMargins')
                if api_net_margin:
                    net_margin = api_net_margin * 100  # API returns as decimal
            
            # Log calculated metrics
            metrics_calculated = sum([
                pe_ratio is not None,
                pb_ratio is not None,
                ps_ratio is not None,
                roe is not None,
                roa is not None,
                debt_to_equity is not None,
                current_ratio is not None,
                quick_ratio is not None,
                gross_margin is not None,
                operating_margin is not None,
                net_margin is not None
            ])
            
            print(f"  Successfully calculated {metrics_calculated}/11 metrics")
            
            # Insert metrics into database
            query = """
            INSERT INTO ValuationMetrics 
            (company_id, calculation_date, pe_ratio, pb_ratio, ps_ratio, 
             roe, roa, debt_to_equity, current_ratio, quick_ratio,
             gross_margin, operating_margin, net_margin)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            pe_ratio = VALUES(pe_ratio),
            pb_ratio = VALUES(pb_ratio),
            ps_ratio = VALUES(ps_ratio),
            roe = VALUES(roe),
            roa = VALUES(roa),
            debt_to_equity = VALUES(debt_to_equity),
            current_ratio = VALUES(current_ratio),
            quick_ratio = VALUES(quick_ratio),
            gross_margin = VALUES(gross_margin),
            operating_margin = VALUES(operating_margin),
            net_margin = VALUES(net_margin)
            """
            
            params = (
                company_id,
                calc_date,
                pe_ratio,
                pb_ratio,
                ps_ratio,
                roe,
                roa,
                debt_to_equity,
                current_ratio,
                quick_ratio,
                gross_margin,
                operating_margin,
                net_margin
            )
            
            result = self.execute_query(query, params)
            
            if result is not None:
                return True
            else:
                print(f"  ⚠ Failed to insert valuation metrics")
                return False
            
        except Exception as e:
            print(f"  ✗ Error calculating metrics for {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def safe_get_metric(self, data_dict, key):
        """
        Safely extract metric value from dictionary
        
        Args:
            data_dict: Dictionary containing metric data
            key: Key to extract
            
        Returns:
            Float value or None
        """
        try:
            value = data_dict.get(key)
            if value is None:
                return None
            if pd.isna(value):
                return None
            
            float_val = float(value)
            if np.isinf(float_val) or np.isnan(float_val):
                return None
            
            return float_val
        except:
            return None
    
    def safe_get(self, data, key):
        """
        Safely extract value from DataFrame
        
        Args:
            data: pandas Series or DataFrame
            key: Key/index to extract
            
        Returns:
            Float value or None if not found/invalid
        """
        try:
            if key in data.index:
                value = data[key]
                # Handle various null/missing value types
                if value is None:
                    return None
                if pd.isna(value):
                    return None
                if isinstance(value, str) and value.lower() in ['nan', 'none', 'null', '']:
                    return None
                
                # Try to convert to float
                try:
                    float_value = float(value)
                    # Check for infinity
                    if np.isinf(float_value):
                        return None
                    return float_value
                except (ValueError, TypeError):
                    return None
        except Exception as e:
            print(f"Warning: Error extracting {key}: {e}")
            pass
        return None
    
    def run_full_etl(self):
        """Execute complete ETL pipeline for all companies"""
        if not self.connect():
            return
        
        print("\n" + "="*60)
        print("EQUITY RESEARCH DATABASE - ETL PIPELINE")
        print("="*60 + "\n")
        
        for company in self.target_companies:
            ticker = company['ticker']
            print(f"\n{'─'*60}")
            print(f"Processing: {ticker} - {company['name']}")
            print(f"{'─'*60}")
            
            # 1. Extract and Load Company Info
            print(f"[1/5] Extracting company information...")
            company_data = self.extract_company_data(ticker)
            if not company_data:
                continue
            
            sector_id = self.get_or_create_sector(company['sector'])
            company_id = self.load_company(company_data, sector_id)
            
            if not company_id:
                print(f"✗ Failed to load company {ticker}")
                continue
            
            print(f"✓ Company loaded (ID: {company_id})")
            
            # 2. Extract and Load Stock Prices
            print(f"[2/5] Extracting stock prices (2 years)...")
            price_data = self.extract_stock_prices(ticker)
            if price_data is not None:
                count = self.load_stock_prices(company_id, price_data)
                print(f"✓ Loaded {count} price records")
            
            # 3. Extract and Load Financial Statements
            print(f"[3/5] Extracting financial statements...")
            financials = self.extract_financial_statements(ticker)
            
            if financials:
                # Process quarterly statements
                income_loaded = 0
                balance_loaded = 0
                cashflow_loaded = 0
                
                # Income statements
                if financials['income'] is not None and not financials['income'].empty:
                    for col in financials['income'].columns[:4]:  # Last 4 quarters
                        if self.load_income_statement(company_id, financials['income'][col], col):
                            income_loaded += 1
                    print(f"✓ Loaded {income_loaded} income statements")
                else:
                    print("⚠ No income statement data available")
                
                # Balance sheets
                if financials['balance'] is not None and not financials['balance'].empty:
                    for col in financials['balance'].columns[:4]:
                        if self.load_balance_sheet(company_id, financials['balance'][col], col):
                            balance_loaded += 1
                    print(f"✓ Loaded {balance_loaded} balance sheets")
                else:
                    print("⚠ No balance sheet data available")
                
                # Cash flow statements
                if financials['cashflow'] is not None and not financials['cashflow'].empty:
                    print(f"  Processing {len(financials['cashflow'].columns)} cash flow periods...")
                    for col in financials['cashflow'].columns[:4]:
                        if self.load_cashflow_statement(company_id, financials['cashflow'][col], col):
                            cashflow_loaded += 1
                        else:
                            print(f"  ⚠ Failed to load cash flow for period {col}")
                    print(f"✓ Loaded {cashflow_loaded} cash flow statements")
                else:
                    print("⚠ No cash flow data available")
            else:
                print("⚠ No financial statement data extracted")
            
            # 4. Calculate Valuation Metrics
            print(f"[4/5] Calculating valuation metrics...")
            if self.calculate_and_load_metrics(company_id, ticker):
                print(f"✓ Valuation metrics calculated")
            
            # 5. Rate limiting
            print(f"[5/5] Waiting to avoid API rate limits...")
            time.sleep(2)
        
        print("\n" + "="*60)
        print("ETL PIPELINE COMPLETED")
        print("="*60 + "\n")
        
        self.disconnect()


# Example usage
if __name__ == "__main__":
    # Database configuration
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'your_password',  # Change this
        'database': 'equity_research_db'
    }
    
    # Run ETL Pipeline
    etl = EquityETLPipeline(db_config)
    etl.run_full_etl()
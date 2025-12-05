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
import numpy as np
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
        """Calculate and load valuation metrics"""
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
                return False
            
            current_price = float(price_result[0]['close_price'])
            calc_date = price_result[0]['trade_date']
            
            # Calculate metrics
            pe_ratio = info.get('trailingPE')
            pb_ratio = info.get('priceToBook')
            roe = info.get('returnOnEquity')
            debt_to_equity = info.get('debtToEquity')
            
            query = """
            INSERT INTO ValuationMetrics 
            (company_id, calculation_date, pe_ratio, pb_ratio, roe, debt_to_equity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            pe_ratio = VALUES(pe_ratio),
            pb_ratio = VALUES(pb_ratio),
            roe = VALUES(roe),
            debt_to_equity = VALUES(debt_to_equity)
            """
            
            params = (
                company_id,
                calc_date,
                pe_ratio if pe_ratio else None,
                pb_ratio if pb_ratio else None,
                roe * 100 if roe else None,  # Convert to percentage
                debt_to_equity if debt_to_equity else None
            )
            
            self.execute_query(query, params)
            return True
            
        except Exception as e:
            print(f"✗ Error calculating metrics for {ticker}: {e}")
            return False
    
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
    
    # ==================== FORECAST METHODS ====================
    
    def calculate_stock_price_forecast(self, price_data, company_id=None, periods_ahead=30, cutoff_index=None):
        """
        Forecast stock price, EPS, and revenue using multiple mathematical models
        
        Args:
            price_data: DataFrame with stock price history
            company_id: Company ID in database (for EPS/Revenue forecasting, optional)
            periods_ahead: Number of days to forecast ahead (default: 30 days)
            cutoff_index: Index up to which historical data to use (for historical backtesting)
                         If None, uses all available data
            
        Returns:
            Dictionary with comprehensive forecast data including price, EPS, and revenue
        """
        try:
            if price_data is None or price_data.empty or len(price_data) < 20:
                return None
            
            # Sort by date to ensure correct order
            price_data = price_data.sort_values('Date').reset_index(drop=True)
            
            # Use only data up to cutoff_index if provided (for historical forecasts)
            if cutoff_index is not None:
                if cutoff_index < 20:
                    return None
                historical_data = price_data.iloc[:cutoff_index]
            else:
                historical_data = price_data
            
            close_prices = historical_data['Close'].values
            dates = historical_data['Date'].values
            
            # Model 1: Exponential Weighted Moving Average (EWMA)
            ewma_prices = pd.Series(close_prices).ewm(span=20).mean()
            ewma_current = ewma_prices.iloc[-1]
            
            # Model 2: Linear Regression Trend
            x = np.arange(len(close_prices)).reshape(-1, 1)
            from numpy.polynomial import Polynomial
            
            # Fit polynomial trend (degree 2)
            coeffs = np.polyfit(np.arange(len(close_prices)), close_prices, 2)
            poly = np.poly1d(coeffs)
            
            # Get trend direction
            recent_trend = (close_prices[-1] - close_prices[-20]) / close_prices[-20] * 100
            
            # Model 3: Mean Reversion with Volatility
            sma_20 = pd.Series(close_prices).rolling(window=20).mean().iloc[-1]
            sma_50 = pd.Series(close_prices).rolling(window=50).mean().iloc[-1] if len(close_prices) >= 50 else sma_20
            volatility = pd.Series(close_prices).pct_change().std() * np.sqrt(252)  # Annualized
            
            # Calculate forecast
            current_price = close_prices[-1]
            trend_strength = recent_trend / 100
            
            # Blended forecast: 40% EWMA, 30% trend continuation, 30% mean reversion
            ewma_forecast = ewma_current * (1 + trend_strength * 0.3)  # Dampened trend
            trend_forecast = current_price * (1 + trend_strength * 0.2)
            mr_forecast = (sma_20 + sma_50) / 2 * (1 + trend_strength * 0.1)
            
            target_price = (ewma_forecast * 0.4 + trend_forecast * 0.3 + mr_forecast * 0.3)
            
            # Calculate confidence score based on trend consistency and volatility
            price_momentum = abs(recent_trend) / 100
            volatility_factor = 1 / (1 + volatility)  # Lower volatility = higher confidence
            confidence = min(0.95, 0.5 + price_momentum * 0.3 + volatility_factor * 0.2)
            
            # Generate recommendation
            price_change_pct = (target_price - current_price) / current_price * 100
            
            if price_change_pct > 15:
                recommendation = 'Strong Buy'
            elif price_change_pct > 7:
                recommendation = 'Buy'
            elif price_change_pct < -15:
                recommendation = 'Strong Sell'
            elif price_change_pct < -7:
                recommendation = 'Sell'
            else:
                recommendation = 'Hold'
            
            # Initialize forecast dictionary with price data
            forecast_dict = {
                'target_price': float(target_price),
                'current_price': float(current_price),
                'price_change_pct': float(price_change_pct),
                'confidence_score': float(confidence),
                'recommendation': recommendation,
                'volatility': float(volatility),
                'trend_strength': float(trend_strength),
                'sma_20': float(sma_20),
                'sma_50': float(sma_50)
            }
            
            # Calculate EPS and Revenue forecasts if company_id is provided
            if company_id is not None:
                eps_forecast = self.calculate_eps_forecast(company_id, None)
                print("EPS Forecast:", eps_forecast)
                revenue_forecast = self.calculate_revenue_forecast(company_id)
                
                # Add EPS forecast data to dictionary
                if eps_forecast:
                    forecast_dict['eps_current'] = eps_forecast.get('current_eps')
                    forecast_dict['eps_forecasted'] = eps_forecast.get('forecasted_eps')
                    forecast_dict['eps_growth_rate'] = eps_forecast.get('growth_rate')
                    forecast_dict['eps_dampened_growth'] = eps_forecast.get('dampened_growth_rate')
                else:
                    forecast_dict['eps_current'] = None
                    forecast_dict['eps_forecasted'] = None
                    forecast_dict['eps_growth_rate'] = None
                    forecast_dict['eps_dampened_growth'] = None
                
                # Add Revenue forecast data to dictionary
                if revenue_forecast:
                    forecast_dict['revenue_last_quarter'] = revenue_forecast.get('last_quarter_revenue')
                    forecast_dict['revenue_annualized'] = revenue_forecast.get('annualized_revenue')
                    forecast_dict['revenue_forecasted'] = revenue_forecast.get('forecasted_annual_revenue')
                    forecast_dict['revenue_growth_rate'] = revenue_forecast.get('growth_rate')
                else:
                    forecast_dict['revenue_last_quarter'] = None
                    forecast_dict['revenue_annualized'] = None
                    forecast_dict['revenue_forecasted'] = None
                    forecast_dict['revenue_growth_rate'] = None
            
            return forecast_dict
            
        except Exception as e:
            print(f"Error calculating stock price forecast: {e}")
            return None
    
    def calculate_eps_forecast(self, company_id, ticker):
        """
        Forecast EPS using historical growth rates and financial trends
        
        Args:
            company_id: Company ID in database
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with EPS forecast data
        """
        try:
            # Get last 4 quarters of EPS data
            query = """
            SELECT ist.eps_diluted, fs.fiscal_quarter, fs.fiscal_year
            FROM IncomeStatements ist
            JOIN FinancialStatements fs ON ist.statement_id = fs.statement_id
            WHERE fs.company_id = %s AND fs.statement_type = 'IncomeStatement'
            ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter DESC
            LIMIT 4
            """
            
            results = self.execute_query(query, (company_id,), fetch=True)
            
            if not results or len(results) < 2:
                return None
            
            eps_values = [r['eps_diluted'] for r in results if r['eps_diluted'] is not None]
            
            if len(eps_values) < 2:
                return None
            
            eps_values = list(reversed(eps_values))  # Chronological order
            print("EPS Values for Forecasting type:", type(eps_values[0]))
            eps_values = [float(d) for d in eps_values]
            print("EPS Values for Forecasting type:", type(eps_values[0]))
            
            # Calculate growth rate
            if len(eps_values) >= 2:
                # Use compound growth rate
                quarterly_growth_rates = []
                for i in range(1, len(eps_values)):
                    if eps_values[i-1] != 0:
                        growth = (eps_values[i] - eps_values[i-1]) / abs(eps_values[i-1])
                        quarterly_growth_rates.append(growth)
                
                if quarterly_growth_rates:
                    avg_growth = np.mean(quarterly_growth_rates)
                    # Assume next 4 quarters follow similar trend (with dampening)
                    current_eps = eps_values[-1]
                    dampened_growth = avg_growth * 0.7  # Dampen future growth
                    next_eps = current_eps * (1 + dampened_growth * 4)  # 4 quarters ahead
                    print("------------------------------------------------")
                    return {
                        'current_eps': float(current_eps),
                        'forecasted_eps': float(next_eps),
                        'growth_rate': float(avg_growth * 100),  # Percentage
                        'dampened_growth_rate': float(dampened_growth * 4 * 100)
                    }
        
        except Exception as e:
            print(f"Error calculating EPS forecast: {e}")
            return None
        
        return None
    
    def calculate_revenue_forecast(self, company_id):
        """
        Forecast annual revenue based on historical growth and financial statements
        
        Args:
            company_id: Company ID in database
            
        Returns:
            Dictionary with revenue forecast data
        """
        try:
            # Get last 4 quarters of revenue data
            query = """
            SELECT ist.revenue, fs.fiscal_year, fs.fiscal_quarter
            FROM IncomeStatements ist
            JOIN FinancialStatements fs ON ist.statement_id = fs.statement_id
            WHERE fs.company_id = %s AND fs.statement_type = 'IncomeStatement' 
            AND ist.revenue IS NOT NULL
            ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter DESC
            LIMIT 4
            """
            
            results = self.execute_query(query, (company_id,), fetch=True)
            
            if not results or len(results) < 2:
                return None
            
            revenue_values = [r['revenue'] for r in results if r['revenue'] is not None]
            
            if len(revenue_values) < 2:
                return None
            
            revenue_values = list(reversed(revenue_values))  # Chronological order
            revenue_values = [float(d) for d in revenue_values]
            
            # Calculate growth rate
            quarterly_growth_rates = []
            for i in range(1, len(revenue_values)):
                if revenue_values[i-1] > 0:
                    growth = (revenue_values[i] - revenue_values[i-1]) / revenue_values[i-1]
                    quarterly_growth_rates.append(growth)
            
            if quarterly_growth_rates:
                avg_growth = np.mean(quarterly_growth_rates)
                # Project next 4 quarters
                current_revenue = revenue_values[-1]
                dampened_growth = avg_growth * 0.75  # Dampen growth projection
                annualized_revenue = revenue_values[-1] * 4  # Annualize last quarter
                next_year_revenue = annualized_revenue * (1 + dampened_growth * 4)
                
                return {
                    'last_quarter_revenue': float(current_revenue),
                    'annualized_revenue': float(annualized_revenue),
                    'forecasted_annual_revenue': float(next_year_revenue),
                    'growth_rate': float(avg_growth * 100)
                }
        
        except Exception as e:
            print(f"Error calculating revenue forecast: {e}")
            return None
        
        return None
    
    def load_forecast(self, company_id, forecast_data, revenue_forecast, eps_forecast, 
                      forecast_date=None, target_date=None):
        """
        Load forecast data into Forecasts table
        
        Args:
            company_id: Company ID
            forecast_data: Dictionary with stock price forecast
            revenue_forecast: Dictionary with revenue forecast
            eps_forecast: Dictionary with EPS forecast
            forecast_date: Date when forecast is made (default: today)
            target_date: Target date for forecast (default: 30 days from forecast_date)
            
        Returns:
            Boolean indicating success
        """
        try:
            query = """
            INSERT INTO Forecasts
            (company_id, forecast_date, target_date, target_price, 
             revenue_forecast, eps_forecast, recommendation, confidence_score, model_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Mathematical-v1.0')
            """
            if forecast_date is None:
                forecast_date = datetime.now().date()
            if target_date is None:
                target_date = forecast_date + timedelta(days=30)
            
            target_price = forecast_data.get('target_price') if forecast_data else None
            revenue = revenue_forecast.get('forecasted_annual_revenue') if revenue_forecast else None
            eps = eps_forecast.get('eps_forecasted') if eps_forecast else None
            recommendation = forecast_data.get('recommendation', 'Hold') if forecast_data else 'Hold'
            confidence = forecast_data.get('confidence_score', 0.5) if forecast_data else 0.5
            
            params = (
                company_id,
                forecast_date,
                target_date,
                target_price,
                revenue_forecast["forecasted_annual_revenue"],
                eps_forecast["forecasted_eps"],
                recommendation,
                confidence
            )
            print("inside loading forecast eps_forecast:", eps_forecast)
            print("inside loading forecast revenue_forecast:", revenue_forecast)
            print("Loading forecast with params:", params)
            
            if self.execute_query(query, params):
                return True
            else:
                return False
                
        except Exception as e:
            print(f"Error loading forecast: {e}")
            return False
    
    def generate_and_load_forecasts(self, company_id, ticker, periodic=False):
        """
        Generate all forecasts for a company and load to database
        
        Args:
            company_id: Company ID in database
            ticker: Stock ticker symbol
            periodic: If True, generate forecasts for every 15 days in historical data range
                     If False, generate only current forecast
            
        Returns:
            Boolean indicating success
        """
        try:
            # Get historical price data
            price_data = self.extract_stock_prices(ticker, period='2y')
            
            if price_data is None or price_data.empty:
                print(f"⚠ No price data available for {ticker} forecast")
                return False
            
            price_data = price_data.sort_values('Date').reset_index(drop=True)
            
            if periodic:
                return self._generate_periodic_forecasts(company_id, ticker, price_data)
            else:
                return self._generate_current_forecast(company_id, ticker, price_data)
                
        except Exception as e:
            print(f"Error generating forecasts for {ticker}: {e}")
            return False
    
    def _generate_current_forecast(self, company_id, ticker, price_data):
        """
        Generate current forecast using all available historical data
        
        Args:
            company_id: Company ID
            ticker: Stock ticker
            price_data: Historical price DataFrame
            
        Returns:
            Boolean indicating success
        """
        try:
            # Calculate all forecasts using current data (integrated in stock price forecast)
            stock_forecast = self.calculate_stock_price_forecast(price_data, company_id=company_id)
            print(f"\n  Current Forecast for {ticker}: {stock_forecast}")
            if stock_forecast:
                # Load forecast to database
                # Extract price, EPS, and revenue forecasts from the integrated calculation
                price_forecast_data = {
                    'target_price': stock_forecast.get('target_price'),
                    'recommendation': stock_forecast.get('recommendation'),
                    'confidence_score': stock_forecast.get('confidence_score')
                }
                
                revenue_forecast_data = {
                    'forecasted_annual_revenue': stock_forecast.get('revenue_forecasted')
                }
                
                eps_forecast_data = {
                    'forecasted_eps': stock_forecast.get('eps_forecasted')
                }
                
                if self.load_forecast(company_id, price_forecast_data, revenue_forecast_data, eps_forecast_data):
                    print(f"  Target Price: ${stock_forecast['target_price']:.2f} | " +
                          f"Recommendation: {stock_forecast['recommendation']} | " +
                          f"Confidence: {stock_forecast['confidence_score']:.2%}")
                    if stock_forecast.get('eps_forecasted'):
                        print(f"  EPS Forecast: ${stock_forecast['eps_forecasted']:.4f} (Growth: {stock_forecast.get('eps_dampened_growth', 0):.2f}%)")
                    if stock_forecast.get('revenue_forecasted'):
                        print(f"  Revenue Forecast: ${stock_forecast['revenue_forecasted']:,.0f} (Growth: {stock_forecast.get('revenue_growth_rate', 0):.2f}%)")
                    return True
                else:
                    print(f"⚠ Failed to load forecast for {ticker}")
                    return False
            else:
                print(f"⚠ Could not calculate forecast for {ticker}")
                return False
                
        except Exception as e:
            print(f"Error generating current forecast for {ticker}: {e}")
            return False
    
    def _generate_periodic_forecasts(self, company_id, ticker, price_data):
        """
        Generate forecasts periodically every 15 days across historical data range
        
        Args:
            company_id: Company ID
            ticker: Stock ticker
            price_data: Historical price DataFrame
            
        Returns:
            Boolean indicating success
        """
        try:
            price_data = price_data.sort_values('Date').reset_index(drop=True)
            
            # Find valid interval points every 15 days
            start_date = price_data.iloc[0]['Date']
            end_date = price_data.iloc[-1]['Date']
            
            # Generate 15-day intervals
            current_date = start_date
            forecast_count = 0
            failed_count = 0
            
            print(f"\n  Generating periodic forecasts every 15 days from {start_date.date()} to {end_date.date()}...")
            
            while current_date <= end_date:
                # Find the index of the closest date
                date_diff = (price_data['Date'] - current_date).abs()
                closest_idx = date_diff.argmin()
                
                # Skip if we don't have enough data before this point
                if closest_idx < 20:
                    current_date += timedelta(days=15)
                    continue
                
                # Skip if this date doesn't have 30 days of future data for target_date
                future_data = price_data[price_data['Date'] > price_data.iloc[closest_idx]['Date']]
                if len(future_data) < 5:  # At least some future data
                    break
                
                try:
                    forecast_date = price_data.iloc[closest_idx]['Date'].date()
                    target_date = forecast_date + timedelta(days=30)
                    
                    # Calculate forecasts using data up to this point (integrated in stock price forecast)
                    stock_forecast = self.calculate_stock_price_forecast(price_data, company_id=company_id, cutoff_index=closest_idx+1)
                    
                    if stock_forecast:
                        # Load forecast with specific dates
                        # Extract price, EPS, and revenue forecasts from the integrated calculation
                        price_forecast_data = {
                            'target_price': stock_forecast.get('target_price'),
                            'recommendation': stock_forecast.get('recommendation'),
                            'confidence_score': stock_forecast.get('confidence_score')
                        }
                        
                        revenue_forecast_data = {
                            'forecasted_annual_revenue': stock_forecast.get('revenue_forecasted')
                        }
                        
                        eps_forecast_data = {
                            'forecasted_eps': stock_forecast.get('eps_forecasted')
                        }
                        
                        if self.load_forecast(company_id, price_forecast_data, revenue_forecast_data, eps_forecast_data,
                                            forecast_date=forecast_date, target_date=target_date):
                            forecast_count += 1
                        else:
                            failed_count += 1
                    
                except Exception as e:
                    print(f"    ⚠ Error generating forecast for date {current_date.date()}: {e}")
                    failed_count += 1
                
                # Move to next 15-day interval
                current_date += timedelta(days=15)
            
            if forecast_count > 0:
                print(f"  ✓ Generated {forecast_count} periodic forecasts")
                if failed_count > 0:
                    print(f"  ⚠ Failed to generate {failed_count} forecasts")
                return True
            else:
                print(f"  ⚠ Could not generate any periodic forecasts for {ticker}")
                return False
                
        except Exception as e:
            print(f"Error generating periodic forecasts for {ticker}: {e}")
            return False
    
    
    def run_full_etl(self, enable_periodic_forecasts=False):
        """
        Execute complete ETL pipeline for all companies
        
        Args:
            enable_periodic_forecasts: If True, generates forecasts for every 15 days
                                      in the historical data range. If False, generates
                                      only current forecasts. Default: False
        """
        if not self.connect():
            return
        
        print("\n" + "="*60)
        print("EQUITY RESEARCH DATABASE - ETL PIPELINE")
        if enable_periodic_forecasts:
            print("(With Periodic Forecasting: Every 15 Days)")
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
            print(f"[4/6] Calculating valuation metrics...")
            if self.calculate_and_load_metrics(company_id, ticker):
                print(f"✓ Valuation metrics calculated")
            
            # 5. Generate and Load Forecasts
            if enable_periodic_forecasts:
                print(f"[5/6] Generating periodic forecasts (every 15 days)...")
                if self.generate_and_load_forecasts(company_id, ticker, periodic=True):
                    print(f"✓ Periodic forecasts generated and loaded")
                else:
                    print(f"⚠ Could not generate periodic forecasts for {ticker}")
            else:
                print(f"[5/6] Generating current forecasts...")
                if self.generate_and_load_forecasts(company_id, ticker, periodic=False):
                    print(f"✓ Current forecasts generated and loaded")
                else:
                    print(f"⚠ Could not generate forecasts for {ticker}")
            
            # 6. Rate limiting
            print(f"[6/6] Waiting to avoid API rate limits...")
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
        'database': 'EquityResearchDB',
    }
    
    # Create ETL pipeline instance
    etl = EquityETLPipeline(db_config)
    
    # Run ETL Pipeline with current forecasts only
    print("Running ETL Pipeline with current forecasts...")
    etl.run_full_etl(enable_periodic_forecasts=True)
    
    # OPTIONAL: Run ETL Pipeline with periodic forecasts for every 15 days
    # Uncomment the following line to enable periodic forecasting
    # This will generate forecasts for every 15-day interval across the entire
    # historical data range available (e.g., 2 years of data will generate ~48 forecasts per stock)
    # Note: This will take significantly longer to run
    # print("\nRunning ETL Pipeline with periodic forecasts...")
    # etl.run_full_etl(enable_periodic_forecasts=True)
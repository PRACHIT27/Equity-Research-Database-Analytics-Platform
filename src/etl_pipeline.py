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
    
    def extract_stock_prices(self, ticker, period='5y'):
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
        """Load stock price data into database"""
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
        for _, row in price_data.iterrows():
            params = (
                company_id,
                row['Date'].date(),
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                float(row['Close']),  # Using Close as adjusted_close
                int(row['Volume'])
            )
            
            if self.execute_query(query, params):
                count += 1
        
        return count
    
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
        """Load income statement data"""
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
            # Insert income statement details
            query = """
            INSERT INTO IncomeStatements 
            (statement_id, revenue, cost_of_revenue, gross_profit, 
             operating_expenses, operating_income, net_income, shares_outstanding)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            revenue = VALUES(revenue),
            net_income = VALUES(net_income)
            """
            
            params = (
                statement_id,
                self.safe_get(statement_data, 'Total Revenue'),
                self.safe_get(statement_data, 'Cost Of Revenue'),
                self.safe_get(statement_data, 'Gross Profit'),
                self.safe_get(statement_data, 'Operating Expense'),
                self.safe_get(statement_data, 'Operating Income'),
                self.safe_get(statement_data, 'Net Income'),
                self.safe_get(statement_data, 'Diluted Average Shares')
            )
            
            self.execute_query(query, params)
            return True
        
        return False
    
    def load_balance_sheet(self, company_id, statement_data, period_date):
        """Load balance sheet data"""
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
            query = """
            INSERT INTO BalanceSheets 
            (statement_id, total_assets, current_assets, cash_and_equivalents,
             total_liabilities, current_liabilities, long_term_debt, total_equity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            total_assets = VALUES(total_assets),
            total_equity = VALUES(total_equity)
            """
            
            params = (
                statement_id,
                self.safe_get(statement_data, 'Total Assets'),
                self.safe_get(statement_data, 'Current Assets'),
                self.safe_get(statement_data, 'Cash And Cash Equivalents'),
                self.safe_get(statement_data, 'Total Liabilities Net Minority Interest'),
                self.safe_get(statement_data, 'Current Liabilities'),
                self.safe_get(statement_data, 'Long Term Debt'),
                self.safe_get(statement_data, 'Total Equity Gross Minority Interest')
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
        """Safely extract value from DataFrame"""
        try:
            if key in data.index:
                value = data[key]
                if pd.notna(value):
                    return float(value)
        except:
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
                if financials['income'] is not None and not financials['income'].empty:
                    for col in financials['income'].columns[:4]:  # Last 4 quarters
                        self.load_income_statement(company_id, financials['income'][col], col)
                    print(f"✓ Income statements loaded")
                
                if financials['balance'] is not None and not financials['balance'].empty:
                    for col in financials['balance'].columns[:4]:
                        self.load_balance_sheet(company_id, financials['balance'][col], col)
                    print(f"✓ Balance sheets loaded")
            
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
        'database': 'EquityResearchDB'
    }
    
    # Run ETL Pipeline
    etl = EquityETLPipeline(db_config)
    etl.run_full_etl()
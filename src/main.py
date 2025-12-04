"""
Equity Research Database - Main Application
CLI Interface for querying and analyzing financial data
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime, timedelta
import getpass
from tabulate import tabulate

class EquityResearchApp:
    """Main application for Equity Research Database"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.current_user = None
        self.user_role = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            return True
        except Error as e:
            print(f"✗ Database connection error: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
    
    def execute_query(self, query, params=None):
        """Execute SQL query and return results"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            results = cursor.fetchall()
            cursor.close()
            return results
        except Error as e:
            print(f"✗ Query error: {e}")
            return []
    
    def login(self):
        """User authentication"""
        print("\n" + "="*60)
        print("EQUITY RESEARCH DATABASE - LOGIN")
        print("="*60)
        
        username = input("\nUsername: ")
        password = getpass.getpass("Password: ")
        
        # In production, use proper password hashing (bcrypt)
        # For demo, we'll use simple authentication
        query = """
        SELECT user_id, username, role_type as role 
        FROM Users 
        WHERE username = %s AND is_active = TRUE
        """
        
        result = self.execute_query(query, (username,))
        print(result)
        if result:
            self.current_user = result[0]['username']
            self.user_role = result[0]['role']
            
            # Update last login
            update_query = "UPDATE Users SET last_login = NOW() WHERE username = %s"
            cursor = self.connection.cursor()
            cursor.execute(update_query, (username,))
            self.connection.commit()
            cursor.close()
            
            print(f"\n✓ Welcome {self.current_user} ({self.user_role})")
            return True
        else:
            print("\n✗ Invalid credentials")
            return False
    
    def display_menu(self):
        """Display main menu based on user role"""
        print("\n" + "="*60)
        print(f"MAIN MENU - {self.user_role}")
        print("="*60)
        print("\n1. Company Research")
        print("2. View Stock Prices")
        print("3. Financial Statements")
        print("4. Valuation Metrics")
        print("5. Sector Analysis")
        print("6. View Forecasts")
        print("7. Search Companies")
        
        if self.user_role == 'Admin':
            print("\n--- Admin Functions ---")
            print("8. Database Statistics")
            print("9. User Management")
        
        print("\n0. Logout")
        print("="*60)
    
    def company_research(self):
        """Research company financials"""
        print("\n" + "─"*60)
        print("COMPANY RESEARCH")
        print("─"*60)
        
        ticker = input("\nEnter ticker symbol (e.g., AAPL): ").upper()
        
        # Get company info
        query = """
        SELECT c.*, s.sector_name
        FROM Companies c
        JOIN Sectors s ON c.sector_id = s.sector_id
        WHERE c.ticker_symbol = %s
        """
        
        result = self.execute_query(query, (ticker,))
        
        if not result:
            print(f"\n✗ Company {ticker} not found")
            return
        
        company = result[0]
        
        print(f"\n{'─'*60}")
        print(f"Company: {company['company_name']} ({company['ticker_symbol']})")
        print(f"{'─'*60}")
        print(f"Sector: {company['sector_name']}")
        print(f"Exchange: {company['exchange']}")
        print(f"Country: {company['country']}")
        print(f"Market Cap: ${company['market_cap']:,.0f}" if company['market_cap'] else "Market Cap: N/A")
        
        if company['description']:
            print(f"\nDescription:\n{company['description'][:300]}...")
        
        # Latest stock price
        price_query = """
        SELECT trade_date, close_price, volume
        FROM StockPrices
        WHERE company_id = %s
        ORDER BY trade_date DESC
        LIMIT 1
        """
        
        price = self.execute_query(price_query, (company['company_id'],))
        if price:
            print(f"\n{'─'*60}")
            print(f"Latest Stock Price (as of {price[0]['trade_date']}):")
            print(f"Close: ${price[0]['close_price']:.2f}")
            print(f"Volume: {price[0]['volume']:,}")
        
        # Latest valuation metrics
        metrics_query = """
        SELECT *
        FROM ValuationMetrics
        WHERE company_id = %s
        ORDER BY calculation_date DESC
        LIMIT 1
        """
        
        metrics = self.execute_query(metrics_query, (company['company_id'],))
        if metrics:
            m = metrics[0]
            print(f"\n{'─'*60}")
            print("Valuation Metrics:")
            print(f"P/E Ratio: {m['pe_ratio']:.2f}" if m['pe_ratio'] else "P/E Ratio: N/A")
            print(f"P/B Ratio: {m['pb_ratio']:.2f}" if m['pb_ratio'] else "P/B Ratio: N/A")
            print(f"ROE: {m['roe']:.2f}%" if m['roe'] else "ROE: N/A")
            print(f"Debt-to-Equity: {m['debt_to_equity']:.2f}" if m['debt_to_equity'] else "D/E: N/A")
    
    def view_stock_prices(self):
        """View historical stock prices"""
        print("\n" + "─"*60)
        print("STOCK PRICES")
        print("─"*60)
        
        ticker = input("\nEnter ticker symbol: ").upper()
        days = int(input("Number of days (default 30): ") or "30")
        
        query = """
        SELECT sp.trade_date, sp.open_price, sp.high_price, 
               sp.low_price, sp.close_price, sp.volume
        FROM StockPrices sp
        JOIN Companies c ON sp.company_id = c.company_id
        WHERE c.ticker_symbol = %s
        ORDER BY sp.trade_date DESC
        LIMIT %s
        """
        
        results = self.execute_query(query, (ticker, days))
        
        if results:
            df = pd.DataFrame(results)
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            print(f"\n{ticker} - Last {len(results)} Trading Days:")
            print(tabulate(df.head(20), headers='keys', tablefmt='grid', 
                          showindex=False, floatfmt='.2f'))
            
            # Calculate simple statistics
            print(f"\n{'─'*60}")
            print("Statistics:")
            print(f"Average Close: ${df['close_price'].mean():.2f}")
            print(f"Highest Close: ${df['close_price'].max():.2f}")
            print(f"Lowest Close: ${df['close_price'].min():.2f}")
            print(f"Average Volume: {df['volume'].mean():,.0f}")
        else:
            print(f"\n✗ No price data found for {ticker}")
    
    def view_financial_statements(self):
        """View financial statements"""
        print("\n" + "─"*60)
        print("FINANCIAL STATEMENTS")
        print("─"*60)
        
        ticker = input("\nEnter ticker symbol: ").upper()
        
        print("\nStatement Types:")
        print("1. Income Statement")
        print("2. Balance Sheet")
        print("3. Cash Flow Statement")
        
        choice = input("\nSelect type (1-3): ")
        
        if choice == '1':
            self.view_income_statement(ticker)
        elif choice == '2':
            self.view_balance_sheet(ticker)
        elif choice == '3':
            self.view_cashflow_statement(ticker)
    
    def view_income_statement(self, ticker):
        """Display income statements"""
        query = """
        SELECT fs.fiscal_year, fs.fiscal_quarter, 
               ins.revenue, ins.gross_profit, ins.operating_income, 
               ins.net_income, ins.eps_diluted
        FROM FinancialStatements fs
        JOIN IncomeStatements ins ON fs.statement_id = ins.statement_id
        JOIN Companies c ON fs.company_id = c.company_id
        WHERE c.ticker_symbol = %s
        ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter DESC
        LIMIT 8
        """
        
        results = self.execute_query(query, (ticker,))
        
        if results:
            df = pd.DataFrame(results)
            print(f"\n{ticker} - Income Statements (in millions):")
            
            # Convert to millions for readability
            for col in ['revenue', 'gross_profit', 'operating_income', 'net_income']:
                if col in df.columns:
                    df[col] = df[col] / 1_000_000
            
            print(tabulate(df, headers='keys', tablefmt='grid', 
                          showindex=False, floatfmt='.2f'))
        else:
            print(f"\n✗ No income statement data for {ticker}")
    
    def view_balance_sheet(self, ticker):
        """Display balance sheets"""
        query = """
        SELECT fs.fiscal_year, fs.fiscal_quarter,
               bs.total_assets, bs.current_assets, bs.total_liabilities,
               bs.total_equity
        FROM FinancialStatements fs
        JOIN BalanceSheets bs ON fs.statement_id = bs.statement_id
        JOIN Companies c ON fs.company_id = c.company_id
        WHERE c.ticker_symbol = %s
        ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter DESC
        LIMIT 8
        """
        
        results = self.execute_query(query, (ticker,))
        
        if results:
            df = pd.DataFrame(results)
            print(f"\n{ticker} - Balance Sheets (in millions):")
            
            for col in ['total_assets', 'current_assets', 'total_liabilities', 'total_equity']:
                if col in df.columns:
                    df[col] = df[col] / 1_000_000
            
            print(tabulate(df, headers='keys', tablefmt='grid',
                          showindex=False, floatfmt='.2f'))
        else:
            print(f"\n✗ No balance sheet data for {ticker}")
    
    def cashflow_statement(self, ticker):
        """Display cash flow statements"""
        query = """
        SELECT fs.fiscal_year, fs.fiscal_quarter,
               cfs.operating_cash_flow, cfs.investing_cash_flow,
               cfs.financing_cash_flow, cfs.free_cash_flow
        FROM FinancialStatements fs
        JOIN CashFlowStatements cfs ON fs.statement_id = cfs.statement_id
        JOIN Companies c ON fs.company_id = c.company_id
        WHERE c.ticker_symbol = %s
        ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter DESC
        LIMIT 8
        """
        
        results = self.execute_query(query, (ticker,))
        
        if results:
            df = pd.DataFrame(results)
            print(f"\n{ticker} - Cash Flow Statements (in millions):")
            
            for col in df.columns:
                if col not in ['fiscal_year', 'fiscal_quarter']:
                    df[col] = df[col] / 1_000_000
            
            print(tabulate(df, headers='keys', tablefmt='grid',
                          showindex=False, floatfmt='.2f'))
        else:
            print(f"\n✗ No cash flow data for {ticker}")
    
    def view_valuation_metrics(self):
        """View and compare valuation metrics"""
        print("\n" + "─"*60)
        print("VALUATION METRICS")
        print("─"*60)
        
        query = """
        SELECT c.ticker_symbol, c.company_name, s.sector_name,
               vm.pe_ratio, vm.pb_ratio, vm.roe, vm.debt_to_equity,
               vm.net_margin, vm.calculation_date
        FROM vw_latest_valuations vm
        JOIN Companies c ON vm.ticker_symbol = c.ticker_symbol
        JOIN Sectors s ON c.sector_id = s.sector_id
        ORDER BY s.sector_name, c.ticker_symbol
        """
        
        results = self.execute_query(query, ())
        
        if results:
            df = pd.DataFrame(results)
            print("\nLatest Valuation Metrics:")
            print(tabulate(df, headers='keys', tablefmt='grid',
                          showindex=False, floatfmt='.2f'))
        else:
            print("\n✗ No valuation data available")
    
    def sector_analysis(self):
        """Analyze companies by sector"""
        print("\n" + "─"*60)
        print("SECTOR ANALYSIS")
        print("─"*60)
        
        query = """
        SELECT s.sector_name, COUNT(c.company_id) as company_count,
               AVG(vm.pe_ratio) as avg_pe, AVG(vm.roe) as avg_roe,
               AVG(vm.debt_to_equity) as avg_de
        FROM Sectors s
        LEFT JOIN Companies c ON s.sector_id = c.sector_id
        LEFT JOIN ValuationMetrics vm ON c.company_id = vm.company_id
        WHERE vm.calculation_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        GROUP BY s.sector_id, s.sector_name
        ORDER BY company_count DESC
        """
        
        results = self.execute_query(query, ())
        
        if results:
            df = pd.DataFrame(results)
            print("\nSector Comparison:")
            print(tabulate(df, headers='keys', tablefmt='grid',
                          showindex=False, floatfmt='.2f'))
        else:
            print("\n✗ No sector data available")
    
    def search_companies(self):
        """Search for companies"""
        print("\n" + "─"*60)
        print("SEARCH COMPANIES")
        print("─"*60)
        
        search = input("\nEnter company name or ticker: ")
        
        query = """
        SELECT c.ticker_symbol, c.company_name, s.sector_name, 
               c.market_cap, c.country
        FROM Companies c
        JOIN Sectors s ON c.sector_id = s.sector_id
        WHERE c.ticker_symbol LIKE %s OR c.company_name LIKE %s
        ORDER BY c.market_cap DESC
        """
        
        search_param = f"%{search}%"
        results = self.execute_query(query, (search_param, search_param))
        
        if results:
            df = pd.DataFrame(results)
            print(f"\nSearch Results for '{search}':")
            print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
        else:
            print(f"\n✗ No companies found matching '{search}'")
    
    def database_statistics(self):
        """Show database statistics (Admin only)"""
        if self.user_role != 'Admin':
            print("\n✗ Access denied. Admin privileges required.")
            return
        
        print("\n" + "─"*60)
        print("DATABASE STATISTICS")
        print("─"*60)
        
        stats = {
            'Companies': "SELECT COUNT(*) as count FROM Companies",
            'Stock Prices': "SELECT COUNT(*) as count FROM StockPrices",
            'Income Statements': "SELECT COUNT(*) as count FROM IncomeStatements",
            'Balance Sheets': "SELECT COUNT(*) as count FROM BalanceSheets",
            'Valuation Metrics': "SELECT COUNT(*) as count FROM ValuationMetrics",
            'Users': "SELECT COUNT(*) as count FROM Users WHERE is_active = TRUE"
        }
        
        print()
        for label, query in stats.items():
            result = self.execute_query(query, ())
            count = result[0]['count'] if result else 0
            print(f"{label:.<40} {count:>10,}")
        
        # Latest data update
        query = "SELECT MAX(trade_date) as latest FROM StockPrices"
        result = self.execute_query(query, ())
        if result and result[0]['latest']:
            print(f"\nLatest stock price data: {result[0]['latest']}")
    
    def run(self):
        """Main application loop"""
        if not self.connect():
            return
        
        print("\n" + "="*60)
        print("EQUITY RESEARCH DATABASE & ANALYTICS PLATFORM")
        print("="*60)
        
        if not self.login():
            self.disconnect()
            return
        
        while True:
            self.display_menu()
            choice = input("\nSelect option: ")
            
            if choice == '0':
                print(f"\n✓ Goodbye {self.current_user}!")
                break
            elif choice == '1':
                self.company_research()
            elif choice == '2':
                self.view_stock_prices()
            elif choice == '3':
                self.view_financial_statements()
            elif choice == '4':
                self.view_valuation_metrics()
            elif choice == '5':
                self.sector_analysis()
            elif choice == '6':
                print("\n✗ Forecasts feature coming soon")
            elif choice == '7':
                self.search_companies()
            elif choice == '8' and self.user_role == 'Admin':
                self.database_statistics()
            else:
                print("\n✗ Invalid option")
            
            input("\nPress Enter to continue...")
        
        self.disconnect()


if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'database': 'EquityResearchDB'
    }
    
    app = EquityResearchApp(db_config)
    app.run()
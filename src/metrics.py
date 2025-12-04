"""
Equity Research Database - Streamlit Web Interface
Interactive dashboard for financial data analysis
"""

import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Equity Research Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_connection():
    """Create database connection"""
    return mysql.connector.connect(
        host='localhost',
        user='root',
        database='EquityResearchDB'
    )

@st.cache_data(ttl=300)
def execute_query(query, params=None):
    """Execute SQL query with caching"""
    conn = get_connection()
    df = pd.read_sql(query, conn, params=params)
    return df

# Sidebar navigation
st.sidebar.markdown("# 📈 Equity Research")
st.sidebar.markdown("### Navigation")

page = st.sidebar.radio(
    "Select Page",
    ["Dashboard", "Company Research", "Stock Prices", "Financial Statements", 
     "Valuation Analysis", "Sector Comparison"]
)

# Session state for user
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.role = None

# Authentication
def authenticate(username, password):
    """Simple authentication"""
    query = "SELECT user_id, username, role_type as role FROM Users WHERE username = %s AND is_active = TRUE"
    df = execute_query(query, (username,))
    
    if not df.empty:
        st.session_state.authenticated = True
        st.session_state.username = df.iloc[0]['username']
        st.session_state.role = df.iloc[0]['role']
        return True
    return False

# Login page
if not st.session_state.authenticated:
    st.markdown('<div class="main-header">🔐 Login</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Login")
            
            if submit:
                if authenticate(username, password):
                    st.success(f"Welcome {st.session_state.username}!")
                    st.rerun()
                else:
                    st.error("Invalid credentials")
        
        st.info("Demo credentials: username='analyst1', password='password'")
    
    st.stop()

# Logout button
if st.sidebar.button("Logout"):
    st.session_state.authenticated = False
    st.rerun()

st.sidebar.markdown(f"**User:** {st.session_state.username}")
st.sidebar.markdown(f"**Role:** {st.session_state.role}")

# ====================
# DASHBOARD PAGE
# ====================
if page == "Dashboard":
    st.markdown('<div class="main-header">📊 Dashboard Overview</div>', unsafe_allow_html=True)
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        companies_count = execute_query("SELECT COUNT(*) as count FROM Companies").iloc[0]['count']
        st.metric("Companies", companies_count)
    
    with col2:
        prices_count = execute_query("SELECT COUNT(*) as count FROM StockPrices").iloc[0]['count']
        st.metric("Stock Prices", f"{prices_count:,}")
    
    with col3:
        sectors_count = execute_query("SELECT COUNT(*) as count FROM Sectors").iloc[0]['count']
        st.metric("Sectors", sectors_count)
    
    with col4:
        latest_date = execute_query("SELECT MAX(trade_date) as latest FROM StockPrices").iloc[0]['latest']
        st.metric("Latest Data", latest_date.strftime('%Y-%m-%d') if latest_date else "N/A")
    
    # Latest stock prices
    st.markdown('<div class="section-header">📈 Latest Stock Prices</div>', unsafe_allow_html=True)
    
    query = """
    SELECT c.ticker_symbol, c.company_name, s.sector_name,
           sp.trade_date, sp.close_price, sp.volume
    FROM Companies c
    JOIN Sectors s ON c.sector_id = s.sector_id
    JOIN StockPrices sp ON c.company_id = sp.company_id
    WHERE sp.trade_date = (
        SELECT MAX(trade_date) FROM StockPrices WHERE company_id = c.company_id
    )
    ORDER BY c.market_cap DESC
    """
    
    latest_prices = execute_query(query)
    
    if not latest_prices.empty:
        st.dataframe(
            latest_prices.style.format({
                'close_price': '${:.2f}',
                'volume': '{:,.0f}'
            }),
            use_container_width=True,
            hide_index=True
        )
    
    # Sector distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="section-header">🏢 Companies by Sector</div>', unsafe_allow_html=True)
        
        sector_query = """
        SELECT s.sector_name, COUNT(c.company_id) as count
        FROM Sectors s
        LEFT JOIN Companies c ON s.sector_id = c.sector_id
        GROUP BY s.sector_id, s.sector_name
        ORDER BY count DESC
        """
        
        sector_data = execute_query(sector_query)
        
        if not sector_data.empty:
            fig = px.pie(
                sector_data,
                values='count',
                names='sector_name',
                title='Company Distribution by Sector'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown('<div class="section-header">💰 Average P/E Ratios by Sector</div>', unsafe_allow_html=True)
        
        pe_query = """
        SELECT s.sector_name, AVG(vm.pe_ratio) as avg_pe
        FROM Sectors s
        JOIN Companies c ON s.sector_id = c.sector_id
        JOIN ValuationMetrics vm ON c.company_id = vm.company_id
        WHERE vm.pe_ratio IS NOT NULL
        GROUP BY s.sector_id, s.sector_name
        ORDER BY avg_pe DESC
        """
        
        pe_data = execute_query(pe_query)
        
        if not pe_data.empty:
            fig = px.bar(
                pe_data,
                x='sector_name',
                y='avg_pe',
                title='Average P/E Ratio by Sector',
                labels={'sector_name': 'Sector', 'avg_pe': 'Avg P/E Ratio'}
            )
            st.plotly_chart(fig, use_container_width=True)

# ====================
# COMPANY RESEARCH PAGE
# ====================
elif page == "Company Research":
    st.markdown('<div class="main-header">🔍 Company Research</div>', unsafe_allow_html=True)
    
    # Company selector
    companies = execute_query("SELECT ticker_symbol, company_name FROM Companies ORDER BY ticker_symbol")
    
    selected_ticker = st.selectbox(
        "Select Company",
        companies['ticker_symbol'].tolist(),
        format_func=lambda x: f"{x} - {companies[companies['ticker_symbol']==x]['company_name'].iloc[0]}"
    )
    
    if selected_ticker:
        # Company details
        query = """
        SELECT c.*, s.sector_name
        FROM Companies c
        JOIN Sectors s ON c.sector_id = s.sector_id
        WHERE c.ticker_symbol = %s
        """
        
        company = execute_query(query, (selected_ticker,))
        
        if not company.empty:
            company = company.iloc[0]
            
            st.markdown(f"### {company['company_name']} ({company['ticker_symbol']})")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**Sector:**")
                st.write(company['sector_name'])
                st.markdown("**Exchange:**")
                st.write(company['exchange'])
            
            with col2:
                st.markdown("**Country:**")
                st.write(company['country'])
                st.markdown("**Currency:**")
                st.write(company['currency'])
            
            with col3:
                st.markdown("**Market Cap:**")
                if pd.notna(company['market_cap']):
                    st.write(f"${company['market_cap']:,.0f}")
                else:
                    st.write("N/A")
            
            with st.expander("Company Description"):
                st.write(company['description'])
            
            # Latest metrics
            st.markdown('<div class="section-header">📊 Latest Valuation Metrics</div>', unsafe_allow_html=True)
            
            metrics_query = """
            SELECT * FROM ValuationMetrics
            WHERE company_id = (SELECT company_id FROM Companies WHERE ticker_symbol = %s)
            ORDER BY calculation_date DESC
            LIMIT 1
            """
            
            metrics = execute_query(metrics_query, (selected_ticker,))
            
            if not metrics.empty:
                m = metrics.iloc[0]
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    pe = m['pe_ratio'] if pd.notna(m['pe_ratio']) else 0
                    st.metric("P/E Ratio", f"{pe:.2f}" if pe else "N/A")
                
                with col2:
                    pb = m['pb_ratio'] if pd.notna(m['pb_ratio']) else 0
                    st.metric("P/B Ratio", f"{pb:.2f}" if pb else "N/A")
                
                with col3:
                    roe = m['roe'] if pd.notna(m['roe']) else 0
                    st.metric("ROE", f"{roe:.2f}%" if roe else "N/A")
                
                with col4:
                    de = m['debt_to_equity'] if pd.notna(m['debt_to_equity']) else 0
                    st.metric("Debt/Equity", f"{de:.2f}" if de else "N/A")

# ====================
# STOCK PRICES PAGE
# ====================
elif page == "Stock Prices":
    st.markdown('<div class="main-header">📈 Stock Price Analysis</div>', unsafe_allow_html=True)
    
    companies = execute_query("SELECT ticker_symbol, company_name FROM Companies ORDER BY ticker_symbol")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        selected_ticker = st.selectbox(
            "Select Company",
            companies['ticker_symbol'].tolist(),
            format_func=lambda x: f"{x} - {companies[companies['ticker_symbol']==x]['company_name'].iloc[0]}"
        )
    
    with col2:
        period = st.selectbox("Time Period", ["1 Month", "3 Months", "6 Months", "1 Year", "2 Years"])
    
    if selected_ticker:
        # Get company_id
        company_id = execute_query(
            "SELECT company_id FROM Companies WHERE ticker_symbol = %s",
            (selected_ticker,)
        ).iloc[0]['company_id']
        
        # Map period to days
        period_days = {
            "1 Month": 30,
            "3 Months": 90,
            "6 Months": 180,
            "1 Year": 365,
            "2 Years": 730
        }
        
        days = period_days[period]
        
        # Fetch stock prices
        query = """
        SELECT trade_date, open_price, high_price, low_price, close_price, volume
        FROM StockPrices
        WHERE company_id = %s
        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        ORDER BY trade_date ASC
        """
        
        prices = execute_query(query, (company_id, days))
        
        if not prices.empty:
            # Price chart
            fig = go.Figure()
            
            fig.add_trace(go.Candlestick(
                x=prices['trade_date'],
                open=prices['open_price'],
                high=prices['high_price'],
                low=prices['low_price'],
                close=prices['close_price'],
                name='Price'
            ))
            
            fig.update_layout(
                title=f"{selected_ticker} Stock Price - {period}",
                yaxis_title="Price (USD)",
                xaxis_title="Date",
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Volume chart
            fig_volume = px.bar(
                prices,
                x='trade_date',
                y='volume',
                title=f"{selected_ticker} Trading Volume"
            )
            
            st.plotly_chart(fig_volume, use_container_width=True)
            
            # Statistics
            st.markdown('<div class="section-header">📊 Statistics</div>', unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Current Price", f"${prices['close_price'].iloc[-1]:.2f}")
            
            with col2:
                price_change = prices['close_price'].iloc[-1] - prices['close_price'].iloc[0]
                pct_change = (price_change / prices['close_price'].iloc[0]) * 100
                st.metric("Change", f"${price_change:.2f}", f"{pct_change:.2f}%")
            
            with col3:
                st.metric("Highest", f"${prices['high_price'].max():.2f}")
            
            with col4:
                st.metric("Lowest", f"${prices['low_price'].min():.2f}")

# ====================
# FINANCIAL STATEMENTS PAGE
# ====================
elif page == "Financial Statements":
    st.markdown('<div class="main-header">📄 Financial Statements</div>', unsafe_allow_html=True)
    
    companies = execute_query("SELECT ticker_symbol, company_name FROM Companies ORDER BY ticker_symbol")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        selected_ticker = st.selectbox(
            "Select Company",
            companies['ticker_symbol'].tolist(),
            format_func=lambda x: f"{x} - {companies[companies['ticker_symbol']==x]['company_name'].iloc[0]}"
        )
    
    with col2:
        stmt_type = st.selectbox(
            "Statement Type",
            ["Income Statement", "Balance Sheet", "Cash Flow"]
        )
    
    if selected_ticker:
        company_id = execute_query(
            "SELECT company_id FROM Companies WHERE ticker_symbol = %s",
            (selected_ticker,)
        ).iloc[0]['company_id']
        
        if stmt_type == "Income Statement":
            query = """
            SELECT fs.fiscal_year, fs.fiscal_quarter,
                   ins.revenue, ins.gross_profit, ins.operating_income,
                   ins.net_income, ins.eps_diluted
            FROM FinancialStatements fs
            JOIN IncomeStatements ins ON fs.statement_id = ins.statement_id
            WHERE fs.company_id = %s
            ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter DESC
            LIMIT 12
            """
            
            data = execute_query(query, (company_id,))
            
            if not data.empty:
                # Convert to millions
                for col in ['revenue', 'gross_profit', 'operating_income', 'net_income']:
                    data[col] = data[col] / 1_000_000
                
                st.dataframe(
                    data.style.format({
                        'revenue': '${:,.2f}M',
                        'gross_profit': '${:,.2f}M',
                        'operating_income': '${:,.2f}M',
                        'net_income': '${:,.2f}M',
                        'eps_diluted': '${:.2f}'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Revenue trend
                fig = px.line(
                    data,
                    x='fiscal_quarter',
                    y='revenue',
                    title=f"{selected_ticker} Revenue Trend",
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)

# ====================
# VALUATION ANALYSIS PAGE
# ====================
elif page == "Valuation Analysis":
    st.markdown('<div class="main-header">💹 Valuation Analysis</div>', unsafe_allow_html=True)
    
    query = """
    SELECT c.ticker_symbol, c.company_name, s.sector_name,
           vm.pe_ratio, vm.pb_ratio, vm.roe, vm.debt_to_equity,
           vm.net_margin, vm.calculation_date
    FROM Companies c
    JOIN Sectors s ON c.sector_id = s.sector_id
    JOIN ValuationMetrics vm ON c.company_id = vm.company_id
    WHERE vm.calculation_date = (
        SELECT MAX(calculation_date)
        FROM ValuationMetrics
        WHERE company_id = c.company_id
    )
    ORDER BY c.market_cap DESC
    """
    
    metrics = execute_query(query)
    
    if not metrics.empty:
        st.dataframe(
            metrics.style.format({
                'pe_ratio': '{:.2f}',
                'pb_ratio': '{:.2f}',
                'roe': '{:.2f}%',
                'debt_to_equity': '{:.2f}',
                'net_margin': '{:.2f}%'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # Scatter plot: P/E vs ROE
        fig = px.scatter(
            metrics,
            x='roe',
            y='pe_ratio',
            color='sector_name',
            size='pb_ratio',
            hover_data=['ticker_symbol', 'company_name'],
            title='P/E Ratio vs ROE by Sector',
            labels={'roe': 'Return on Equity (%)', 'pe_ratio': 'P/E Ratio'}
        )
        
        st.plotly_chart(fig, use_container_width=True)

# ====================
# SECTOR COMPARISON PAGE
# ====================
elif page == "Sector Comparison":
    st.markdown('<div class="main-header">🏢 Sector Comparison</div>', unsafe_allow_html=True)
    
    query = """
    SELECT s.sector_name,
           COUNT(c.company_id) as company_count,
           AVG(vm.pe_ratio) as avg_pe,
           AVG(vm.pb_ratio) as avg_pb,
           AVG(vm.roe) as avg_roe,
           AVG(vm.debt_to_equity) as avg_de
    FROM Sectors s
    LEFT JOIN Companies c ON s.sector_id = c.sector_id
    LEFT JOIN ValuationMetrics vm ON c.company_id = vm.company_id
    GROUP BY s.sector_id, s.sector_name
    ORDER BY company_count DESC
    """
    
    sector_data = execute_query(query)
    
    if not sector_data.empty:
        st.dataframe(
            sector_data.style.format({
                'avg_pe': '{:.2f}',
                'avg_pb': '{:.2f}',
                'avg_roe': '{:.2f}%',
                'avg_de': '{:.2f}'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # Comparison charts
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                sector_data,
                x='sector_name',
                y='avg_pe',
                title='Average P/E Ratio by Sector',
                labels={'sector_name': 'Sector', 'avg_pe': 'Avg P/E'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                sector_data,
                x='sector_name',
                y='avg_roe',
                title='Average ROE by Sector',
                labels={'sector_name': 'Sector', 'avg_roe': 'Avg ROE (%)'}
            )
            st.plotly_chart(fig, use_container_width=True)
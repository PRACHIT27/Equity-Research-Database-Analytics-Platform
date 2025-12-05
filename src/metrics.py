"""
Equity Research Database - Streamlit Web Interface
Interactive dashboard for financial data analysis
"""

import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import yfinance as yf
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
        color: #FFFFFFs;
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
        color: #FFFFFF;
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
     "Valuation Analysis", "Forecast Analysis", "Sector Comparison", "Metrics Comparison"]
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

def safe_div(n, d):
    try:
        if d is None or d == 0:
            return None
        return float(n) / float(d)
    except Exception:
        return None

# Login page
if not st.session_state.authenticated:
    st.markdown('<div class="main-header">Login</div>', unsafe_allow_html=True)
    
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


if page == "Dashboard":
    st.markdown('<h2>Dashboard Overview</h2>', unsafe_allow_html=True)
    
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
    st.markdown('### Latest Stock Prices', unsafe_allow_html=True)
    companies_count = execute_query("SELECT COUNT(*) as count FROM Companies").iloc[0]['count']
    metrics_count = execute_query("SELECT COUNT(*) as count FROM ValuationMetrics").iloc[0]['count']
    prices_count = execute_query("SELECT COUNT(*) as count FROM StockPrices").iloc[0]['count']
    
    avg_pe = execute_query("SELECT AVG(pe_ratio) as avg FROM ValuationMetrics WHERE pe_ratio > 0").iloc[0]['avg']
    avg_roe = execute_query("SELECT AVG(roe) as avg FROM ValuationMetrics WHERE roe > 0").iloc[0]['avg']
    st.metric("Stock Prices", f"{prices_count:,}", delta="Real-time")
    with col3:
        st.metric("Avg P/E Ratio", f"{avg_pe:.2f}" if avg_pe else "N/A")
    with col4:
        st.metric("Avg ROE", f"{avg_roe:.1f}%" if avg_roe else "N/A")
    
    # Market Overview Section
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('### Market Performance Overview', unsafe_allow_html=True)
        
        # Get price data for all companies (last 30 days)
        query = """
        SELECT c.ticker_symbol, sp.trade_date, sp.close_price,
               FIRST_VALUE(sp.close_price) OVER (
                   PARTITION BY c.ticker_symbol ORDER BY sp.trade_date
               ) as first_price
        FROM Companies c
        JOIN StockPrices sp ON c.company_id = sp.company_id
        WHERE sp.trade_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
        ORDER BY c.ticker_symbol, sp.trade_date
        """
        
        price_data = execute_query(query)
        
        if not price_data.empty:
            # Calculate percentage change
            price_data['pct_change'] = ((price_data['close_price'] - price_data['first_price']) / 
                                        price_data['first_price'] * 100)
            
            fig = px.line(
                price_data, 
                x='trade_date', 
                y='pct_change',
                color='ticker_symbol',
                title='30-Day Performance (% Change)',
                labels={'pct_change': 'Return (%)', 'trade_date': 'Date'},
                height=700,
                width=1200,
            )
            
            fig.update_layout(
                hovermode='closest',
                title_pad_t = 10,
                legend=dict(orientation="v", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            
            st.plotly_chart(fig, use_container_width=True)
            
    with col2:
        st.markdown("### Top Performers")
        
        query = """
        SELECT 
            c.ticker_symbol,
            c.company_name,
            sp1.close_price as current_price,
            sp2.close_price as price_30d_ago,
            ((sp1.close_price - sp2.close_price) / sp2.close_price * 100) as pct_change
        FROM Companies c
        JOIN StockPrices sp1 ON c.company_id = sp1.company_id
        JOIN StockPrices sp2 ON c.company_id = sp2.company_id
        WHERE sp1.trade_date = (SELECT MAX(trade_date) FROM StockPrices WHERE company_id = c.company_id)
        AND sp2.trade_date = (
            SELECT MAX(trade_date) FROM StockPrices 
            WHERE company_id = c.company_id 
            AND trade_date <= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        )
        ORDER BY pct_change DESC
        """
        
        performers = execute_query(query)
        
        if not performers.empty:
            for _, row in performers.iterrows():
                change_class = "positive" if row['pct_change'] > 0 else "negative"
                st.markdown(f"""
                <div style='padding: 0.5rem; margin: 0.3rem 0; background: #000000; border-radius: 0.5rem;'>
                    <strong>{row['ticker_symbol']}</strong><br>
                    <span class='{change_class}'>{row['pct_change']:+.2f}%</span>
                </div>
                """, unsafe_allow_html=True)
                
        # Valuation Overview
    st.markdown("---")
    st.markdown("### Valuation Metrics Snapshot")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # P/E Ratio distribution
        query = """
        SELECT c.ticker_symbol, vm.pe_ratio
        FROM ValuationMetrics vm
        JOIN Companies c ON vm.company_id = c.company_id
        WHERE vm.pe_ratio IS NOT NULL AND vm.pe_ratio > 0 AND vm.pe_ratio < 100
        """
        pe_data = execute_query(query)
        
        if not pe_data.empty:
            fig = px.bar(
                pe_data.sort_values('pe_ratio'),
                x='ticker_symbol',
                y='pe_ratio',
                title='P/E Ratio Comparison',
                color='pe_ratio',
                color_continuous_scale='RdYlGn_r',
                labels={'pe_ratio': 'P/E Ratio', 'ticker_symbol': 'Company'}
            )
            fig.add_hline(y=pe_data['pe_ratio'].median(), line_dash="dash", 
                         annotation_text="Median", line_color="white")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # ROE vs ROA scatter
        query = """
        SELECT c.ticker_symbol, vm.roe, vm.roa, c.market_cap
        FROM ValuationMetrics vm
        JOIN Companies c ON vm.company_id = c.company_id
        WHERE vm.roe IS NOT NULL AND vm.roa IS NOT NULL
        """
        profitability_data = execute_query(query)
        
        if not profitability_data.empty:
            fig = px.scatter(
                profitability_data,
                x='roa',
                y='roe',
                size='market_cap',
                text='ticker_symbol',
                title='Profitability Matrix: ROE vs ROA',
                labels={'roa': 'ROA (%)', 'roe': 'ROE (%)'},
                color='roe',
                color_continuous_scale='Viridis'
            )
            fig.update_traces(textposition='top center')
            st.plotly_chart(fig, use_container_width=True)
    
    # Sector Analysis
    st.markdown("---")
    st.markdown("### Sector Analysis")
    
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
        st.markdown('<div class="section-header"> Companies by Sector</div>', unsafe_allow_html=True)
        
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
                title='Company Distribution by Sector',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown('<div class="section-header"> Average P/E Ratios by Sector</div>', unsafe_allow_html=True)
        
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


elif page == "Company Research":
    st.markdown('<h2> Company Research</h2>', unsafe_allow_html=True)
    
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
            st.markdown('### Latest Valuation Metrics', unsafe_allow_html=True)
            
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


elif page == "Stock Prices":
    st.markdown('<div class="main-header"> Stock Price Analysis</div>', unsafe_allow_html=True)
    
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
        
        prices = execute_query(query, (str(company_id), days))
        
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
                
        company_info = execute_query(
            "SELECT * FROM Companies c JOIN Sectors s ON c.sector_id = s.sector_id WHERE c.ticker_symbol = %s",
            (selected_ticker,)
        ).iloc[0]
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.markdown(f"## {company_info['company_name']}")
            st.markdown(f"**{company_info['ticker_symbol']}** | {company_info['sector_name']}")
        
        company_info['company_id'] = str(company_info['company_id'])
        
        with col2:
            latest_price = execute_query(
                "SELECT close_price FROM StockPrices WHERE company_id = %s ORDER BY trade_date DESC LIMIT 1",
                (company_info['company_id'],)
            )
            if not latest_price.empty:
                st.metric("Current Price", f"${latest_price.iloc[0]['close_price']:.2f}")
        
        with col3:
            if pd.notna(company_info['market_cap']):
                st.metric("Market Cap", f"${company_info['market_cap']/1e9:.2f}B")
        
        # Tabs for different analyses
        tab1, tab2 = st.tabs(["Overview", "Price Analysis"])
        
        with tab1:
            st.markdown("### Company Overview")
            st.write(company_info['description'])
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Key Information")
                st.markdown(f"""
                - **Exchange:** {company_info['exchange']}
                - **Country:** {company_info['country']}
                - **Currency:** {company_info['currency']}
                - **Incorporation:** {company_info['incorporation_date'] if pd.notna(company_info['incorporation_date']) else 'N/A'}
                """)
            
            with col2:
                # Latest metrics summary
                metrics = execute_query(
                    "SELECT * FROM ValuationMetrics WHERE company_id = %s ORDER BY calculation_date DESC LIMIT 1",
                    (company_info['company_id'],)
                )
                
                if not metrics.empty:
                    m = metrics.iloc[0]
                    st.markdown("#### Latest Metrics")
                    
                    metric_col1, metric_col2 = st.columns(2)
                    with metric_col1:
                        st.metric("P/E Ratio", f"{m['pe_ratio']:.2f}" if pd.notna(m['pe_ratio']) else "N/A")
                        st.metric("ROE", f"{m['roe']:.2f}%" if pd.notna(m['roe']) else "N/A")
                        st.metric("Current Ratio", f"{m['current_ratio']:.2f}" if pd.notna(m['current_ratio']) else "N/A")
                    
                    with metric_col2:
                        st.metric("P/B Ratio", f"{m['pb_ratio']:.2f}" if pd.notna(m['pb_ratio']) else "N/A")
                        st.metric("ROA", f"{m['roa']:.2f}%" if pd.notna(m['roa']) else "N/A")
                        st.metric("Debt/Equity", f"{m['debt_to_equity']:.2f}" if pd.notna(m['debt_to_equity']) else "N/A")
        
        with tab2:
            st.markdown("### Price Analysis")
            
            period = st.select_slider(
                "Time Period",
                options=["1M", "3M", "6M", "1Y", "2Y", "5Y"],
                value="6M"
            )
            
            period_map = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "2Y": 730, "5Y": 1825}
            days = period_map[period]
            
            query = """
            SELECT trade_date, open_price, high_price, low_price, close_price, volume
            FROM StockPrices
            WHERE company_id = %s
            AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY trade_date ASC
            """
            
            prices = execute_query(query, (company_info['company_id'], days))
            
            if not prices.empty:
                # Candlestick chart
                fig = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.03,
                    row_heights=[0.7, 0.3],
                    subplot_titles=(f'{selected_ticker} Stock Price', 'Volume')
                )
                
                fig.add_trace(
                    go.Candlestick(
                        x=prices['trade_date'],
                        open=prices['open_price'],
                        high=prices['high_price'],
                        low=prices['low_price'],
                        close=prices['close_price'],
                        name='Price'
                    ),
                    row=1, col=1
                )
                
                # Add moving averages
                prices['MA20'] = prices['close_price'].rolling(window=20).mean()
                prices['MA50'] = prices['close_price'].rolling(window=50).mean()
                
                fig.add_trace(
                    go.Scatter(x=prices['trade_date'], y=prices['MA20'], 
                              name='20-Day MA', line=dict(color='orange', width=1)),
                    row=1, col=1
                )
                
                fig.add_trace(
                    go.Scatter(x=prices['trade_date'], y=prices['MA50'], 
                              name='50-Day MA', line=dict(color='red', width=1)),
                    row=1, col=1
                )
                
                # Volume bars
                colors = ['red' if row['close_price'] < row['open_price'] else 'green' 
                         for _, row in prices.iterrows()]
                
                fig.add_trace(
                    go.Bar(x=prices['trade_date'], y=prices['volume'], 
                          name='Volume', marker_color=colors, showlegend=False),
                    row=2, col=1
                )
                
                fig.update_layout(
                    height=700,
                    xaxis_rangeslider_visible=False,
                    hovermode='closest'
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Price statistics
                col1, col2, col3, col4 = st.columns(4)


elif page == "Financial Statements":
    st.markdown('<div class="main-header"> Financial Statements</div>', unsafe_allow_html=True)
    
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
            ["Income Statement"]
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
            
            data = execute_query(query, (str(company_id),))
            data = data.dropna()
            print(data)
            
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


elif page == "Valuation Analysis":
    st.markdown('<div class="main-header"> Valuation Analysis</div>', unsafe_allow_html=True)
    
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
        metrics = metrics[['ticker_symbol', 'company_name', 'sector_name', 'pe_ratio', 'roe', 'pb_ratio']].dropna()
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

        # # --- Alpha / Beta / Delta (volatility) analysis ---
        # st.markdown('### ⚖️ Risk & Performance: Alpha, Beta, Delta')
        # # Let user select companies to compute metrics for
        # choices = metrics['ticker_symbol'].tolist()[:12]
        # selected_abd = st.multiselect('Select companies for Alpha/Beta/Delta (up to 10)', choices, default=choices[:6])

        # if selected_abd:
        #     placeholders = ', '.join(['%s'] * len(selected_abd))
        #     price_query = f"""
        #     SELECT c.ticker_symbol, sp.trade_date, sp.adjusted_close as close_price
        #     FROM Companies c
        #     JOIN StockPrices sp ON c.company_id = sp.company_id
        #     WHERE c.ticker_symbol IN ({placeholders})
        #     AND sp.trade_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
        #     ORDER BY c.ticker_symbol, sp.trade_date
        #     """
        #     price_df = execute_query(price_query, tuple(selected_abd))

        #     if price_df.empty:
        #         st.warning('Not enough price data in DB to compute alpha/beta. Ensure StockPrices is populated.')
        #     else:
        #         # pivot to wide format
        #         price_pivot = price_df.pivot(index='trade_date', columns='ticker_symbol', values='close_price')
        #         price_pivot.index = pd.to_datetime(price_pivot.index)
        #         # compute daily returns
        #         returns = price_pivot.pct_change().dropna(how='all')

        #         # fetch market benchmark (S&P 500) using yfinance
        #         end = returns.index.max()
        #         start = returns.index.min()
        #         try:
        #             market = yf.Ticker("^GSPC").history(start=start.strftime('%Y-%m-%d'), end=(end + timedelta(days=1)).strftime('%Y-%m-%d'))
        #             print("Fetched market data from yfinance.")
        #             print(market.head())
        #             print(market.columns)
        #             print(returns.head())
        #             print(f"market.Close[0:5]:\n{market['Close'].head()}")
        #             market_ret = market['Close'].pct_change().reindex(returns.index).dropna()
        #         except Exception as e:
        #             print("Error fetching market data from yfinance.", e)
        #             market = None
        #             market_ret = None

        #         results = []
        #         for ticker in returns.columns:
        #             print(f"Computing ABD for {ticker}")
        #             stock_ret = returns[ticker].dropna()
        #             print(f"Stock returns head:\n{stock_ret.head()}")
        #             print(f"Market returns head:\n{market_ret.head() if market_ret is not None else 'N/A'}")
        #             # align with market
        #             if market_ret is not None and not market_ret.empty:
        #                 aligned = pd.concat([stock_ret, market_ret], axis=1, join='inner').dropna()
        #                 if aligned.shape[0] < 30:
        #                     beta = None
        #                     alpha = None
        #                 else:
        #                     # regression: stock = alpha + beta * market
        #                     try:
        #                         slope, intercept = np.polyfit(aligned.iloc[:,1].values, aligned.iloc[:,0].values, 1)
        #                         beta = float(slope)
        #                         # annualize alpha (daily intercept * 252 trading days)
        #                         alpha = float(intercept) * 252
        #                         print(f"{ticker} - Alpha: {alpha}, Beta: {beta}")
        #                     except Exception as e:
        #                         print(f"Regression failed for {ticker}", e)
        #                         beta = None
        #                         alpha = None
        #             else:
        #                 beta = None
        #                 alpha = None

        #             # Delta = annualized volatility
        #             try:
        #                 delta = float(stock_ret.std() * np.sqrt(252))
        #             except Exception:
        #                 delta = None

        #             results.append({'ticker': ticker, 'alpha': alpha, 'beta': beta, 'delta': delta})

        #         abd_df = pd.DataFrame(results).set_index('ticker')

                # Plot Beta
                # st.markdown('#### Beta (market sensitivity)')
                # figb = px.bar(abd_df.reset_index(), x='ticker', y='beta', title='Beta (slope vs S&P500)')
                # st.plotly_chart(figb, use_container_width=True)

                # # Plot Alpha
                # st.markdown('#### Alpha (annualized excess return)')
                # figa = px.bar(abd_df.reset_index(), x='ticker', y='alpha', title='Alpha (annualized)')
                # st.plotly_chart(figa, use_container_width=True)

                # # Plot Delta (volatility)
                # st.markdown('#### Delta (annualized volatility)')
                # figd = px.bar(abd_df.reset_index(), x='ticker', y='delta', title='Delta (annualized volatility)')
                # st.plotly_chart(figd, use_container_width=True)


elif page == "Forecast Analysis":
    st.markdown('<div class="main-header"> Forecast Analysis & Trading Signals</div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="section-header"> Trading Recommendations Based on Forecasts</div>
    This page visualizes buy/hold/sell signals based on the forecasting model predictions.
    """, unsafe_allow_html=True)
    
    # Get all stocks with forecasts
    query_stocks = """
    SELECT DISTINCT c.company_id, c.ticker_symbol, c.company_name, c.sector_id
    FROM Companies c
    INNER JOIN Forecasts f ON c.company_id = f.company_id
    ORDER BY c.ticker_symbol
    """
    
    stocks_df = execute_query(query_stocks)
    
    if not stocks_df.empty:
        # Stock selector
        selected_stock = st.selectbox(
            "Select Stock",
            stocks_df['ticker_symbol'].values,
            key="forecast_stock"
        )
        
        selected_company = stocks_df[stocks_df['ticker_symbol'] == selected_stock].iloc[0]
        company_id = int(selected_company['company_id'])
        
        # Get forecast data for selected stock
        query_forecasts = """
        SELECT f.forecast_id,
               c.ticker_symbol,
               c.company_name,
               f.forecast_date,
               f.target_date,
               f.target_price,
               f.recommendation,
               f.confidence_score,
               f.eps_forecast,
               f.revenue_forecast,
               sp.close_price,
               ROUND((f.target_price - sp.close_price) / sp.close_price * 100, 2) as expected_return_pct,
               DATEDIFF(f.target_date, f.forecast_date) as forecast_days_ahead
        FROM Forecasts f
        JOIN Companies c ON f.company_id = c.company_id
        LEFT JOIN StockPrices sp ON c.company_id = sp.company_id 
            AND DATE(sp.trade_date) = DATE(f.forecast_date)
        WHERE f.company_id = %s
        ORDER BY f.forecast_date DESC
        LIMIT 100
        """
        
        forecasts = execute_query(query_forecasts, (company_id,))
        
        if not forecasts.empty:
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            latest_forecast = forecasts.iloc[0]
            
            with col1:
                st.metric(
                    "Current Stock Price",
                    f"${latest_forecast['close_price']:.2f}" if latest_forecast['close_price'] else "N/A"
                )
            
            with col2:
                st.metric(
                    "Target Price (30 days)",
                    f"${latest_forecast['target_price']:.2f}" if latest_forecast['target_price'] else "N/A"
                )
            
            with col3:
                expected_return = latest_forecast['expected_return_pct']
                if expected_return is not None:
                    color = "green" if expected_return > 0 else "red"
                    st.metric(
                        "Expected Return",
                        f"{expected_return:.2f}%",
                    )
                else:
                    st.metric("Expected Return", "N/A")
            
            with col4:
                confidence = latest_forecast['confidence_score']
                st.metric(
                    "Confidence Score",
                    f"{confidence:.1%}" if confidence else "N/A"
                )
            
            # Recommendation badge
            rec = latest_forecast['recommendation']
            
            # Color mapping for recommendations
            rec_colors = {
                'Strong Buy': '🟢',
                'Buy': '🟢',
                'Hold': '🟡',
                'Sell': '🔴',
                'Strong Sell': '🔴'
            }
            
            st.markdown(f"""
            <div style="background-color: #f0f2f6; padding: 1.5rem; border-radius: 0.5rem; margin: 1rem 0;">
                <h3 style="margin: 0; color: #2c3e50;">Current Recommendation</h3>
                <h2 style="margin: 0.5rem 0; color: #1f77b4;">
                    {rec_colors.get(rec, '⚪')} {rec}
                </h2>
                <p style="margin: 0.5rem 0; color: #555;">
                    Forecast Date: {latest_forecast['forecast_date'] if pd.notna(latest_forecast['forecast_date']) else 'N/A'} | 
                    Target Date: {latest_forecast['target_date'] if pd.notna(latest_forecast['target_date']) else 'N/A'}
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # Historical forecasts timeline
            st.markdown('<div class="section-header">📈 Forecast History</div>', unsafe_allow_html=True)
            
            # Filter for viewable data
            display_forecasts = forecasts[['forecast_date', 'target_date', 'target_price', 'recommendation', 
                                          'confidence_score', 'expected_return_pct', 'eps_forecast', 
                                          'revenue_forecast']].copy()
            display_forecasts['forecast_date'] = pd.to_datetime(display_forecasts['forecast_date'])
            display_forecasts['target_date'] = pd.to_datetime(display_forecasts['target_date'])
            print(display_forecasts)
            print(display_forecasts.dtypes)
            display_forecasts['forecast_date'] = display_forecasts['forecast_date'].dt.date
            display_forecasts['target_date'] = display_forecasts['target_date'].dt.date
            
            st.dataframe(
                display_forecasts.style.format({
                    'target_price': '${:.2f}',
                    'confidence_score': '{:.1%}',
                    'expected_return_pct': '{:.2f}%',
                    'eps_forecast': '{:.2f}',
                    'revenue_forecast': '${:,.0f}'
                }).background_gradient(
                    subset=['expected_return_pct'],
                    cmap='RdYlGn',
                    vmin=-20,
                    vmax=20
                ),
                use_container_width=True,
                hide_index=True
            )
            
            # Recommendation timeline visualization
            st.markdown('<div class="section-header">⏰ Trading Signal Timeline</div>', unsafe_allow_html=True)
            
            # Prepare data for timeline
            timeline_data = forecasts[['forecast_date', 'recommendation', 'confidence_score', 'expected_return_pct']].copy()
            timeline_data = timeline_data.sort_values('forecast_date')
            
            # Create timeline visualization
            fig_timeline = go.Figure()
            
            # Define colors for recommendations
            color_map = {
                'Strong Buy': '#00cc00',
                'Buy': '#7fff00',
                'Hold': '#ffff00',
                'Sell': '#ff9999',
                'Strong Sell': '#ff0000'
            }
            
            for rec in timeline_data['recommendation'].unique():
                rec_data = timeline_data[timeline_data['recommendation'] == rec]
                
                fig_timeline.add_trace(go.Scatter(
                    x=rec_data['forecast_date'],
                    y=[rec] * len(rec_data),
                    mode='markers',
                    name=rec,
                    marker=dict(
                        size=rec_data['confidence_score'] * 10 + 5,
                        color=color_map.get(rec, '#cccccc'),
                        opacity=0.7,
                        line=dict(width=2, color='white')
                    ),
                    text=[f"<b>{r}</b><br>Confidence: {c:.1%}<br>Expected Return: {e:.2f}%" 
                          for r, c, e in zip(rec_data['recommendation'], 
                                            rec_data['confidence_score'], 
                                            rec_data['expected_return_pct'])],
                    hovertemplate='<b>Forecast Date:</b> %{x}<br>%{text}<extra></extra>'
                ))
            
            fig_timeline.update_layout(
                title=f'{selected_stock} - Trading Signal Timeline',
                xaxis_title='Forecast Date',
                yaxis_title='Recommendation',
                height=400,
                hovermode='closest',
                template='plotly_white'
            )
            
            st.plotly_chart(fig_timeline, use_container_width=True)
            
            # Expected Return Timeline
            st.markdown('<div class="section-header"> Expected Return Over Time</div>', unsafe_allow_html=True)
            
            fig_returns = go.Figure()
            
            fig_returns.add_trace(go.Scatter(
                x=timeline_data['forecast_date'],
                y=timeline_data['expected_return_pct'],
                mode='lines+markers',
                name='Expected Return %',
                fill='tozeroy',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=8),
                hovertemplate='<b>Date:</b> %{x}<br><b>Expected Return:</b> %{y:.2f}%<extra></extra>'
            ))
            
            # Add 0% reference line
            fig_returns.add_hline(y=0, line_dash="dash", line_color="red", opacity=0.5)
            
            fig_returns.update_layout(
                title=f'{selected_stock} - Expected Return Forecast',
                xaxis_title='Forecast Date',
                yaxis_title='Expected Return (%)',
                height=400,
                template='plotly_white'
            )
            
            st.plotly_chart(fig_returns, use_container_width=True)
            
            # Date-Time Action Calendar
            st.markdown('<div class="section-header"> Buy/Hold/Sell Action Calendar</div>', unsafe_allow_html=True)
            
            # Create a detailed action calendar by date
            action_calendar = forecasts[['forecast_date', 'target_date', 'recommendation', 'confidence_score', 
                                        'expected_return_pct', 'target_price']].copy()
            action_calendar = action_calendar.sort_values('forecast_date')
            action_calendar['forecast_date'] = pd.to_datetime(action_calendar['forecast_date'])
            action_calendar['target_date'] = pd.to_datetime(action_calendar['target_date'])
            action_calendar['forecast_date'] = action_calendar['forecast_date'].dt.date
            action_calendar['target_date'] = action_calendar['target_date'].dt.date
            action_calendar['action'] = action_calendar['recommendation']
            
            # Create color mapping for actions
            action_colors = {
                'Strong Buy': '#00cc00',
                'Buy': '#7fff00',
                'Hold': '#ffff00',
                'Sell': '#ff9999',
                'Strong Sell': '#ff0000'
            }
            
            # Display calendar view with HTML table
            st.subheader("Action Calendar by Forecast Date")
            
            # Prepare data for calendar view
            calendar_display = []
            for _, row in action_calendar.iterrows():
                action = row['action']
                color = action_colors.get(action, '#cccccc')
                calendar_display.append({
                    'Forecast Date': row['forecast_date'],
                    'Target Date': row['target_date'],
                    'Action': action,
                    'Confidence': f"{row['confidence_score']:.1%}",
                    'Expected Return': f"{row['expected_return_pct']:.2f}%",
                    'Target Price': f"${row['target_price']:.2f}"
                })
            
            calendar_df = pd.DataFrame(calendar_display)
            
            # Display with color-coded background based on action
            st.dataframe(
                calendar_df.style.applymap(
                    lambda v: f"background-color: {action_colors.get(v, '#ffffff')}; color: black; font-weight: bold;" if v in action_colors else "",
                    subset=['Action']
                ),
                use_container_width=True,
                hide_index=True
            )
            
            # Heatmap style calendar
            st.subheader("Trading Signal Heatmap (Confidence x Date)")
            
            # Create pivot table for heatmap
            heatmap_data = action_calendar.copy()
            heatmap_data['Date'] = heatmap_data['forecast_date'].astype(str)
            heatmap_pivot = heatmap_data.pivot_table(
                values='confidence_score',
                index='action',
                columns='Date',
                aggfunc='first'
            )
            
            # Reorder index based on signal type
            signal_order = ['Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell']
            heatmap_pivot = heatmap_pivot.reindex([s for s in signal_order if s in heatmap_pivot.index])
            
            fig_heatmap = go.Figure(data=go.Heatmap(
                z=heatmap_pivot.values,
                x=heatmap_pivot.columns,
                y=heatmap_pivot.index,
                colorscale=[
                    [0, '#ffffff'],
                    [1, '#0066ff']
                ],
                hovertemplate='<b>Date:</b> %{x}<br><b>Signal:</b> %{y}<br><b>Confidence:</b> %{z:.1%}<extra></extra>',
                colorbar=dict(title='Confidence')
            ))
            
            fig_heatmap.update_layout(
                title=f'{selected_stock} - Trading Signal Confidence Heatmap',
                xaxis_title='Forecast Date',
                yaxis_title='Trading Signal',
                height=400,
                template='plotly_white'
            )
            
            st.plotly_chart(fig_heatmap, use_container_width=True)
            
            # Action timeline with detailed info
            st.subheader("Action Timeline (When to Buy, Hold, Sell)")
            
            # Create detailed timeline with dates
            timeline_html = "<div style='background-color: #f8f9fa; padding: 20px; border-radius: 10px;'>"
            timeline_html += "<h4 style='text-align: center; color: #2c3e50; margin-bottom: 20px;'>Trading Actions by Date</h4>"
            
            for idx, row in action_calendar.iterrows():
                action = row['action']
                color = action_colors.get(action, '#cccccc')
                icon = '🟢' if 'Buy' in action else ('🟡' if action == 'Hold' else '🔴')
                

                timeline_html += f"<div style='background-color: {color}; padding: 12px 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid {color}; opacity: 0.8;'>"
                timeline_html += f"<div style='font-weight: bold; color: #000;'>"
                timeline_html += f"{icon} <b>{action}</b>"
                timeline_html += "</div>"
                timeline_html += "<div style='font-size: 0.9em; color: #333; margin-top: 5px;'>"
                timeline_html += f"<b>Forecast Date:</b> {row['forecast_date']} | <b>Target Date:</b> {row['target_date']}"
                timeline_html += f"</div>"
                timeline_html += "<div style='font-size: 0.9em; color: #333;'>"
                timeline_html += f"<b>Target Price:</b> ${row['target_price']:.2f} |  <b>Expected Return:</b> {row['expected_return_pct']:.2f}% | <b>Confidence:</b> {row['confidence_score']:.1%}"
                timeline_html += "</div>"
                timeline_html += "</div>"
            
            timeline_html += "</div>"
            st.markdown(timeline_html, unsafe_allow_html=True)
            
            # Recommendation distribution
            st.markdown('<div class="section-header"> Recommendation Distribution</div>', unsafe_allow_html=True)
            
            rec_counts = forecasts['recommendation'].value_counts()
            
            fig_rec = px.bar(
                x=rec_counts.index,
                y=rec_counts.values,
                color=rec_counts.index,
                color_discrete_map=color_map,
                title=f'{selected_stock} - Recommendation Distribution',
                labels={'x': 'Recommendation', 'y': 'Count'}
            )
            
            st.plotly_chart(fig_rec, use_container_width=True)
            
            # Summary statistics
            st.markdown('<div class="section-header"> Summary Statistics</div>', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                avg_confidence = forecasts['confidence_score'].mean()
                st.metric("Average Confidence", f"{avg_confidence:.1%}")
            
            with col2:
                avg_return = forecasts['expected_return_pct'].mean()
                st.metric("Average Expected Return", f"{avg_return:.2f}%")
            
            with col3:
                bullish_count = len(forecasts[forecasts['recommendation'].isin(['Strong Buy', 'Buy'])])
                bearish_count = len(forecasts[forecasts['recommendation'].isin(['Sell', 'Strong Sell'])])
                st.metric("Bullish vs Bearish Signals", f"{bullish_count} : {bearish_count}")
        else:
            st.warning(f"No forecasts available for {selected_stock}")
    else:
        st.info("No stocks with forecasts found. Please run the ETL pipeline first.")


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
            
elif page == "Metrics Comparison":
    st.markdown('<div class="main-header">📊 Multi-Company Metrics Comparison</div>', unsafe_allow_html=True)
    
    companies = execute_query("SELECT ticker_symbol, company_name FROM Companies ORDER BY ticker_symbol")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        selected_companies = st.multiselect(
            "Select Companies to Compare (2-6 recommended)",
            companies['ticker_symbol'].tolist(),
            default=companies['ticker_symbol'].tolist()[:3]
        )
    
    with col2:
        comparison_type = st.selectbox(
            "Comparison Type",
            ["Valuation", "Profitability", "Liquidity", "Efficiency", "All Metrics"]
        )
    
    if selected_companies:
        placeholders = ', '.join(['%s'] * len(selected_companies))
        
                # Extended query: include latest income & balance fields to compute efficiency metrics
        query = f"""
                SELECT c.ticker_symbol, c.company_name,
                             vm.pe_ratio, vm.pb_ratio, vm.ps_ratio,
                             vm.roe, vm.roa,
                             vm.debt_to_equity, vm.current_ratio, vm.quick_ratio,
                             vm.gross_margin, vm.operating_margin, vm.net_margin,
                             -- latest income fields
                             (
                                 SELECT ist.revenue FROM IncomeStatements ist
                                 JOIN FinancialStatements fs ON ist.statement_id = fs.statement_id
                                 WHERE fs.company_id = c.company_id AND fs.statement_type='IncomeStatement'
                                 ORDER BY fs.filing_date DESC LIMIT 1
                             ) AS latest_revenue,
                             (
                                 SELECT ist.cost_of_revenue FROM IncomeStatements ist
                                 JOIN FinancialStatements fs ON ist.statement_id = fs.statement_id
                                 WHERE fs.company_id = c.company_id AND fs.statement_type='IncomeStatement'
                                 ORDER BY fs.filing_date DESC LIMIT 1
                             ) AS latest_cost_of_revenue,
                             -- latest balance fields
                             (
                                 SELECT bs.total_assets FROM BalanceSheets bs
                                 JOIN FinancialStatements fs2 ON bs.statement_id = fs2.statement_id
                                 WHERE fs2.company_id = c.company_id AND fs2.statement_type='BalanceSheet'
                                 ORDER BY fs2.filing_date DESC LIMIT 1
                             ) AS latest_total_assets,
                             (
                                 SELECT bs.inventory FROM BalanceSheets bs
                                 JOIN FinancialStatements fs2 ON bs.statement_id = fs2.statement_id
                                 WHERE fs2.company_id = c.company_id AND fs2.statement_type='BalanceSheet'
                                 ORDER BY fs2.filing_date DESC LIMIT 1
                             ) AS latest_inventory,
                             (
                                 SELECT bs.accounts_receivable FROM BalanceSheets bs
                                 JOIN FinancialStatements fs2 ON bs.statement_id = fs2.statement_id
                                 WHERE fs2.company_id = c.company_id AND fs2.statement_type='BalanceSheet'
                                 ORDER BY fs2.filing_date DESC LIMIT 1
                             ) AS latest_accounts_receivable
                FROM Companies c
                JOIN ValuationMetrics vm ON c.company_id = vm.company_id
                WHERE c.ticker_symbol IN ({placeholders})
                """
        
        comparison_data = execute_query(query, tuple(selected_companies))
        
        if not comparison_data.empty:
            if comparison_type == "Valuation":
                # Valuation metrics comparison
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = go.Figure()
                    for _, row in comparison_data.iterrows():
                        fig.add_trace(go.Bar(
                            name=row['ticker_symbol'],
                            x=['P/E', 'P/B', 'P/S'],
                            y=[row['pe_ratio'], row['pb_ratio'], row['ps_ratio']]
                        ))
                    
                    fig.update_layout(
                        title='Valuation Ratios Comparison',
                        barmode='group',
                        height=400
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Scatter plot: P/E vs P/B
                    fig = px.scatter(
                        comparison_data,
                        x='pe_ratio',
                        y='pb_ratio',
                        text='ticker_symbol',
                        size='ps_ratio',
                        color='ticker_symbol',
                        title='P/E vs P/B Ratio',
                        labels={'pe_ratio': 'P/E Ratio', 'pb_ratio': 'P/B Ratio'}
                    )
                    fig.update_traces(textposition='top center')
                    st.plotly_chart(fig, use_container_width=True)
            
            elif comparison_type == "Profitability":
                # Profitability comparison
                df_prof = comparison_data[['ticker_symbol', 'roe', 'roa',
                                           'gross_margin', 'operating_margin', 'net_margin']].copy()
                # Replace missing values with None (Plotly handles None)
                df_prof = df_prof.where(pd.notnull(df_prof), None)

                fig = make_subplots(
                    rows=2, cols=2,
                    subplot_titles=('ROE vs ROA', 'Margin Analysis', 'ROE Comparison', 'Net Margin Comparison'))

                # 1) ROE vs ROA scatter
                for _, r in df_prof.iterrows():
                    fig.add_trace(
                        go.Scatter(
                            x=[r['roe']], y=[r['roa']], mode='markers+text',
                            text=[r['ticker_symbol']], textposition='top center',
                            marker=dict(size=12)
                        ), row=1, col=1
                    )

                # 2) Margin analysis (grouped bars: Gross, Operating, Net)
                margin_x = df_prof['ticker_symbol'].tolist()
                gross_vals = df_prof['gross_margin'].tolist()
                op_vals = df_prof['operating_margin'].tolist()
                net_vals = df_prof['net_margin'].tolist()

                fig.add_trace(go.Bar(name='Gross Margin', x=margin_x, y=gross_vals), row=1, col=2)
                fig.add_trace(go.Bar(name='Operating Margin', x=margin_x, y=op_vals), row=1, col=2)
                fig.add_trace(go.Bar(name='Net Margin', x=margin_x, y=net_vals), row=1, col=2)
                fig.update_xaxes(tickangle= -45, row=1, col=2)

                # 3) ROE Comparison (bar)
                fig.add_trace(go.Bar(name='ROE', x=df_prof['ticker_symbol'], y=df_prof['roe']), row=2, col=1)

                # 4) Net Margin Comparison (bar)
                fig.add_trace(go.Bar(name='Net Margin', x=df_prof['ticker_symbol'], y=df_prof['net_margin']), row=2, col=2)

                fig.update_layout(title='Profitability Comparison', barmode='group', height=700)
                st.plotly_chart(fig, use_container_width=True)

            elif comparison_type == "Liquidity":
                # Liquidity comparison: Current Ratio, Quick Ratio and Debt/Equity (leverage)
                df_liq = comparison_data[['ticker_symbol', 'current_ratio', 'quick_ratio', 'debt_to_equity']].copy()
                df_liq = df_liq.where(pd.notnull(df_liq), None)

                col1, col2 = st.columns(2)

                with col1:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='Current Ratio', x=df_liq['ticker_symbol'], y=df_liq['current_ratio']))
                    fig.add_trace(go.Bar(name='Quick Ratio', x=df_liq['ticker_symbol'], y=df_liq['quick_ratio']))
                    fig.update_layout(title='Liquidity Ratios (Current / Quick)', barmode='group', height=450)
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    # Debt to Equity comparison (higher = more leveraged)
                    fig2 = px.bar(df_liq, x='ticker_symbol', y='debt_to_equity', color='ticker_symbol',
                                  title='Debt / Equity Comparison', labels={'debt_to_equity': 'Debt to Equity'})
                    st.plotly_chart(fig2, use_container_width=True)

            elif comparison_type == "Efficiency":
                # Efficiency metrics: Asset Turnover, Inventory Turnover, Receivables Turnover
                df_eff = comparison_data[['ticker_symbol', 'ps_ratio', 'roa',
                                          'latest_revenue', 'latest_total_assets',
                                          'latest_cost_of_revenue', 'latest_inventory',
                                          'latest_accounts_receivable']].copy()
                # Compute turnovers safely


                df_eff['asset_turnover'] = df_eff.apply(lambda r: safe_div(r['latest_revenue'], r['latest_total_assets']), axis=1)
                df_eff['inventory_turnover'] = df_eff.apply(lambda r: safe_div(r['latest_cost_of_revenue'], r['latest_inventory']), axis=1)
                df_eff['receivables_turnover'] = df_eff.apply(lambda r: safe_div(r['latest_revenue'], r['latest_accounts_receivable']), axis=1)

                # Plot: Turnovers grouped
                fig = go.Figure()
                fig.add_trace(go.Bar(name='Asset Turnover', x=df_eff['ticker_symbol'], y=df_eff['asset_turnover']))
                fig.add_trace(go.Bar(name='Inventory Turnover', x=df_eff['ticker_symbol'], y=df_eff['inventory_turnover']))
                fig.add_trace(go.Bar(name='Receivables Turnover', x=df_eff['ticker_symbol'], y=df_eff['receivables_turnover']))
                fig.update_layout(title='Efficiency Ratios', barmode='group', height=500)
                st.plotly_chart(fig, use_container_width=True)

                # Additional scatter: Asset Turnover vs ROA
                fig2 = px.scatter(df_eff, x='asset_turnover', y='roa', text='ticker_symbol',
                                  title='Asset Turnover vs ROA', labels={'roa':'ROA', 'asset_turnover':'Asset Turnover'})
                fig2.update_traces(textposition='top center')
                st.plotly_chart(fig2, use_container_width=True)

            elif comparison_type == "All Metrics":
                # Combined view: show compact panels for Valuation, Profitability, Liquidity, Efficiency
                st.markdown('### All Metrics Overview')
                # Reuse dataframes from above
                df_val = comparison_data[['ticker_symbol', 'pe_ratio', 'pb_ratio', 'ps_ratio']].copy()
                df_prof = comparison_data[['ticker_symbol', 'roe', 'roa', 'gross_margin', 'operating_margin', 'net_margin']].copy()
                df_liq = comparison_data[['ticker_symbol', 'current_ratio', 'quick_ratio', 'debt_to_equity']].copy()
                df_eff = comparison_data[['ticker_symbol', 'ps_ratio', 'roa',
                                          'latest_revenue', 'latest_total_assets',
                                          'latest_cost_of_revenue', 'latest_inventory',
                                          'latest_accounts_receivable']].copy()

                # compute efficiency turns
                df_eff['asset_turnover'] = df_eff.apply(lambda r: safe_div(r['latest_revenue'], r['latest_total_assets']), axis=1)

                # Layout: 2 rows x 2 cols
                row1col1, row1col2 = st.columns(2)
                with row1col1:
                    fig = go.Figure()
                    for _, r in df_val.iterrows():
                        fig.add_trace(go.Bar(name=r['ticker_symbol'], x=['P/E','P/B','P/S'], y=[r['pe_ratio'], r['pb_ratio'], r['ps_ratio']]))
                    fig.update_layout(title='Valuation Snapshot', barmode='group', height=350)
                    st.plotly_chart(fig, use_container_width=True)

                with row1col2:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='ROE', x=df_prof['ticker_symbol'], y=df_prof['roe']))
                    fig.add_trace(go.Bar(name='Net Margin', x=df_prof['ticker_symbol'], y=df_prof['net_margin']))
                    fig.update_layout(title='Profitability Snapshot', barmode='group', height=350)
                    st.plotly_chart(fig, use_container_width=True)

                row2col1, row2col2 = st.columns(2)
                with row2col1:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='Current Ratio', x=df_liq['ticker_symbol'], y=df_liq['current_ratio']))
                    fig.add_trace(go.Bar(name='Quick Ratio', x=df_liq['ticker_symbol'], y=df_liq['quick_ratio']))
                    fig.update_layout(title='Liquidity Snapshot', barmode='group', height=350)
                    st.plotly_chart(fig, use_container_width=True)

                with row2col2:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='Asset Turnover', x=df_eff['ticker_symbol'], y=df_eff['asset_turnover']))
                    fig.update_layout(title='Efficiency Snapshot', height=350)
                    st.plotly_chart(fig, use_container_width=True)
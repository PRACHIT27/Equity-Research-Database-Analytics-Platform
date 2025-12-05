"""
Equity Research Database - Streamlit Web Interface
Interactive dashboard for financial data analysis
"""

import streamlit as st
import pandas as pd
import mysql.connector
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt
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
     "Valuation Analysis", "Forecast Analysis", "Sector Comparison"]
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

# ====================
# FORECAST ANALYSIS PAGE
# ====================
elif page == "Forecast Analysis":
    st.markdown('<div class="main-header">🔮 Forecast Analysis & Trading Signals</div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="section-header">📊 Trading Recommendations Based on Forecasts</div>
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
            st.markdown('<div class="section-header">📊 Expected Return Over Time</div>', unsafe_allow_html=True)
            
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
            st.markdown('<div class="section-header">📅 Buy/Hold/Sell Action Calendar</div>', unsafe_allow_html=True)
            
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
                
                # timeline_html += f"""
                # <div style='background-color: {color}; padding: 12px 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid {color}; opacity: 0.8;'>
                #     <div style='font-weight: bold; color: #000;'>
                #         {icon} <b>{action}</b>
                #     </div>
                #     <div style='font-size: 0.9em; color: #333; margin-top: 5px;'>
                #         📅 <b>Forecast Date:</b> {row['forecast_date']} | 
                #         🎯 <b>Target Date:</b> {row['target_date']}
                #     </div>
                #     <div style='font-size: 0.9em; color: #333;'>
                #         💰 <b>Target Price:</b> ${row['target_price']:.2f} | 
                #         📈 <b>Expected Return:</b> {row['expected_return_pct']:.2f}% | 
                #         🎯 <b>Confidence:</b> {row['confidence_score']:.1%}
                #     </div>
                # </div>
                # """
                timeline_html += f"<div style='background-color: {color}; padding: 12px 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid {color}; opacity: 0.8;'>"
                timeline_html += f"<div style='font-weight: bold; color: #000;'>"
                timeline_html += f"{icon} <b>{action}</b>"
                timeline_html += "</div>"
                timeline_html += "<div style='font-size: 0.9em; color: #333; margin-top: 5px;'>"
                timeline_html += f"📅 <b>Forecast Date:</b> {row['forecast_date']} | 🎯 <b>Target Date:</b> {row['target_date']}"
                timeline_html += f"</div>"
                timeline_html += "<div style='font-size: 0.9em; color: #333;'>"
                timeline_html += f"💰 <b>Target Price:</b> ${row['target_price']:.2f} | 📈 <b>Expected Return:</b> {row['expected_return_pct']:.2f}% | 🎯 <b>Confidence:</b> {row['confidence_score']:.1%}"
                timeline_html += "</div>"
                timeline_html += "</div>"
            
            timeline_html += "</div>"
            st.markdown(timeline_html, unsafe_allow_html=True)
            
            # Recommendation distribution
            st.markdown('<div class="section-header">🎯 Recommendation Distribution</div>', unsafe_allow_html=True)
            
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
            st.markdown('<div class="section-header">📋 Summary Statistics</div>', unsafe_allow_html=True)
            
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
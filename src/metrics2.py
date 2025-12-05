"""
Enhanced Equity Research Dashboard with Advanced Visualizations
Beautiful, interactive charts and comprehensive analysis views
"""

import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Equity Research Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1f77b4 0%, #ff7f0e 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .positive {
        color: #00d084;
        font-weight: bold;
    }
    .negative {
        color: #ff4444;
        font-weight: bold;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 1rem 2rem;
        font-size: 1.1rem;
    }
</style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_connection():
    return mysql.connector.connect(
        host='localhost',
        user='equity_user',
        password='SecurePassword123!',
        database='equity_research_db'
    )

@st.cache_data(ttl=300)
def execute_query(query, params=None):
    conn = get_connection()
    df = pd.read_sql(query, conn, params=params)
    return df

# Sidebar
st.sidebar.markdown("# 📈 Equity Research")
st.sidebar.markdown("### Navigation")

page = st.sidebar.radio(
    "Select Page",
    ["🏠 Dashboard", "🔍 Company Deep Dive", "📊 Metrics Comparison", 
     "📈 Technical Analysis", "💹 Valuation Heatmap", "🎯 Screening Tool"]
)

# Session state
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = True  # Simplified for demo

st.sidebar.markdown(f"**User:** Analyst")
st.sidebar.markdown(f"**Role:** Premium")

# ====================
# DASHBOARD PAGE
# ====================
if page == "🏠 Dashboard":
    st.markdown('<div class="main-header">📊 Executive Dashboard</div>', unsafe_allow_html=True)
    
    # Key Performance Indicators
    col1, col2, col3, col4, col5 = st.columns(5)
    
    companies_count = execute_query("SELECT COUNT(*) as count FROM Companies").iloc[0]['count']
    metrics_count = execute_query("SELECT COUNT(*) as count FROM ValuationMetrics").iloc[0]['count']
    prices_count = execute_query("SELECT COUNT(*) as count FROM StockPrices").iloc[0]['count']
    
    avg_pe = execute_query("SELECT AVG(pe_ratio) as avg FROM ValuationMetrics WHERE pe_ratio > 0").iloc[0]['avg']
    avg_roe = execute_query("SELECT AVG(roe) as avg FROM ValuationMetrics WHERE roe > 0").iloc[0]['avg']
    
    with col1:
        st.metric("Companies", companies_count, delta="Active")
    with col2:
        st.metric("Stock Prices", f"{prices_count:,}", delta="Real-time")
    with col3:
        st.metric("Avg P/E Ratio", f"{avg_pe:.2f}" if avg_pe else "N/A")
    with col4:
        st.metric("Avg ROE", f"{avg_roe:.1f}%" if avg_roe else "N/A")
    with col5:
        latest_update = execute_query("SELECT MAX(trade_date) as date FROM StockPrices").iloc[0]['date']
        st.metric("Last Update", latest_update.strftime('%m/%d') if latest_update else "N/A")
    
    # Market Overview Section
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📈 Market Performance Overview")
        
        # Get price data for all companies (last 30 days)
        query = """
        SELECT c.ticker_symbol, sp.trade_date, sp.close_price,
               FIRST_VALUE(sp.close_price) OVER (
                   PARTITION BY c.ticker_symbol ORDER BY sp.trade_date
               ) as first_price
        FROM Companies c
        JOIN StockPrices sp ON c.company_id = sp.company_id
        WHERE sp.trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
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
                height=400
            )
            
            fig.update_layout(
                hovermode='x unified',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🎯 Top Performers")
        
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
                <div style='padding: 0.5rem; margin: 0.3rem 0; background: #f0f2f6; border-radius: 0.5rem;'>
                    <strong>{row['ticker_symbol']}</strong><br>
                    <span class='{change_class}'>{row['pct_change']:+.2f}%</span>
                </div>
                """, unsafe_allow_html=True)
    
    # Valuation Overview
    st.markdown("---")
    st.markdown("### 💰 Valuation Metrics Snapshot")
    
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
    st.markdown("### 🏢 Sector Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        query = """
        SELECT s.sector_name, COUNT(c.company_id) as count
        FROM Sectors s
        LEFT JOIN Companies c ON s.sector_id = c.sector_id
        GROUP BY s.sector_id
        ORDER BY count DESC
        """
        sector_dist = execute_query(query)
        
        if not sector_dist.empty:
            fig = px.pie(
                sector_dist,
                values='count',
                names='sector_name',
                title='Company Distribution by Sector',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        query = """
        SELECT s.sector_name, 
               AVG(vm.pe_ratio) as avg_pe,
               AVG(vm.roe) as avg_roe,
               AVG(vm.debt_to_equity) as avg_de
        FROM Sectors s
        JOIN Companies c ON s.sector_id = c.sector_id
        JOIN ValuationMetrics vm ON c.company_id = vm.company_id
        WHERE vm.pe_ratio IS NOT NULL
        GROUP BY s.sector_id
        """
        sector_metrics = execute_query(query)
        
        if not sector_metrics.empty:
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='Avg P/E',
                x=sector_metrics['sector_name'],
                y=sector_metrics['avg_pe'],
                marker_color='lightblue'
            ))
            
            fig.add_trace(go.Bar(
                name='Avg ROE',
                x=sector_metrics['sector_name'],
                y=sector_metrics['avg_roe'],
                marker_color='lightgreen'
            ))
            
            fig.update_layout(
                title='Average Metrics by Sector',
                barmode='group',
                xaxis_tickangle=-45,
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)

# ====================
# COMPANY DEEP DIVE
# ====================
elif page == "🔍 Company Deep Dive":
    st.markdown('<div class="main-header">🔍 Company Deep Dive Analysis</div>', unsafe_allow_html=True)
    
    companies = execute_query("SELECT ticker_symbol, company_name FROM Companies ORDER BY ticker_symbol")
    
    selected_ticker = st.selectbox(
        "Select Company",
        companies['ticker_symbol'].tolist(),
        format_func=lambda x: f"{x} - {companies[companies['ticker_symbol']==x]['company_name'].iloc[0]}"
    )
    
    if selected_ticker:
        # Company header
        company_info = execute_query(
            "SELECT * FROM Companies c JOIN Sectors s ON c.sector_id = s.sector_id WHERE c.ticker_symbol = %s",
            (selected_ticker,)
        ).iloc[0]
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.markdown(f"## {company_info['company_name']}")
            st.markdown(f"**{company_info['ticker_symbol']}** | {company_info['sector_name']}")
        
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
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Price Analysis", "💰 Financials", "🎯 Valuation"])
        
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
                options=["1M", "3M", "6M", "1Y", "2Y"],
                value="6M"
            )
            
            period_map = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "2Y": 730}
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
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Price statistics
                col1, col2, col3, col4 = st.columns(4)
                
                price_change = prices['close_price'].iloc[-1] - prices['close_price'].iloc[0]
                pct_change = (price_change / prices['close_price'].iloc[0]) * 100
                
                with col1:
                    st.metric("Period Change", f"${price_change:.2f}", f"{pct_change:+.2f}%")
                with col2:
                    st.metric("Highest", f"${prices['high_price'].max():.2f}")
                with col3:
                    st.metric("Lowest", f"${prices['low_price'].min():.2f}")
                with col4:
                    st.metric("Avg Volume", f"{prices['volume'].mean():,.0f}")
        
        with tab3:
            st.markdown("### Financial Statements")
            
            # Income statement trend
            query = """
            SELECT fs.fiscal_year, fs.fiscal_quarter,
                   ins.revenue, ins.gross_profit, ins.operating_income, ins.net_income
            FROM FinancialStatements fs
            JOIN IncomeStatements ins ON fs.statement_id = ins.statement_id
            WHERE fs.company_id = %s
            ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter DESC
            LIMIT 8
            """
            
            income_data = execute_query(query, (company_info['company_id'],))
            
            if not income_data.empty:
                # Convert to millions
                for col in ['revenue', 'gross_profit', 'operating_income', 'net_income']:
                    if col in income_data.columns:
                        income_data[col] = pd.to_numeric(income_data[col], errors='coerce') / 1_000_000
                
                income_data['period'] = income_data['fiscal_year'].astype(str) + '-' + income_data['fiscal_quarter'].astype(str)
                income_data = income_data.iloc[::-1]  # Reverse for chronological order
                
                fig = go.Figure()
                
                fig.add_trace(go.Bar(
                    name='Revenue',
                    x=income_data['period'],
                    y=income_data['revenue'],
                    marker_color='lightblue'
                ))
                
                fig.add_trace(go.Scatter(
                    name='Net Income',
                    x=income_data['period'],
                    y=income_data['net_income'],
                    line=dict(color='green', width=3),
                    mode='lines+markers'
                ))
                
                fig.update_layout(
                    title='Revenue and Net Income Trend',
                    xaxis_title='Period',
                    yaxis_title='Amount ($M)',
                    hovermode='x unified',
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        with tab4:
            st.markdown("### Valuation Analysis")
            
            metrics = execute_query(
                "SELECT * FROM ValuationMetrics WHERE company_id = %s ORDER BY calculation_date DESC LIMIT 1",
                (company_info['company_id'],)
            )
            
            if not metrics.empty:
                m = metrics.iloc[0]
                
                # Radar chart for metrics
                categories = []
                values = []
                
                metric_map = {
                    'ROE': ('roe', 20, 'higher'),
                    'ROA': ('roa', 10, 'higher'),
                    'Current Ratio': ('current_ratio', 2, 'optimal'),
                    'Quick Ratio': ('quick_ratio', 1.5, 'optimal'),
                    'Gross Margin': ('gross_margin', 40, 'higher'),
                    'Operating Margin': ('operating_margin', 20, 'higher'),
                    'Net Margin': ('net_margin', 15, 'higher')
                }
                
                for name, (col, benchmark, _) in metric_map.items():
                    if pd.notna(m[col]):
                        categories.append(name)
                        # Normalize to 0-100 scale
                        normalized = min((m[col] / benchmark) * 100, 100)
                        values.append(normalized)
                
                if categories:
                    fig = go.Figure()
                    
                    fig.add_trace(go.Scatterpolar(
                        r=values,
                        theta=categories,
                        fill='toself',
                        name=selected_ticker,
                        line_color='rgb(0, 123, 255)'
                    ))
                    
                    fig.add_trace(go.Scatterpolar(
                        r=[100] * len(categories),
                        theta=categories,
                        fill='toself',
                        name='Benchmark',
                        line_color='rgba(255, 0, 0, 0.3)',
                        fillcolor='rgba(255, 0, 0, 0.1)'
                    ))
                    
                    fig.update_layout(
                        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                        showlegend=True,
                        title=f'{selected_ticker} Performance Radar',
                        height=500
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                # Detailed metrics table
                st.markdown("#### Detailed Metrics")
                
                metrics_display = pd.DataFrame({
                    'Metric': ['P/E Ratio', 'P/B Ratio', 'P/S Ratio', 'ROE', 'ROA', 
                              'Debt-to-Equity', 'Current Ratio', 'Quick Ratio',
                              'Gross Margin', 'Operating Margin', 'Net Margin'],
                    'Value': [
                        f"{m['pe_ratio']:.2f}" if pd.notna(m['pe_ratio']) else 'N/A',
                        f"{m['pb_ratio']:.2f}" if pd.notna(m['pb_ratio']) else 'N/A',
                        f"{m['ps_ratio']:.2f}" if pd.notna(m['ps_ratio']) else 'N/A',
                        f"{m['roe']:.2f}%" if pd.notna(m['roe']) else 'N/A',
                        f"{m['roa']:.2f}%" if pd.notna(m['roa']) else 'N/A',
                        f"{m['debt_to_equity']:.2f}" if pd.notna(m['debt_to_equity']) else 'N/A',
                        f"{m['current_ratio']:.2f}" if pd.notna(m['current_ratio']) else 'N/A',
                        f"{m['quick_ratio']:.2f}" if pd.notna(m['quick_ratio']) else 'N/A',
                        f"{m['gross_margin']:.2f}%" if pd.notna(m['gross_margin']) else 'N/A',
                        f"{m['operating_margin']:.2f}%" if pd.notna(m['operating_margin']) else 'N/A',
                        f"{m['net_margin']:.2f}%" if pd.notna(m['net_margin']) else 'N/A'
                    ]
                })
                
                st.dataframe(metrics_display, use_container_width=True, hide_index=True)

# ====================
# METRICS COMPARISON
# ====================
elif page == "📊 Metrics Comparison":
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
        
        query = f"""
        SELECT c.ticker_symbol, c.company_name,
               vm.pe_ratio, vm.pb_ratio, vm.ps_ratio,
               vm.roe, vm.roa,
               vm.debt_to_equity, vm.current_ratio, vm.quick_ratio,
               vm.gross_margin, vm.operating_margin, vm.net_margin
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
                fig = make_subplots(
                    rows=2, cols=2,
                    subplot_titles=('ROE vs ROA', 'Margin Analysis', 'ROE Comparison', 'Net Margin Comparison'))
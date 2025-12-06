"""
Dashboard Page Module
Complete implementation of friend's dashboard features
ARCHITECTURE: UI → Controllers → Services → Repositories → Database
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

def show_dashboard(controllers):
    """
    Display dashboard - ALL of friend's features using your controllers.

    Args:
        controllers: Dictionary of controller instances
    """
    st.markdown('<h2>Dashboard Overview</h2>', unsafe_allow_html=True)

    try:
        # Get data through controllers
        companies = controllers['company'].get_all_companies()
        latest_prices = controllers['price'].get_latest_prices()
        sectors = controllers['company'].get_all_sectors()
        valuation_metrics = controllers['financial'].get_all_valuation_metrics()

        # ========== ROW 1: KEY METRICS (4 Cards) ==========
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            companies_count = len(companies) if companies else 0
            st.metric("Companies", companies_count)

        with col2:
            prices_count = len(latest_prices) if latest_prices else 0
            st.metric("Stock Prices", f"{prices_count:,}")

        with col3:
            sectors_count = len(sectors) if sectors else 0
            st.metric("Sectors", sectors_count)

        with col4:
            if latest_prices:
                df_prices = pd.DataFrame(latest_prices)
                latest_date = pd.to_datetime(df_prices['trade_date']).max()
                st.metric("Latest Data", latest_date.strftime('%Y-%m-%d') if pd.notna(latest_date) else "N/A")
            else:
                st.metric("Latest Data", "N/A")

        # ========== LATEST STOCK PRICES TABLE ==========
        st.markdown('### Latest Stock Prices')

        if latest_prices:
            df_prices = pd.DataFrame(latest_prices)

            display_cols = ['ticker_symbol', 'company_name', 'sector_name',
                            'trade_date', 'close_price', 'volume']
            available_cols = [col for col in display_cols if col in df_prices.columns]

            df_display = df_prices[available_cols].copy()
            if 'trade_date' in df_display.columns:
                df_display = df_display.rename(columns={'trade_date': 'trade_date'})

            # Sort by market cap (need to merge with companies)
            companies_dict = {c['company_id']: c.get('market_cap', 0) for c in companies}
            df_display['market_cap'] = df_display['company_id'].map(companies_dict) if 'company_id' in df_display.columns else 0
            df_display = df_display.sort_values('market_cap', ascending=False)

            # Display without market_cap column
            display_final = [col for col in ['ticker_symbol', 'company_name', 'sector_name',
                                             'trade_date', 'close_price', 'volume'] if col in df_display.columns]

            st.dataframe(
                df_display[display_final],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "close_price": st.column_config.NumberColumn("Close Price", format="$%.2f"),
                    "volume": st.column_config.NumberColumn("Volume", format="%d"),
                }
            )
        else:
            st.info("No price data available")

        # ========== ROW 2: SECTOR CHARTS ==========
        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('### Companies by Sector')

            if companies:
                df_companies = pd.DataFrame(companies)
                sector_counts = df_companies.groupby('sector_name').size().reset_index(name='count')
                sector_counts = sector_counts.sort_values('count', ascending=False)

                fig = px.pie(
                    sector_counts,
                    values='count',
                    names='sector_name',
                    title='Company Distribution by Sector',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No company data available")

        with col2:
            st.markdown('### Average P/E Ratios by Sector')

            if valuation_metrics:
                sector_pe = controllers['financial'].get_sector_valuation_averages()

                if sector_pe:
                    df_pe = pd.DataFrame(sector_pe)

                    if 'avg_pe_ratio' in df_pe.columns:
                        df_pe = df_pe.dropna(subset=['avg_pe_ratio'])
                        df_pe = df_pe.sort_values('avg_pe_ratio', ascending=False)

                        if not df_pe.empty:
                            fig = px.bar(
                                df_pe,
                                x='sector_name',
                                y='avg_pe_ratio',
                                title='Average P/E Ratio by Sector',
                                labels={'sector_name': 'Sector', 'avg_pe_ratio': 'Avg P/E Ratio'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No P/E data available")
                    else:
                        st.info("No P/E data available")
                else:
                    st.info("No sector valuation data available")
            else:
                st.info("No valuation metrics available")

        # ========== MARKET PERFORMANCE OVERVIEW (30-Day Chart) ==========
        st.markdown("---")
        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown('### Market Performance Overview')

            # Get 30-day performance for top companies
            if companies and len(companies) > 0:
                performance_data = []

                # Use top 10 companies by market cap
                df_companies = pd.DataFrame(companies)
                if 'market_cap' in df_companies.columns:
                    df_companies = df_companies.dropna(subset=['market_cap'])
                    df_companies['market_cap'] = pd.to_numeric(df_companies['market_cap'], errors='coerce')
                    top_companies = df_companies.nlargest(10, 'market_cap')
                else:
                    top_companies = df_companies.head(10)

                for _, company in top_companies.iterrows():
                    company_id = company['company_id']
                    ticker = company['ticker_symbol']

                    # Get 365-day price history (for friend's feature)
                    end_date = datetime.now().date()
                    start_date = end_date - timedelta(days=365)

                    prices = controllers['price'].get_price_history(company_id, start_date, end_date)

                    print(prices)

                    if prices and len(prices) > 1:
                        df_prices = pd.DataFrame(prices)
                        df_prices['trade_date'] = pd.to_datetime(df_prices['trade_date'])
                        df_prices = df_prices.sort_values('trade_date')

                        # Calculate percentage change from first price
                        first_price = df_prices['close_price'].iloc[0]

                        for _, row in df_prices.iterrows():
                            pct_change = ((row['close_price'] - first_price) / first_price) * 100
                            performance_data.append({
                                'ticker_symbol': ticker,
                                'trade_date': row['trade_date'],
                                'pct_change': pct_change,
                                'close_price': row['close_price'],
                                'first_price': first_price
                            })

                if performance_data:
                    df_perf = pd.DataFrame(performance_data)

                    fig = px.line(
                        df_perf,
                        x='trade_date',
                        y='pct_change',
                        color='ticker_symbol',
                        title='30-Day Performance (% Change)',
                        labels={'pct_change': 'Return (%)', 'trade_date': 'Date'},
                        height=400
                    )

                    fig.update_layout(
                        hovermode='closest',
                        legend=dict(orientation="v", yanchor="top", y=0.99, xanchor="left", x=0.01)
                    )
                    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Not enough price data for performance analysis")
            else:
                st.info("No companies available")

        with col2:
            st.markdown("### Top Performers")

            # Get top performers through controller (30-day)
            try:
                performers = controllers['analytics'].get_top_performer(30, 10)

                if performers:
                    for perf in performers:
                        ticker = perf.get('ticker_symbol', 'N/A')
                        change_pct = perf.get('return_pct', 0)

                        change_class = "positive" if change_pct > 0 else "negative"
                        change_color = "#00cc00" if change_pct > 0 else "#ff0000"

                        st.markdown(f"""
                        <div style='padding: 0.5rem; margin: 0.3rem 0; background: #000000; border-radius: 0.5rem;'>
                            <strong style='color: white;'>{ticker}</strong><br>
                            <span style='color: {change_color}; font-weight: bold;'>{change_pct:+.2f}%</span>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.info("No performance data available")
            except:
                # Fallback: show top companies by market cap
                if companies:
                    df_companies = pd.DataFrame(companies)
                    if 'market_cap' in df_companies.columns:
                        df_companies['market_cap'] = pd.to_numeric(df_companies['market_cap'], errors='coerce')
                        top_companies = df_companies.nlargest(10, 'market_cap')
                        for _, comp in top_companies.iterrows():
                            st.markdown(f"""
                            <div style='padding: 0.5rem; margin: 0.3rem 0; background: #1e1e1e; border-radius: 0.5rem;'>
                                <strong style='color: white;'>{comp['ticker_symbol']}</strong><br>
                                <span style='color: #888;'>${comp['market_cap']:,.0f}M</span>
                            </div>
                            """, unsafe_allow_html=True)

        # ========== VALUATION OVERVIEW ==========
        st.markdown("---")
        st.markdown("### Valuation Metrics Snapshot")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### P/E Ratio Distribution")

            if valuation_metrics:
                df_metrics = pd.DataFrame(valuation_metrics)

                # Filter valid P/E ratios
                pe_data = df_metrics[
                    (df_metrics['pe_ratio'].notna()) &
                    (df_metrics['pe_ratio'] > 0) &
                    (df_metrics['pe_ratio'] < 100)
                    ].copy()

                if not pe_data.empty:
                    pe_data = pe_data.sort_values('pe_ratio')

                    fig = px.bar(
                        pe_data,
                        x='ticker_symbol',
                        y='pe_ratio',
                        title='P/E Ratio Comparison',
                        color='pe_ratio',
                        color_continuous_scale='RdYlGn_r',
                        labels={'pe_ratio': 'P/E Ratio', 'ticker_symbol': 'Company'}
                    )

                    median_pe = pe_data['pe_ratio'].median()
                    fig.add_hline(y=median_pe, line_dash="dash",
                                  annotation_text="Median", line_color="white")

                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No P/E ratio data available")
            else:
                st.info("No valuation metrics available")

        with col2:
            st.markdown("#### ROE vs ROA Scatter")

            if valuation_metrics:
                df_metrics = pd.DataFrame(valuation_metrics)

                # Get companies for market cap
                companies_dict = {c['company_id']: c for c in companies}

                prof_data = df_metrics[
                    (df_metrics['roe'].notna()) &
                    (df_metrics['roa'].notna())
                    ].copy()

                if not prof_data.empty:
                    # Add market cap
                    prof_data['market_cap'] = prof_data['company_id'].map(
                        lambda x: companies_dict.get(x, {}).get('market_cap', 0)
                    )

                    # 🔑 CRITICAL FIX: Explicitly coerce the market_cap column to numeric
                    prof_data['market_cap'] = pd.to_numeric(prof_data['market_cap'], errors='coerce')

                    # Also, drop any rows where market_cap became NaN after coercion
                    prof_data = prof_data.dropna(subset=['market_cap'])

                    if not prof_data.empty:
                        fig = px.scatter(
                            prof_data,
                            x='roa',
                            y='roe',
                            size='market_cap',
                            text='ticker_symbol',
                            title='Profitability Matrix: ROE vs ROA',
                            labels={'roa': 'ROA', 'roe': 'ROE'},
                            color='roe',
                            color_continuous_scale='Viridis'
                        )

                        fig.update_traces(textposition='top center')
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No profitability data after cleaning market cap")
                else:
                    st.info("No profitability data available")
            else:
                st.info("No valuation metrics available")

        # ========== SECTOR ANALYSIS ==========
        st.markdown("---")
        st.markdown("### Sector Analysis")

        # Latest prices table (sorted by market cap)
        if latest_prices:
            df_prices = pd.DataFrame(latest_prices)

            display_cols = ['ticker_symbol', 'company_name', 'sector_name',
                            'trade_date', 'close_price', 'volume']
            available_cols = [col for col in display_cols if col in df_prices.columns]

            df_display = df_prices[available_cols].copy()
            if 'trade_date' in df_display.columns:
                df_display = df_display.rename(columns={'trade_date': 'trade_date'})

            # Add market cap for sorting
            companies_dict = {c['company_id']: c.get('market_cap', 0) for c in companies}
            if 'company_id' in df_prices.columns:
                df_display['market_cap'] = df_prices['company_id'].map(companies_dict)
                df_display = df_display.sort_values('market_cap', ascending=False)
                df_display = df_display.drop('market_cap', axis=1)

            st.dataframe(
                df_display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "close_price": st.column_config.NumberColumn("Close Price", format="$%.2f"),
                    "volume": st.column_config.NumberColumn("Volume", format="%d"),
                }
            )

        # ========== SECTOR DISTRIBUTION CHARTS ==========
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('### Companies by Sector')

            if companies:
                df = pd.DataFrame(companies)
                sector_counts = df.groupby('sector_name').size().reset_index(name='count')
                sector_counts = sector_counts.sort_values('count', ascending=False)

                fig = px.pie(
                    sector_counts,
                    values='count',
                    names='sector_name',
                    title='Company Distribution by Sector',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig, use_container_width=True, key='sector_pie_chart_final')
            else:
                st.info("No company data available")


        with col2:
                st.markdown('### Average P/E Ratios by Sector')

                if valuation_metrics:
                    sector_pe = controllers['financial'].get_sector_valuation_averages()

                    if sector_pe:
                        df_pe = pd.DataFrame(sector_pe)

                        if 'avg_pe_ratio' in df_pe.columns:
                            df_pe = df_pe.dropna(subset=['avg_pe_ratio'])
                            df_pe = df_pe.sort_values('avg_pe_ratio', ascending=False)

                            if not df_pe.empty:
                                fig = px.bar(
                                    df_pe,
                                    x='sector_name',
                                    y='avg_pe_ratio',
                                    title='Average P/E Ratio by Sector',
                                    labels={'sector_name': 'Sector', 'avg_pe_ratio': 'Avg P/E Ratio'}
                                )
                                st.plotly_chart(fig, use_container_width=True, key='dashboard_sector_pe_bar')
                            else:
                                st.info("No P/E data available")
                        else:
                            st.info("No P/E data available")
                    else:
                        st.info("No sector valuation data available")
                else:
                    st.info("No valuation metrics available")

    except Exception as e:
        st.error(f"❌ Error loading dashboard: {e}")
        import traceback
        with st.expander("Show Error Details"):
            st.code(traceback.format_exc())
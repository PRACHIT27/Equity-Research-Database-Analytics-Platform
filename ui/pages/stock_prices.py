"""
Stock Prices Page Module
Friend's COMPLETE UI features with proper architecture
ARCHITECTURE: UI → Controllers → Services → Repositories → Database
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta

def show_stock_prices(controllers, permissions):
    """
    Display stock price analysis page - Friend's EXACT features.

    Args:
        controllers: Dictionary of controller instances
        permissions: User permissions dictionary
    """
    st.markdown('<div class="main-header">📈 Stock Price Analysis</div>', unsafe_allow_html=True)

    try:
        # Get companies through controller
        companies = controllers['company'].get_all_companies()

        if not companies:
            st.warning("⚠️ No companies available")
            return

        # ========== COMPANY AND PERIOD SELECTORS ==========
        col1, col2 = st.columns([2, 1])

        with col1:
            company_dict = {c['ticker_symbol']: c for c in companies}

            selected_ticker = st.selectbox(
                "Select Company",
                sorted(company_dict.keys()),
                format_func=lambda x: f"{x} - {company_dict[x]['company_name']}"
            )

        with col2:
            period = st.selectbox(
                "Time Period",
                ["1 Month", "3 Months", "6 Months", "1 Year", "2 Years"]
            )

        if selected_ticker:
            company = company_dict[selected_ticker]
            company_id = company['company_id']

            # Map period to days
            period_days = {
                "1 Month": 30,
                "3 Months": 90,
                "6 Months": 180,
                "1 Year": 365,
                "2 Years": 730
            }

            days = period_days[period]

            # ========== FETCH PRICE DATA THROUGH CONTROLLER ==========
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            prices = controllers['price'].get_price_history(company_id, start_date, end_date)

            if prices:
                df_prices = pd.DataFrame(prices)
                df_prices['trade_date'] = pd.to_datetime(df_prices['trade_date'])
                df_prices = df_prices.sort_values('trade_date')

                # Use trade_date (friend's column name)
                df_prices['trade_date'] = df_prices['trade_date']

                # ========== CANDLESTICK CHART ==========
                fig = go.Figure()

                fig.add_trace(go.Candlestick(
                    x=df_prices['trade_date'],
                    open=df_prices['open_price'],
                    high=df_prices['high_price'],
                    low=df_prices['low_price'],
                    close=df_prices['close_price'],
                    name='Price'
                ))

                fig.update_layout(
                    title=f"{selected_ticker} Stock Price - {period}",
                    yaxis_title="Price (USD)",
                    xaxis_title="Date",
                    height=500
                )

                st.plotly_chart(fig, use_container_width=True, key='stock_price_candlestick_main')

                # ========== VOLUME CHART ==========
                fig_volume = px.bar(
                    df_prices,
                    x='trade_date',
                    y='volume',
                    title=f"{selected_ticker} Trading Volume"
                )

                st.plotly_chart(fig_volume, use_container_width=True, key='stock_volume_bar_main')

                # ========== STATISTICS ==========
                st.markdown('<div class="section-header">📊 Statistics</div>', unsafe_allow_html=True)

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Current Price", f"${df_prices['close_price'].iloc[-1]:.2f}")

                with col2:
                    price_change = df_prices['close_price'].iloc[-1] - df_prices['close_price'].iloc[0]
                    pct_change = (price_change / df_prices['close_price'].iloc[0]) * 100
                    st.metric("Change", f"${price_change:.2f}", f"{pct_change:.2f}%")

                with col3:
                    st.metric("Highest", f"${df_prices['high_price'].max():.2f}")

                with col4:
                    st.metric("Lowest", f"${df_prices['low_price'].min():.2f}")

            else:
                st.info(f"No price data available for {selected_ticker}")

            # ========== COMPANY INFO SECTION ==========
            st.markdown("---")

            col1, col2, col3 = st.columns([2, 1, 1])

            with col1:
                st.markdown(f"## {company['company_name']}")
                st.markdown(f"**{company['ticker_symbol']}** | {company.get('sector_name', 'N/A')}")

            with col2:
                # Latest price
                if prices:
                    latest_price = df_prices['close_price'].iloc[-1]
                    st.metric("Current Price", f"${latest_price:.2f}")
                else:
                    st.metric("Current Price", "N/A")

            with col3:
                if company.get('market_cap'):
                    market_cap_b = company['market_cap'] / 1000  # Convert M to B
                    st.metric("Market Cap", f"${market_cap_b:.2f}B")
                else:
                    st.metric("Market Cap", "N/A")

            # ========== TABS: OVERVIEW & PRICE ANALYSIS ==========
            tab1, tab2 = st.tabs(["Overview", "Price Analysis"])

            with tab1:
                st.markdown("### Company Overview")

                if company.get('description'):
                    st.write(company['description'])
                else:
                    st.write("No description available")

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("#### Key Information")
                    st.markdown(f"""
                    - **Exchange:** {company.get('exchange', 'N/A')}
                    - **Country:** {company.get('incorporation_country', 'N/A')}
                    - **Headquarters:** {company.get('headquarters', 'N/A')}
                    - **Incorporation:** {company.get('founded_date', 'N/A')}
                    """)

                with col2:
                    st.markdown("#### Latest Metrics")

                    # Get valuation metrics through controller
                    try:
                        metrics_list = controllers['financial'].get_valuation_metrics_by_company(company_id)

                        if metrics_list:
                            m = metrics_list[0] if isinstance(metrics_list, list) else metrics_list

                            metric_col1, metric_col2 = st.columns(2)

                            with metric_col1:
                                pe = m.get('pe_ratio')
                                st.metric("P/E Ratio", f"{pe:.2f}" if pe and pd.notna(pe) else "N/A")

                                roe = m.get('roe')
                                roe_val = roe * 100 if roe and pd.notna(roe) else None
                                st.metric("ROE", f"{roe_val:.2f}%" if roe_val else "N/A")

                                current = m.get('current_ratio')
                                st.metric("Current Ratio", f"{current:.2f}" if current and pd.notna(current) else "N/A")

                            with metric_col2:
                                pb = m.get('pb_ratio')
                                st.metric("P/B Ratio", f"{pb:.2f}" if pb and pd.notna(pb) else "N/A")

                                roa = m.get('roa')
                                roa_val = roa * 100 if roa and pd.notna(roa) else None
                                st.metric("ROA", f"{roa_val:.2f}%" if roa_val else "N/A")

                                de = m.get('debt_to_equity')
                                st.metric("Debt/Equity", f"{de:.2f}" if de and pd.notna(de) else "N/A")
                        else:
                            st.info("No valuation metrics available")
                    except Exception:
                        st.info("Valuation metrics not available")

            with tab2:
                st.markdown("### Price Analysis")

                period_slider = st.select_slider(
                    "Time Period",
                    options=["1M", "3M", "6M", "1Y", "2Y", "5Y"],
                    value="6M"
                )

                period_map = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "2Y": 730, "5Y": 1825}
                days_slider = period_map[period_slider]

                # Get price data through controller
                end_date_slider = date.today()
                start_date_slider = end_date_slider - timedelta(days=days_slider)

                prices_slider = controllers['price'].get_price_history(company_id, start_date_slider, end_date_slider)

                if prices_slider:
                    df_prices_slider = pd.DataFrame(prices_slider)
                    df_prices_slider['trade_date'] = pd.to_datetime(df_prices_slider['trade_date'])
                    df_prices_slider = df_prices_slider.sort_values('trade_date')
                    df_prices_slider['trade_date'] = df_prices_slider['trade_date']

                    # ========== ADVANCED CANDLESTICK WITH MA AND VOLUME ==========
                    fig = make_subplots(
                        rows=2, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.03,
                        row_heights=[0.7, 0.3],
                        subplot_titles=(f'{selected_ticker} Stock Price', 'Volume')
                    )

                    # Candlestick
                    fig.add_trace(
                        go.Candlestick(
                            x=df_prices_slider['trade_date'],
                            open=df_prices_slider['open_price'],
                            high=df_prices_slider['high_price'],
                            low=df_prices_slider['low_price'],
                            close=df_prices_slider['close_price'],
                            name='Price'
                        ),
                        row=1, col=1
                    )

                    # Calculate moving averages
                    df_prices_slider['MA20'] = df_prices_slider['close_price'].rolling(window=20).mean()
                    df_prices_slider['MA50'] = df_prices_slider['close_price'].rolling(window=50).mean()

                    # 20-Day MA
                    fig.add_trace(
                        go.Scatter(
                            x=df_prices_slider['trade_date'],
                            y=df_prices_slider['MA20'],
                            name='20-Day MA',
                            line=dict(color='orange', width=1)
                        ),
                        row=1, col=1
                    )

                    # 50-Day MA
                    fig.add_trace(
                        go.Scatter(
                            x=df_prices_slider['trade_date'],
                            y=df_prices_slider['MA50'],
                            name='50-Day MA',
                            line=dict(color='red', width=1)
                        ),
                        row=1, col=1
                    )

                    # Volume bars (colored by price direction)
                    colors = ['red' if row['close_price'] < row['open_price'] else 'green'
                              for _, row in df_prices_slider.iterrows()]

                    fig.add_trace(
                        go.Bar(
                            x=df_prices_slider['trade_date'],
                            y=df_prices_slider['volume'],
                            name='Volume',
                            marker_color=colors,
                            showlegend=False
                        ),
                        row=2, col=1
                    )

                    fig.update_layout(
                        height=700,
                        xaxis_rangeslider_visible=False,
                        hovermode='closest'
                    )

                    st.plotly_chart(fig, use_container_width=True, key='stock_price_analysis_candlestick_ma')

                    # ========== PRICE STATISTICS ==========
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric("Current Price", f"${df_prices_slider['close_price'].iloc[-1]:.2f}")

                    with col2:
                        price_change = df_prices_slider['close_price'].iloc[-1] - df_prices_slider['close_price'].iloc[0]
                        pct_change = (price_change / df_prices_slider['close_price'].iloc[0]) * 100
                        st.metric("Change", f"${price_change:.2f}", f"{pct_change:.2f}%")

                    with col3:
                        st.metric("Highest", f"${df_prices_slider['high_price'].max():.2f}")

                    with col4:
                        st.metric("Lowest", f"${df_prices_slider['low_price'].min():.2f}")
                else:
                    st.info(f"No price data available for {selected_ticker} in selected period")

    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        import traceback
        with st.expander("Show Error Details"):
            st.code(traceback.format_exc())
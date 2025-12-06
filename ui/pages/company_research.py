"""
Company Research Page Module
Friend's UI features with proper architecture
ARCHITECTURE: UI → Controllers → Services → Repositories → Database
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta

def show_company_research(controllers):
    """
    Display company research page - Friend's features using your controllers.

    Args:
        controllers: Dictionary of controller instances
    """
    st.markdown('<h2>Company Research</h2>', unsafe_allow_html=True)

    # Get companies through controller
    companies = controllers['company'].get_all_companies()

    if not companies:
        st.warning("⚠️ No companies available")
        return

    # Company selector
    company_dict = {c['ticker_symbol']: c for c in companies}

    selected_ticker = st.selectbox(
        "Select Company",
        sorted(company_dict.keys()),
        format_func=lambda x: f"{x} - {company_dict[x]['company_name']}"
    )

    if selected_ticker:
        company = company_dict[selected_ticker]
        company_id = company['company_id']

        # ========== COMPANY HEADER ==========
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            st.markdown(f"## {company['company_name']}")
            st.markdown(f"**{company['ticker_symbol']}** | {company.get('sector_name', 'N/A')}")

        with col2:
            # Get latest price through controller
            try:
                latest_price_data = controllers['price'].get_price_by_company_and_date(
                    company_id,
                    date.today()
                )

                if not latest_price_data:
                    # Get most recent price
                    end_date = date.today()
                    start_date = end_date - timedelta(days=30)
                    prices = controllers['price'].get_price_history(company_id, start_date, end_date)

                    if prices:
                        latest_price_data = prices[-1]

                if latest_price_data:
                    st.metric("Current Price", f"${latest_price_data['close_price']:.2f}")
                else:
                    st.metric("Current Price", "N/A")
            except:
                st.metric("Current Price", "N/A")

        with col3:
            if company.get('market_cap'):
                market_cap_b = company['market_cap'] / 1000  # Convert M to B
                st.metric("Market Cap", f"${market_cap_b:.2f}B")
            else:
                st.metric("Market Cap", "N/A")

        # ========== TABS ==========
        tab1, = st.tabs(["Overview"])

        # ========== TAB 1: OVERVIEW ==========
        with tab1:
            st.markdown("### Company Overview")

            # Company details
            st.markdown(f"### {company['company_name']}")

            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown("**Sector:**")
                st.write(company.get('sector_name', 'N/A'))
                st.markdown("**Exchange:**")
                st.write(company.get('exchange', 'N/A'))

            with col2:
                st.markdown("**Country:**")
                st.write(company.get('incorporation_country', 'N/A'))
                st.markdown("**Headquarters:**")
                st.write(company.get('headquarters', 'N/A'))

            with col3:
                st.markdown("**Market Cap:**")
                if company.get('market_cap'):
                    st.write(f"${company['market_cap']:,.0f}M")
                else:
                    st.write("N/A")
                st.markdown("**Employees:**")
                if company.get('employees'):
                    st.write(f"{company['employees']:,}")
                else:
                    st.write("N/A")

            # Description
            if company.get('description'):
                with st.expander("Company Description"):
                    st.write(company['description'])

            # ========== LATEST VALUATION METRICS ==========
            st.markdown("---")
            st.markdown('### Latest Valuation Metrics')

            try:
                metrics_list = controllers['financial'].get_valuation_metrics_by_company(company_id)

                if metrics_list:
                    # Get most recent
                    latest_metric = metrics_list[0] if isinstance(metrics_list, list) else metrics_list

                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("#### Key Information")
                        st.markdown(f"""
                        - **Exchange:** {company.get('exchange', 'N/A')}
                        - **Country:** {company.get('incorporation_country', 'N/A')}
                        - **Headquarters:** {company.get('headquarters', 'N/A')}
                        - **Founded:** {company.get('founded_date', 'N/A')}
                        """)

                    with col2:
                        st.markdown("#### Latest Metrics")

                        metric_col1, metric_col2 = st.columns(2)

                        with metric_col1:
                            pe = latest_metric.get('pe_ratio')
                            st.metric("P/E Ratio", f"{pe:.2f}" if pe and pd.notna(pe) else "N/A")

                            roe = latest_metric.get('roe')
                            st.metric("ROE", f"{roe*100:.2f}%" if roe and pd.notna(roe) else "N/A")

                            current = latest_metric.get('current_ratio')
                            st.metric("Current Ratio", f"{current:.2f}" if current and pd.notna(current) else "N/A")

                        with metric_col2:
                            pb = latest_metric.get('pb_ratio')
                            st.metric("P/B Ratio", f"{pb:.2f}" if pb and pd.notna(pb) else "N/A")

                            roa = latest_metric.get('roa')
                            st.metric("ROA", f"{roa*100:.2f}%" if roa and pd.notna(roa) else "N/A")

                            de = latest_metric.get('debt_to_equity')
                            st.metric("Debt/Equity", f"{de:.2f}" if de and pd.notna(de) else "N/A")
                else:
                    st.info("No valuation metrics available for this company")
            except Exception as e:
                st.info("Valuation metrics not available")
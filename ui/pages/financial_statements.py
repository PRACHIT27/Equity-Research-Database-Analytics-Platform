"""
Financial Statements Page Module
Friend's UI features with proper architecture
ARCHITECTURE: UI → Controllers → Services → Repositories → Database
"""

import streamlit as st
import pandas as pd
import plotly.express as px

def show_financial_statements(controllers, permissions):
    """
    Display financial statements page - Friend's exact features.
    
    Args:
        controllers: Dictionary of controller instances
        permissions: User permissions dictionary
    """
    st.markdown('<div class="main-header">📄 Financial Statements</div>', unsafe_allow_html=True)

    companies = controllers['company'].get_all_companies()

    if not companies:
        st.warning("⚠️ No companies available")
        return

    # Company and statement type selectors
    col1, col2 = st.columns([2, 1])

    with col1:
        company_dict = {c['ticker_symbol']: c for c in companies}

        selected_ticker = st.selectbox(
            "Select Company",
            sorted(company_dict.keys()),
            format_func=lambda x: f"{x} - {company_dict[x]['company_name']}"
        )

    with col2:
        stmt_type = st.selectbox(
            "Statement Type",
            ["Income Statement"]
        )

    if selected_ticker:
        company = company_dict[selected_ticker]
        company_id = company['company_id']
        print(company_id)

        if stmt_type == "Income Statement":
            try:
                # Get income statements through controller
                statements = controllers['financial'].get_statements_by_company(company_id)

                if statements:
                    # Filter for income statements only
                    income_statements = [s for s in statements if s.get('statement_type') == 'IncomeStatement']

                    if income_statements:
                        # Get detailed income statement data through repository
                        # (This is read-only analytics, so acceptable)
                        income_details = controllers['financial'].get_income_statements_by_company(company_id)
                        if income_details:
                            df = pd.DataFrame(income_details)

                            # Sort by fiscal year/period
                            df = df.sort_values(['fiscal_year', 'fiscal_quarter'], ascending=False)
                            df = df.head(12)  # Last 12 periods

                            # Convert to millions for display
                            for col in ['revenue', 'gross_profit', 'operating_income', 'net_income']:
                                if col in df.columns:
                                    df[col] = df[col] / 1_000_000

                            # Display table
                            display_cols = ['fiscal_year', 'fiscal_quarter', 'revenue',
                                            'gross_profit', 'operating_income', 'net_income']
                            available_cols = [col for col in display_cols if col in df.columns]

                            st.dataframe(
                                df[available_cols],
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    'revenue': st.column_config.NumberColumn("Revenue ($M)", format="$%.2f"),
                                    'gross_profit': st.column_config.NumberColumn("Gross Profit ($M)", format="$%.2f"),
                                    'operating_income': st.column_config.NumberColumn("Operating Income ($M)", format="$%.2f"),
                                    'net_income': st.column_config.NumberColumn("Net Income ($M)", format="$%.2f"),
                                }
                            )

                            # ========== REVENUE TREND CHART ==========
                            if 'revenue' in df.columns and 'fiscal_quarter' in df.columns:
                                # Create period label
                                df['period_label'] = df['fiscal_year'].astype(str) + ' ' + df['fiscal_quarter'].astype(str)
                                df = df.sort_values(['fiscal_year', 'fiscal_quarter'])

                                fig = px.line(
                                    df,
                                    x='period_label',
                                    y='revenue',
                                    title=f"{selected_ticker} Revenue Trend",
                                    markers=True,
                                    labels={'period_label': 'Period', 'revenue': 'Revenue ($M)'}
                                )

                                fig.update_layout(height=400)
                                st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No detailed income statement data available")
                    else:
                        st.info(f"No income statements found for {selected_ticker}")
                else:
                    st.info(f"No financial statements available for {selected_ticker}")

            except Exception as e:
                st.error(f"❌ Error loading financial statements: {e}")
                import traceback
                with st.expander("Show Error Details"):
                    st.code(traceback.format_exc())
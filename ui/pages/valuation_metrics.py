"""
Valuation Analysis Page Module
Friend's UI features with proper architecture
ARCHITECTURE: UI → Controllers → Services → Repositories → Database
"""

import streamlit as st
import pandas as pd
import plotly.express as px

def show_valuation_metrics(controllers, permissions):
    """
    Display valuation analysis page - Friend's exact features.

    Args:
        controllers: Dictionary of controller instances
        permissions: User permissions dictionary
    """
    st.markdown('<div class="main-header">💎 Valuation Analysis</div>', unsafe_allow_html=True)

    try:
        # Get all valuation metrics through controller
        valuation_metrics = controllers['financial'].get_all_valuation_metrics()

        if not valuation_metrics:
            st.warning("⚠️ No valuation metrics available")
            st.info("💡 Add valuation metrics or run the calculation stored procedure")
            return

        df_metrics = pd.DataFrame(valuation_metrics)

        # CRITICAL: Convert to pure pandas (avoid narwhals)
        df_metrics = df_metrics.copy()
        for col in df_metrics.columns:
            if df_metrics[col].dtype == 'object':
                df_metrics[col] = pd.to_numeric(df_metrics[col], errors='ignore')

        # Get latest metrics per company
        if 'calculation_date' in df_metrics.columns:
            df_latest = df_metrics.sort_values('calculation_date').groupby('company_id').tail(1).reset_index(drop=True)
        else:
            df_latest = df_metrics.reset_index(drop=True)

        # ========== VALUATION METRICS TABLE ==========
        display_cols = ['ticker_symbol', 'company_name', 'sector_name',
                        'pe_ratio', 'pb_ratio', 'roe', 'debt_to_equity',
                        'net_margin', 'calculation_date']

        available_cols = [col for col in display_cols if col in df_latest.columns]

        st.dataframe(
            df_latest[available_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                'pe_ratio': st.column_config.NumberColumn("P/E Ratio", format="%.2f"),
                'pb_ratio': st.column_config.NumberColumn("P/B Ratio", format="%.2f"),
                'roe': st.column_config.NumberColumn("ROE", format="%.2f"),
                'debt_to_equity': st.column_config.NumberColumn("D/E Ratio", format="%.2f"),
                'net_margin': st.column_config.NumberColumn("Net Margin", format="%.2f"),
                'calculation_date': st.column_config.DateColumn("Date", format="YYYY-MM-DD")
            }
        )

        # ========== P/E vs ROE SCATTER PLOT ==========
        st.markdown("---")
        st.markdown("### 📊 P/E Ratio vs ROE by Sector")

        # Filter out null values
        scatter_data = df_latest[
            (df_latest['pe_ratio'].notna()) &
            (df_latest['roe'].notna()) &
            (df_latest['pb_ratio'].notna())
            ].copy()

        if not scatter_data.empty:
            fig = px.scatter(
                scatter_data,
                x='roe',
                y='pe_ratio',
                color='sector_name',
                size='pb_ratio',
                hover_data=['ticker_symbol', 'company_name'],
                title='P/E Ratio vs ROE by Sector',
                labels={'roe': 'Return on Equity', 'pe_ratio': 'P/E Ratio'}
            )

            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough data for P/E vs ROE analysis")

    except Exception as e:
        st.error(f"❌ Error loading valuation analysis: {e}")
        import traceback
        with st.expander("Show Error Details"):
            st.code(traceback.format_exc())
"""
Table Components
Reusable table display and formatting utilities
"""

import streamlit as st
from datetime import datetime

def display_dataframe_with_export(df, filename_prefix, columns=None):
    """
    Display dataframe with export functionality.

    Args:
        df: Pandas DataFrame
        filename_prefix: Prefix for export filename
        columns: List of columns to display (None for all)
    """
    if df.empty:
        st.info("No data available")
        return

    # Display count
    st.caption(f"Total Records: {len(df)}")

    # Select columns to display
    if columns:
        display_df = df[columns]
    else:
        display_df = df

    # Display table
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

    # Export button
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True
    )


def display_company_table(companies_df):
    """
    Display companies with proper formatting.

    Args:
        companies_df: DataFrame of companies
    """
    if companies_df.empty:
        st.info("No companies found")
        return

    st.dataframe(
        companies_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "market_cap": st.column_config.NumberColumn(
                "Market Cap",
                help="Market capitalization in millions",
                format="$%.2fM"
            ),
            "employees": st.column_config.NumberColumn(
                "Employees",
                format="%d"
            ),
            "ticker_symbol": st.column_config.TextColumn(
                "Ticker",
                width="small"
            )
        }
    )


def display_stock_price_table(prices_df):
    """
    Display stock prices with proper formatting.

    Args:
        prices_df: DataFrame of stock prices
    """
    if prices_df.empty:
        st.info("No price data found")
        return

    st.dataframe(
        prices_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "open_price": st.column_config.NumberColumn("Open", format="$%.2f"),
            "high_price": st.column_config.NumberColumn("High", format="$%.2f"),
            "low_price": st.column_config.NumberColumn("Low", format="$%.2f"),
            "close_price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "adjusted_close": st.column_config.NumberColumn("Adj Close", format="$%.2f"),
            "volume": st.column_config.NumberColumn("Volume", format="%d"),
            "daily_return": st.column_config.NumberColumn("Daily Return", format="%.2f%%"),
            "trading_date": st.column_config.DateColumn("Date", format="YYYY-MM-DD")
        }
    )


def display_forecast_table(forecasts_df):
    """
    Display analyst forecasts with proper formatting.

    Args:
        forecasts_df: DataFrame of forecasts
    """
    if forecasts_df.empty:
        st.info("No forecasts available")
        return

    # Color code recommendations
    def color_recommendation(rec):
        colors = {
            'Strong Buy': '🟢',
            'Buy': '🟡',
            'Hold': '⚪',
            'Sell': '🟠',
            'Strong Sell': '🔴'
        }
        return colors.get(rec, '⚪') + ' ' + rec

    if 'recommendation' in forecasts_df.columns:
        forecasts_df['recommendation_display'] = forecasts_df['recommendation'].apply(color_recommendation)

    st.dataframe(
        forecasts_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "target_price": st.column_config.NumberColumn(
                "Target Price",
                format="$%.2f"
            ),
            "confidence_score": st.column_config.ProgressColumn(
                "Confidence",
                format="%.0%%",
                min_value=0,
                max_value=1
            ),
            "forecast_date": st.column_config.DateColumn(
                "Forecast Date",
                format="YYYY-MM-DD"
            ),
            "target_date": st.column_config.DateColumn(
                "Target Date",
                format="YYYY-MM-DD"
            )
        }
    )


def create_data_table_config(column_formats):
    """
    Create column configuration for dataframe display.

    Args:
        column_formats: Dict of column_name -> format_type

    Returns:
        dict: Column configuration for st.dataframe
    """
    config = {}

    for col, fmt in column_formats.items():
        if fmt == 'currency':
            config[col] = st.column_config.NumberColumn(col, format="$%.2f")
        elif fmt == 'currency_millions':
            config[col] = st.column_config.NumberColumn(col, format="$%.2fM")
        elif fmt == 'number':
            config[col] = st.column_config.NumberColumn(col, format="%d")
        elif fmt == 'percentage':
            config[col] = st.column_config.NumberColumn(col, format="%.2f%%")
        elif fmt == 'date':
            config[col] = st.column_config.DateColumn(col, format="YYYY-MM-DD")

    return config


def display_summary_statistics(df, metrics):
    """
    Display summary statistics in metric cards.

    Args:
        df: DataFrame to analyze
        metrics: List of tuples (column_name, label, format_type)
    """
    cols = st.columns(len(metrics))

    for idx, (col_name, label, fmt) in enumerate(metrics):
        with cols[idx]:
            if col_name in df.columns:
                value = df[col_name].mean() if fmt != 'count' else len(df)

                if fmt == 'currency':
                    st.metric(label, f"${value:,.2f}")
                elif fmt == 'currency_millions':
                    st.metric(label, f"${value:,.2f}M")
                elif fmt == 'percentage':
                    st.metric(label, f"{value:.2f}%")
                else:
                    st.metric(label, f"{value:,}")
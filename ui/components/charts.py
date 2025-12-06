"""
Chart Components
Reusable chart and visualization utilities
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def create_candlestick_chart(df, ticker_symbol):
    """
    Create candlestick chart for stock prices.

    Args:
        df: DataFrame with OHLC data
        ticker_symbol: Stock ticker for title

    Returns:
        plotly.graph_objects.Figure
    """
    fig = go.Figure(data=[go.Candlestick(
        x=df['trading_date'],
        open=df['open_price'],
        high=df['high_price'],
        low=df['low_price'],
        close=df['close_price'],
        name='Price'
    )])

    fig.update_layout(
        title=f'{ticker_symbol} Stock Price',
        xaxis_title='Date',
        yaxis_title='Price ($)',
        height=500,
        xaxis_rangeslider_visible=False,
        hovermode='x unified'
    )

    return fig


def create_line_chart(df, x_col, y_col, title, x_label, y_label):
    """
    Create line chart.

    Args:
        df: DataFrame
        x_col: X-axis column name
        y_col: Y-axis column name
        title: Chart title
        x_label: X-axis label
        y_label: Y-axis label

    Returns:
        plotly.graph_objects.Figure
    """
    fig = px.line(
        df,
        x=x_col,
        y=y_col,
        title=title,
        labels={x_col: x_label, y_col: y_label}
    )

    fig.update_layout(
        height=400,
        hovermode='x unified'
    )

    return fig


def create_bar_chart(df, x_col, y_col, title, orientation='v', color_col=None):
    """
    Create bar chart.

    Args:
        df: DataFrame
        x_col: X-axis column
        y_col: Y-axis column
        title: Chart title
        orientation: 'v' for vertical, 'h' for horizontal
        color_col: Column to use for coloring

    Returns:
        plotly.graph_objects.Figure
    """
    fig = px.bar(
        df,
        x=x_col if orientation == 'v' else y_col,
        y=y_col if orientation == 'v' else x_col,
        title=title,
        orientation=orientation,
        color=color_col if color_col else y_col,
        color_continuous_scale='Blues'
    )

    fig.update_layout(height=400)

    return fig


def create_pie_chart(df, values_col, names_col, title, hole=0.4):
    """
    Create pie/donut chart.

    Args:
        df: DataFrame
        values_col: Column for values
        names_col: Column for labels
        title: Chart title
        hole: Size of hole for donut chart (0 for pie)

    Returns:
        plotly.graph_objects.Figure
    """
    fig = px.pie(
        df,
        values=values_col,
        names=names_col,
        title=title,
        hole=hole,
        color_discrete_sequence=px.colors.qualitative.Set3
    )

    fig.update_traces(
        textposition='inside',
        textinfo='percent+label'
    )

    fig.update_layout(
        height=400,
        showlegend=True
    )

    return fig


def create_scatter_plot(df, x_col, y_col, title, color_col=None, size_col=None):
    """
    Create scatter plot.

    Args:
        df: DataFrame
        x_col: X-axis column
        y_col: Y-axis column
        title: Chart title
        color_col: Column for color coding
        size_col: Column for bubble size

    Returns:
        plotly.graph_objects.Figure
    """
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        title=title,
        color=color_col,
        size=size_col,
        hover_data=df.columns
    )

    fig.update_layout(height=500)

    return fig


def create_heatmap(df, x_col, y_col, value_col, title):
    """
    Create heatmap.

    Args:
        df: DataFrame
        x_col: X-axis column
        y_col: Y-axis column
        value_col: Values for heat
        title: Chart title

    Returns:
        plotly.graph_objects.Figure
    """
    pivot_df = df.pivot(index=y_col, columns=x_col, values=value_col)

    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='Blues'
    ))

    fig.update_layout(
        title=title,
        height=500
    )

    return fig


def create_recommendation_chart(forecasts_df):
    """
    Create recommendation distribution chart.

    Args:
        forecasts_df: DataFrame with forecasts

    Returns:
        plotly.graph_objects.Figure
    """
    if 'recommendation' not in forecasts_df.columns:
        return None

    rec_counts = forecasts_df['recommendation'].value_counts().reset_index()
    rec_counts.columns = ['recommendation', 'count']

    # Color mapping
    color_map = {
        'Strong Buy': '#28a745',
        'Buy': '#5cb85c',
        'Hold': '#ffc107',
        'Sell': '#f0ad4e',
        'Strong Sell': '#dc3545'
    }

    fig = px.bar(
        rec_counts,
        x='recommendation',
        y='count',
        title='Analyst Recommendations Distribution',
        color='recommendation',
        color_discrete_map=color_map
    )

    fig.update_layout(
        showlegend=False,
        height=400
    )

    return fig
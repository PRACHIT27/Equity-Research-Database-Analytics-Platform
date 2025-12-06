"""
Display Formatting Utilities
Functions for formatting data for display
"""

from datetime import datetime, date
from typing import Optional

def format_currency(value: Optional[float], millions: bool = True) -> str:
    """
    Format value as currency.

    Args:
        value: Numeric value
        millions: If True, append 'M' for millions

    Returns:
        Formatted currency string
    """
    if value is None:
        return "N/A"

    if millions:
        return f"${value:,.2f}M"
    else:
        return f"${value:,.2f}"


def format_number(value: Optional[int]) -> str:
    """
    Format number with thousands separator.

    Args:
        value: Numeric value

    Returns:
        Formatted number string
    """
    if value is None:
        return "N/A"

    return f"{value:,}"


def format_percentage(value: Optional[float], decimals: int = 2) -> str:
    """
    Format value as percentage.

    Args:
        value: Numeric value (0.15 = 15%)
        decimals: Number of decimal places

    Returns:
        Formatted percentage string
    """
    if value is None:
        return "N/A"

    return f"{value * 100:.{decimals}f}%"


def format_date(value: Optional[date]) -> str:
    """
    Format date.

    Args:
        value: Date object

    Returns:
        Formatted date string
    """
    if value is None:
        return "N/A"

    if isinstance(value, str):
        return value

    return value.strftime("%Y-%m-%d")


def format_datetime(value: Optional[datetime]) -> str:
    """
    Format datetime.

    Args:
        value: Datetime object

    Returns:
        Formatted datetime string
    """
    if value is None:
        return "N/A"

    if isinstance(value, str):
        return value

    return value.strftime("%Y-%m-%d %H:%M:%S")


def format_ratio(value: Optional[float], decimals: int = 2) -> str:
    """
    Format financial ratio.

    Args:
        value: Ratio value
        decimals: Number of decimal places

    Returns:
        Formatted ratio string
    """
    if value is None:
        return "N/A"

    return f"{value:.{decimals}f}"


def format_market_cap_tier(market_cap: Optional[float]) -> str:
    """
    Categorize company by market cap.

    Args:
        market_cap: Market capitalization in millions

    Returns:
        Market cap category
    """
    if market_cap is None:
        return "Unknown"

    if market_cap >= 200000:
        return "Mega Cap"
    elif market_cap >= 10000:
        return "Large Cap"
    elif market_cap >= 2000:
        return "Mid Cap"
    elif market_cap >= 300:
        return "Small Cap"
    else:
        return "Micro Cap"


def format_recommendation_color(recommendation: str) -> str:
    """
    Get color for recommendation.

    Args:
        recommendation: Recommendation text

    Returns:
        Color code
    """
    colors = {
        'Strong Buy': '#28a745',
        'Buy': '#5cb85c',
        'Hold': '#ffc107',
        'Sell': '#f0ad4e',
        'Strong Sell': '#dc3545'
    }

    return colors.get(recommendation, '#6c757d')


def truncate_text(text: Optional[str], max_length: int = 100) -> str:
    """
    Truncate text to max length.

    Args:
        text: Text to truncate
        max_length: Maximum length

    Returns:
        Truncated text with ellipsis
    """
    if not text:
        return ""

    if len(text) <= max_length:
        return text

    return text[:max_length - 3] + "..."
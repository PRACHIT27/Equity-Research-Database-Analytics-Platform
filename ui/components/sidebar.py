"""
Sidebar Component
Reusable sidebar navigation and user info display
"""

import streamlit as st

def render_sidebar(user_info, permissions):
    """
    Render sidebar with user info and navigation.

    Args:
        user_info: Dictionary with user information
        permissions: Dictionary of user permissions

    Returns:
        str: Selected page name
    """

    with st.sidebar:
        # User Information Section
        st.markdown("---")
        st.markdown(f"### 👤 {user_info['full_name']}")
        st.caption(f"**Role:** {user_info['role_name']}")
        st.caption(f"**Username:** @{user_info['username']}")

        st.markdown("---")

        # Permissions Display
        with st.expander("🔐 Your Permissions", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"✏️ Create: {'✅' if permissions.get('can_create') else '❌'}")
                st.write(f"👁️ Read: {'✅' if permissions.get('can_read') else '❌'}")
                st.write(f"🔄 Update: {'✅' if permissions.get('can_update') else '❌'}")
                st.write(f"🗑️ Delete: {'✅' if permissions.get('can_delete') else '❌'}")
            with col2:
                st.write(f"📊 Reports: {'✅' if permissions.get('can_execute_reports') else '❌'}")
                st.write(f"👥 Users: {'✅' if permissions.get('can_manage_users') else '❌'}")
                st.write(f"✔️ Approve: {'✅' if permissions.get('can_approve') else '❌'}")

        st.markdown("---")

        # Navigation Menu
        menu_items = [
            "🏠 Dashboard",
            "🏢 Companies",
            "🏢 Company Research",
            "📈 Stock Prices",
            "🔮 Forecasts",
            "📊 Valuation Metrics",
            "📄 Financial Statements",
            "⭐ My Watchlist",
            "📈 Analytics"
        ]

        # Add User Management for admins
        if permissions.get('can_manage_users'):
            menu_items.insert(-2, "👥 User Management")

        # Add Reports if user has permission
        if permissions.get('can_execute_reports'):
            menu_items.append("📋 Reports")

        selected_page = st.radio(
            "📋 Navigation",
            menu_items,
            label_visibility="collapsed"
        )

        st.markdown("---")

        # Logout Button
        logout = st.button("🚪 Logout", use_container_width=True, type="primary")

        if logout:
            return None  # Signal logout

        return selected_page


def render_permission_badge(permission_name, has_permission):
    """
    Render a permission badge.

    Args:
        permission_name: Name of permission
        has_permission: Boolean indicating if user has permission
    """
    if has_permission:
        st.success(f"✅ {permission_name}")
    else:
        st.error(f"❌ {permission_name}")
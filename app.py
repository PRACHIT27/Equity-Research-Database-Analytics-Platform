"""
Main Application Entry Point
Equity Research Database Management System
REFACTORED: Proper architecture using Controllers
"""

import streamlit as st
from datetime import datetime

# Import controllers (Facade pattern)
from controller import (
    get_company_controller,
    get_price_controller,
    get_forecast_controller,
    get_financial_controller,
    get_user_controller,
    get_analytics_controller
)

# Import UI pages
from ui.pages import (
    show_dashboard,
    show_companies,
    show_stock_prices,
    show_forecasts,
    show_financial_statements,
    show_valuation_metrics,
    show_user_management,
    show_analytics
)

# Import components
from ui.components.sidebar import render_sidebar
from ui.pages.company_research import show_company_research

# ========== PAGE CONFIGURATION ==========

st.set_page_config(
    page_title="Equity Research Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== CUSTOM CSS ==========

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        padding: 1rem 0;
        border-bottom: 3px solid #1f77b4;
        margin-bottom: 2rem;
    }
    
    .stButton>button {
        width: 100%;
    }
    
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ========== INITIALIZE CONTROLLERS ==========

@st.cache_resource
def initialize_controllers():
    """
    Initialize all controllers (singleton pattern).
    Cached to avoid recreating on every rerun.

    Returns:
        dict: Dictionary of controller instances
    """
    try:
        controllers = {
            'company': get_company_controller(),
            'price': get_price_controller(),
            'forecast': get_forecast_controller(),
            'financial': get_financial_controller(),
            'user': get_user_controller(),
            'analytics': get_analytics_controller()
        }
        return controllers
    except Exception as e:
        st.error(f"Failed to initialize controllers: {e}")
        st.stop()

# ========== SESSION STATE MANAGEMENT ==========

def initialize_session_state():
    """Initialize session state variables"""
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    if 'user_info' not in st.session_state:
        st.session_state.user_info = None

    if 'permissions' not in st.session_state:
        st.session_state.permissions = {}

# ========== AUTHENTICATION ==========

def show_login_page(controllers):
    """Display login page"""
    st.markdown('<div class="main-header">🔐 Login to Equity Research Pro</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("### Welcome Back!")

        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")

            submitted = st.form_submit_button("🚀 Login", use_container_width=True, type="primary")

            if submitted:
                if not username or not password:
                    st.error("❌ Please enter both username and password")
                else:
                    try:
                        # Authenticate using controller
                        user_data = controllers['user'].authenticate(username, password)

                        if user_data:
                            # Store user info in session
                            st.session_state.logged_in = True
                            st.session_state.user_info = user_data
                            st.session_state.permissions = {
                                'can_create': user_data.get('can_create', False),
                                'can_read': user_data.get('can_read', True),
                                'can_update': user_data.get('can_update', False),
                                'can_delete': user_data.get('can_delete', False),
                                'can_execute_reports': user_data.get('can_execute_reports', False),
                                'can_manage_users': user_data.get('can_manage_users', False),
                                'can_approve': user_data.get('can_approve', False)
                            }

                            st.success(f"✅ Welcome back, {user_data['full_name']}!")
                            st.rerun()
                        else:
                            st.error("❌ Invalid username or password")

                    except Exception as e:
                        st.error(f"❌ Login error: {e}")

        st.markdown("---")
        st.info("""
        **Demo Accounts:**
        - Username: `admin` | Password: `password` (Full Access)
        - Username: `analyst1` | Password: `password` (Advanced)
        - Username: `associate1` | Password: `password` (Intermediate)
        """)

# ========== MAIN APPLICATION ==========

def show_main_app(controllers):
    """Display main application after login"""

    user_info = st.session_state.user_info
    permissions = st.session_state.permissions

    # Render sidebar and get selected page
    selected_page = render_sidebar(user_info, permissions)

    # Handle logout
    if selected_page is None:
        st.session_state.logged_in = False
        st.session_state.user_info = None
        st.session_state.permissions = {}
        st.rerun()

    # Route to appropriate page using controllers
    if selected_page == "Dashboard":
        show_dashboard(controllers)

    elif selected_page == "Companies":
        show_companies(controllers, permissions)

    elif selected_page == "Company Research":
        show_company_research(controllers)

    elif selected_page == "Stock Prices":
        show_stock_prices(controllers, permissions)

    elif selected_page == "Forecast":
        if permissions.get('can_create'):
            show_forecasts(controllers, permissions)
        else:
            st.error("You don't have permission to access Forecast")

    elif selected_page == "Valuation Metrics":
        show_valuation_metrics(controllers, permissions)

    elif selected_page == "Financial Statements":
        show_financial_statements(controllers, permissions)

    elif selected_page == "User Management":
        if permissions.get('can_manage_users'):
            show_user_management(controllers, permissions)
        else:
            st.error("You don't have permission to access User Management")

    elif selected_page == "Analytics":
        if permissions.get('can_execute_reports'):
            show_analytics(controllers, permissions)
        else:
            st.error("You don't have permission to access Analytics")

    else:
        st.info("Page under construction")

# ========== APPLICATION ENTRY POINT ==========

def main():
    """Main application entry point"""

    # Initialize session state
    initialize_session_state()

    # Initialize controllers
    controllers = initialize_controllers()

    # Show appropriate page
    if st.session_state.logged_in:
        show_main_app(controllers)
    else:
        show_login_page(controllers)

# ========== RUN APPLICATION ==========

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Application Error: {e}")
        import traceback
        with st.expander("Show Technical Details"):
            st.code(traceback.format_exc())
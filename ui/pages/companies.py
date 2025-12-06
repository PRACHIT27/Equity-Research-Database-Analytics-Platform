"""
Companies Page Module
Complete CRUD operations for companies
REFACTORED: Uses Controllers instead of direct repository access
"""

import streamlit as st
import pandas as pd
from datetime import date
from ui.components.tables import display_company_table

def show_companies(controllers, permissions):
    """
    Display companies management page with full CRUD.

    Args:
        controllers: Dictionary of controller instances
        permissions: User permissions dictionary
    """
    st.markdown('<div class="main-header">🏢 Company Management</div>', unsafe_allow_html=True)

    tabs = ["📋 View All", "🔍 Search", "➕ Create", "✏️ Update", "🗑️ Delete"]
    tab_objects = st.tabs(tabs)

    # ========== TAB 1: VIEW ALL ==========
    with tab_objects[0]:
        st.markdown("### 📋 All Companies")

        try:
            companies = controllers['company'].get_all_companies()

            if companies:
                df = pd.DataFrame(companies)

                # Filters
                col1, col2, col3 = st.columns([2, 2, 1])

                with col1:
                    sectors = ['All'] + sorted(df['sector_name'].unique().tolist())
                    selected_sector = st.selectbox("🏭 Filter by Sector", sectors)

                with col2:
                    sort_options = {
                        'Company Name': 'company_name',
                        'Market Cap': 'market_cap',
                        'Ticker': 'ticker_symbol',
                        'Employees': 'employees'
                    }
                    sort_by = st.selectbox("📊 Sort by", list(sort_options.keys()))

                with col3:
                    sort_order = st.radio("Order", ['⬆️ Asc', '⬇️ Desc'], label_visibility="collapsed")

                # Apply filters
                filtered_df = df.copy()
                if selected_sector != 'All':
                    filtered_df = filtered_df[filtered_df['sector_name'] == selected_sector]

                # Sort
                ascending = '⬆️' in sort_order
                sort_col = sort_options[sort_by]
                filtered_df = filtered_df.sort_values(sort_col, ascending=ascending)

                st.caption(f"📊 Showing {len(filtered_df)} of {len(df)} companies")

                # Display table
                display_cols = [
                    'ticker_symbol', 'company_name', 'sector_name',
                    'market_cap', 'employees', 'exchange', 'headquarters'
                ]
                available_cols = [col for col in display_cols if col in filtered_df.columns]

                display_company_table(filtered_df[available_cols])

                # Export
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"companies_{date.today()}.csv",
                    mime="text/csv"
                )
            else:
                st.info("🔭 No companies found in database")
                st.markdown("👉 Add your first company using the **Create** tab!")

        except Exception as e:
            st.error(f"❌ Error: {e}")

    # ========== TAB 2: SEARCH ==========
    with tab_objects[1]:
        st.markdown("### 🔍 Search Companies")

        search_term = st.text_input(
            "Enter company name or ticker symbol",
            placeholder="e.g., AAPL or Apple",
            help="Search by ticker symbol or company name"
        )

        if search_term:
            try:
                results = controllers['company'].search_companies(search_term)

                if results:
                    st.success(f"✅ Found {len(results)} matching companies")
                    df = pd.DataFrame(results)
                    display_company_table(df)
                else:
                    st.warning(f"⚠️ No companies found matching '{search_term}'")
                    st.info("💡 Try searching with partial names or different keywords")

            except Exception as e:
                st.error(f"❌ Search error: {e}")

    # ========== TAB 3: CREATE ==========
    with tab_objects[2]:
        st.markdown("### ➕ Add New Company")

        if not permissions.get('can_create'):
            st.warning("⚠️ You don't have permission to create companies")
            st.info("🔧 Contact your administrator to request CREATE access")
            return

        with st.form("create_company_form", clear_on_submit=True):
            st.markdown("**📝 Required Information**")

            col1, col2 = st.columns(2)

            with col1:
                ticker = st.text_input(
                    "Ticker Symbol *",
                    max_chars=10,
                    placeholder="AAPL",
                    help="Stock ticker symbol (1-10 characters)"
                ).upper().strip()

                company_name = st.text_input(
                    "Company Name *",
                    placeholder="Apple Inc.",
                    help="Full legal name of the company"
                )

                # Get sectors through controller
                sectors = controllers['company'].get_all_sectors()
                if sectors:
                    sector_dict = {s['sector_name']: s['sector_id'] for s in sectors}
                    sector_name = st.selectbox(
                        "Sector *",
                        options=list(sector_dict.keys()),
                        help="Industry sector classification"
                    )
                    sector_id = sector_dict[sector_name]
                else:
                    st.error("⚠️ No sectors available. Please add sectors first!")
                    sector_id = None

                market_cap = st.number_input(
                    "Market Cap (Millions USD) *",
                    min_value=0.01,
                    value=1000.0,
                    step=100.0,
                    format="%.2f",
                    help="Total market value in millions"
                )

                currency = st.text_input(
                    "Currency of Company",
                    max_chars=10,
                    placeholder="AAPL",
                    help="Currency company trades in"
                ).upper().strip()

            with col2:
                exchange = st.selectbox(
                    "Stock Exchange *",
                    options=['NASDAQ', 'NYSE', 'AMEX', 'LSE', 'TSE', 'OTHER'],
                    help="Primary stock exchange listing"
                )

                incorporation_country = st.text_input(
                    "Country of Incorporation",
                    value="USA",
                    max_chars=100,
                    help="Country where company is legally incorporated"
                )

                founded_date = st.date_input(
                    "Founded Date",
                    value=date(2000, 1, 1),
                    min_value=date(1800, 1, 1),
                    max_value=date.today(),
                    help="Company founding date"
                )


            st.markdown("**📄 Additional Details**")
            description = st.text_area(
                "Company Description",
                placeholder="Brief description of the company's business, products, and services...",
                height=100,
                help="Optional description (max 1000 characters)"
            )

            st.markdown("---")

            col_a, col_b, col_c = st.columns([1, 2, 1])
            with col_b:
                submitted = st.form_submit_button(
                    "✅ Create Company",
                    use_container_width=True,
                    type="primary"
                )

            if submitted:
                # Validation
                errors = []

                if not ticker:
                    errors.append("Ticker symbol is required")
                elif len(ticker) > 10:
                    errors.append("Ticker must be 10 characters or less")

                if not company_name:
                    errors.append("Company name is required")

                if sector_id is None:
                    errors.append("Sector must be selected")

                if market_cap <= 0:
                    errors.append("Market cap must be greater than 0")

                if errors:
                    for error in errors:
                        st.error(f"❌ {error}")
                else:
                    try:
                        # Create using controller
                        result = controllers['company'].create_company(
                            ticker_symbol=ticker,
                            company_name=company_name,
                            sector_id=sector_id,
                            market_cap=market_cap,
                            exchange=exchange,
                            incorporation_country=incorporation_country,
                            founded_date=founded_date,
                            description=description if description else None,
                            currency=currency if currency else None
                        )

                        print(result)

                        if not result.get("success") or "error" in result:
                            st.error("❌ Failed to create company. Please check your inputs.")
                        else:
                            st.success(f"✅ Company {ticker} created successfully!")
                            st.balloons()
                            st.info("💡 Go to 'View All' tab to see your new company!")

                    except Exception as e:
                        st.error(f"❌ Error creating company: {e}")

    # ========== TAB 4: UPDATE ==========
    with tab_objects[3]:
        st.markdown("### ✏️ Update Company Information")

        if not permissions.get('can_update'):
            st.warning("⚠️ You don't have permission to update companies")
            st.info("🔧 Contact your administrator to request UPDATE access")
            return

        try:
            companies = controllers['company'].get_all_companies()

            if companies:
                # Company selection
                company_dict = {
                    f"{c['ticker_symbol']} - {c['company_name']}": c['company_id']
                    for c in companies
                }

                selected_display = st.selectbox(
                    "🏢 Select Company to Update",
                    options=list(company_dict.keys()),
                    help="Choose the company you want to modify"
                )
                company_id = company_dict[selected_display]

                # Load company details through controller
                company = controllers['company'].get_company_by_id(company_id)

                if company:
                    st.markdown("---")
                    st.markdown(f"**Editing:** {company['ticker_symbol']} - {company['company_name']}")

                    with st.form("update_company_form"):
                        col1, col2 = st.columns(2)

                        with col1:
                            st.markdown("**Basic Information**")

                            st.text_input(
                                "Ticker Symbol",
                                value=company['ticker_symbol'],
                                disabled=True,
                                help="Ticker cannot be changed"
                            )

                            new_name = st.text_input(
                                "Company Name",
                                value=company['company_name']
                            )

                            new_market_cap = st.number_input(
                                "Market Cap (Millions USD)",
                                value=float(company['market_cap'] or 0),
                                min_value=0.0,
                                step=100.0,
                                format="%.2f"
                            )

                        with col2:
                            st.markdown("**Location & Details**")

                            new_headquarters = st.text_input(
                                "Country",
                                value=company.get('country', '')
                            )

                            new_description = st.text_area(
                                "Description",
                                value=company.get('description', ''),
                                height=180
                            )

                        st.markdown("---")

                        col_a, col_b, col_c = st.columns([1, 2, 1])
                        with col_b:
                            submitted = st.form_submit_button(
                                "💾 Update Company",
                                use_container_width=True,
                                type="primary"
                            )

                        if submitted:
                            try:
                                # Update using controller
                                controllers['company'].update_company(
                                    company_id=company_id,
                                    company_name=new_name if new_name else None,
                                    market_cap=new_market_cap if new_market_cap > 0 else None,
                                    headquarters=new_headquarters if new_headquarters else None,
                                    description=new_description if new_description else None
                                )

                                st.success(f"✅ Company '{company['ticker_symbol']}' updated successfully!")
                                st.rerun()

                            except Exception as e:
                                st.error(f"❌ Update error: {e}")
            else:
                st.info("🔭 No companies available to update")

        except Exception as e:
            st.error(f"❌ Error: {e}")

    # ========== TAB 5: DELETE ==========
    with tab_objects[4]:
        st.markdown("### 🗑️ Delete Company")

        if not permissions.get('can_delete'):
            st.warning("⚠️ You don't have permission to delete companies")
            st.info("🔧 Contact your administrator to request DELETE access")
            return

        st.warning("⚠️ Warning: This action is PERMANENT and will delete all related data!")

        try:
            companies = controllers['company'].get_all_companies()

            if companies:
                company_dict = {
                    f"{c['ticker_symbol']} - {c['company_name']}": c['company_id']
                    for c in companies
                }

                selected_display = st.selectbox(
                    "🏢 Select Company to Delete",
                    options=list(company_dict.keys())
                )
                company_id = company_dict[selected_display]

                # Show company details through controller
                company = controllers['company'].get_company_by_id(company_id)

                if company:
                    st.markdown("---")
                    st.markdown("**📋 Company Details:**")

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.write(f"**Ticker:** {company['ticker_symbol']}")
                        st.write(f"**Name:** {company['company_name']}")

                    with col2:
                        st.write(f"**Sector:** {company['sector_name']}")
                        market_cap_str = f"${company['market_cap']:,.2f}M" if company['market_cap'] else "N/A"
                        st.write(f"**Market Cap:** {market_cap_str}")

                    with col3:
                        st.write(f"**Exchange:** {company.get('exchange', 'N/A')}")

                    st.markdown("---")
                    st.markdown("**🔒 Confirmation Required:**")

                    confirm_text = st.text_input(
                        f"Type the ticker symbol **'{company['ticker_symbol']}'** to confirm",
                        placeholder=company['ticker_symbol'],
                        help="This is to prevent accidental deletions"
                    )

                    confirm_checkbox = st.checkbox(
                        f"I understand that deleting {company['company_name']} will permanently remove all related data"
                    )

                    col_a, col_b, col_c = st.columns([1, 2, 1])

                    with col_b:
                        delete_enabled = (
                                confirm_checkbox and
                                confirm_text.upper().strip() == company['ticker_symbol']
                        )

                        delete_btn = st.button(
                            "🗑️ PERMANENTLY DELETE COMPANY",
                            type="primary",
                            use_container_width=True,
                            disabled=not delete_enabled
                        )

                    if delete_btn:
                        try:
                            # Delete using controller
                            controllers['company'].delete_company(company_id, confirm=True)

                            st.success(f"✅ Company {company['ticker_symbol']} deleted successfully!")
                            st.rerun()

                        except Exception as e:
                            st.error(f"❌ Deletion failed: {e}")
            else:
                st.info("🔭 No companies available to delete")

        except Exception as e:
            st.error(f"❌ Error: {e}")
"""
Analytics Page Module
Advanced analytics and reporting
"""
import datetime

import streamlit as st
import pandas as pd
import plotly.express as px

def show_analytics(controller, permissions):
    """
    Display analytics and reports page.

    Args:
        repos: Dictionary of repository instances
        services: Dictionary of service instances
        permissions: User permissions dictionary
    """
    st.markdown('<div class="main-header">📈 Analytics & Reports</div>', unsafe_allow_html=True)

    if not permissions.get('can_execute_reports'):
        st.warning("⚠️ You don't have permission to execute reports")
        st.info("📧 Contact your administrator to request REPORT access")
        return

    tabs = ["📊 Sector Analysis", "📈 Performance Analysis", "🔍 Custom Query"]
    tab_objects = st.tabs(tabs)

    # ========== TAB 1: SECTOR ANALYSIS ==========
    with tab_objects[0]:
        st.markdown("### 🏭 Sector-wise Analysis")

        try:
            sector_stats = controller['analytics'].get_sector_statistic()

            if sector_stats:
                df = pd.DataFrame(sector_stats)

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Total Sectors", len(df))

                with col2:
                    total_companies = df['company_count'].sum()
                    st.metric("Total Companies", int(total_companies))

                with col3:
                    if 'avg_market_cap' in df.columns:
                        avg_cap = df['avg_market_cap'].mean()
                        st.metric("Avg Market Cap", f"${avg_cap:,.0f}M" if avg_cap else "N/A")

                with col4:
                    if 'total_market_cap' in df.columns:
                        total_cap = df['total_market_cap'].sum()
                        st.metric("Total Market Cap", f"${total_cap:,.0f}M" if total_cap else "N/A")

                st.markdown("---")

                # Charts
                col1, col2 = st.columns(2)

                with col1:
                    fig = px.bar(
                        df,
                        x='sector_name',
                        y='company_count',
                        title='Companies per Sector',
                        labels={'company_count': 'Number of Companies', 'sector_name': 'Sector'},
                        color='company_count',
                        color_continuous_scale='Blues'
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    if 'avg_market_cap' in df.columns:
                        fig = px.bar(
                            df,
                            x='sector_name',
                            y='avg_market_cap',
                            title='Average Market Cap by Sector',
                            labels={'avg_market_cap': 'Avg Market Cap ($M)', 'sector_name': 'Sector'},
                            color='avg_market_cap',
                            color_continuous_scale='Greens'
                        )
                        st.plotly_chart(fig, use_container_width=True)

                # Data table
                st.markdown("#### 📄 Detailed Sector Statistics")
                st.dataframe(df, use_container_width=True, hide_index=True)

            else:
                st.info("No sector data available")

        except Exception as e:
            st.error(f"❌ Error: {e}")

    # ========== TAB 2: PERFORMANCE ANALYSIS ==========
    with tab_objects[1]:
        st.markdown("### 📈 Stock Performance Analysis")

        try:
            days = st.slider("Analysis Period (Days)", 7, 365, 30)
            limit = st.slider("Top N Companies", 5, 20, 10)

            if st.button("📊 Generate Performance Report", use_container_width=True, type="primary"):
                # Call stored procedure
                performers = controller['analytics'].get_top_performer(days, limit)

                if performers:
                    df = pd.DataFrame(performers)

                    st.success(f"✅ Top {limit} performers in last {days} days")

                    # Chart
                    fig = px.bar(
                        df,
                        x='ticker_symbol',
                        y='return_pct',
                        color='return_pct',
                        title=f'Top {limit} Performing Stocks ({days} Days)',
                        labels={'return_pct': 'Return (%)', 'ticker_symbol': 'Ticker'},
                        color_continuous_scale=['red', 'yellow', 'green']
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # Table
                    st.dataframe(
                        df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "return_pct": st.column_config.NumberColumn(
                                "Return %",
                                format="%.2f%%"
                            ),
                            "start_price": st.column_config.NumberColumn(
                                "Start Price",
                                format="$%.2f"
                            ),
                            "end_price": st.column_config.NumberColumn(
                                "End Price",
                                format="$%.2f"
                            )
                        }
                    )
                else:
                    st.info("No performance data available")

        except Exception as e:
            st.error(f"❌ Error: {e}")

    # ========== TAB 3: CUSTOM QUERY (ADMIN ONLY) ==========
    with tab_objects[2]:
        st.markdown("### 🔍 Custom SQL Query")

        if not permissions.get('can_manage_users'):
            st.warning("⚠️ Admin access required for custom queries")
            return

        st.info("⚡ Execute custom SQL queries (READ-ONLY for safety)")

        query = st.text_area(
            "Enter SQL Query",
            placeholder="SELECT * FROM Company LIMIT 10;",
            height=150,
            help="Only SELECT queries are allowed"
        )

        if st.button("▶️ Execute Query", type="primary"):
            if not query.strip():
                st.warning("Please enter a query")
            elif not query.strip().upper().startswith('SELECT'):
                st.error("❌ Only SELECT queries are allowed for safety")
            else:
                try:
                    results = controller['analytics'].execute_custom_query(query)

                    if results:
                        df = pd.DataFrame(results)
                        st.success(f"✅ Query returned {len(df)} rows")
                        st.dataframe(df, use_container_width=True, hide_index=True)

                        # Export
                        csv = df.to_csv(index=False)
                        st.download_button(
                            "📥 Download Results",
                            data=csv,
                            file_name=f"query_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("Query executed successfully (no results)")

                except Exception as e:
                    st.error(f"❌ Query error: {e}")
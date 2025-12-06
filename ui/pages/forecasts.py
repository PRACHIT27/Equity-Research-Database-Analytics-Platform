"""
Forecast Analysis Page Module
COMPLETE Friend's UI features with proper architecture
ARCHITECTURE: UI → Controllers → Services → Repositories → Database
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def show_forecasts(controllers, permissions):
    """
    Display forecast analysis page - Friend's COMPLETE features.

    Args:
        controllers: Dictionary of controller instances
        permissions: User permissions dictionary
    """
    st.markdown('<div class="main-header">🔮 Forecast Analysis & Trading Signals</div>', unsafe_allow_html=True)

    st.markdown("""
    ### 📊 Trading Recommendations Based on Forecasts
    This page visualizes buy/hold/sell signals based on forecasting model predictions.
    """)

    try:
        # Get all forecasts through controller
        all_forecasts = controllers['forecast'].get_all_forecasts()

        if not all_forecasts:
            st.info("No forecasts available. Please create forecasts first.")
            return

        # Get unique companies with forecasts
        df_all = pd.DataFrame(all_forecasts)
        companies_with_forecasts = df_all['ticker_symbol'].unique().tolist()

        # Stock selector
        selected_stock = st.selectbox(
            "Select Stock",
            companies_with_forecasts,
            key="forecast_stock"
        )

        if selected_stock:
            # Filter forecasts for selected stock
            forecasts = df_all[df_all['ticker_symbol'] == selected_stock].copy()
            forecasts = forecasts.sort_values('forecast_date', ascending=False)

            # CRITICAL: Convert all numeric columns to float to avoid Decimal errors
            numeric_cols = ['target_price', 'price_target', 'confidence_score', 'eps_estimate',
                            'revenue_estimate', 'upside_potential_percent']
            for col in numeric_cols:
                if col in forecasts.columns:
                    forecasts[col] = pd.to_numeric(forecasts[col], errors='coerce')

            if not forecasts.empty:
                latest_forecast = forecasts.iloc[0]
                company_id = latest_forecast['company_id']

                # ========== METRICS ROW ==========
                col1, col2, col3, col4 = st.columns(4)

                # Get current/latest price
                current_price = None
                try:
                    from datetime import date, timedelta
                    end_date = date.today()
                    start_date = end_date - timedelta(days=7)
                    recent_prices = controllers['price'].get_price_history(company_id, start_date, end_date)

                    if recent_prices:
                        current_price = float(recent_prices[-1]['close_price'])
                except:
                    pass

                # Convert target_price to float to avoid Decimal errors
                # (Already done above, but ensure it's float type)

                # Calculate expected return and add to forecasts
                if current_price:
                    forecasts['close_price'] = float(current_price)
                    forecasts['expected_return_pct'] = (
                            (forecasts['target_price'].astype(float) - float(current_price)) / float(current_price) * 100
                    )
                else:
                    forecasts['expected_return_pct'] = None

                # Calculate forecast days ahead
                forecasts['forecast_date_dt'] = pd.to_datetime(forecasts['forecast_date'])
                forecasts['target_date_dt'] = pd.to_datetime(forecasts['target_date'])
                forecasts['forecast_days_ahead'] = (
                        forecasts['target_date_dt'] - forecasts['forecast_date_dt']
                ).dt.days

                with col1:
                    st.metric("Current Stock Price", f"${current_price:.2f}" if current_price else "N/A")

                with col2:
                    target_price = latest_forecast.get('target_price')
                    # Use price_target if target_price not available
                    if not target_price:
                        target_price = latest_forecast.get('price_target')
                    st.metric("Target Price (30 days)", f"${target_price:.2f}" if target_price else "N/A")

                with col3:
                    expected_return = latest_forecast.get('expected_return_pct') or forecasts.iloc[0].get('expected_return_pct')
                    if expected_return and pd.notna(expected_return):
                        st.metric("Expected Return", f"{expected_return:.2f}%")
                    else:
                        st.metric("Expected Return", "N/A")

                with col4:
                    confidence = latest_forecast.get('confidence_score')
                    st.metric("Confidence Score", f"{confidence:.1%}" if confidence and pd.notna(confidence) else "N/A")

                # ========== CURRENT RECOMMENDATION BADGE ==========
                rec = latest_forecast.get('recommendation', 'Hold')

                rec_colors_emoji = {
                    'Strong Buy': '🟢',
                    'Buy': '🟢',
                    'Hold': '🟡',
                    'Sell': '🔴',
                    'Strong Sell': '🔴'
                }

                forecast_date = latest_forecast.get('forecast_date')
                target_date = latest_forecast.get('target_date')

                st.markdown(f"""
                <div style="background-color: #f0f2f6; padding: 1.5rem; border-radius: 0.5rem; margin: 1rem 0;">
                    <h3 style="margin: 0; color: #2c3e50;">Current Recommendation</h3>
                    <h2 style="margin: 0.5rem 0; color: #1f77b4;">
                        {rec_colors_emoji.get(rec, '⚪')} {rec}
                    </h2>
                    <p style="margin: 0.5rem 0; color: #555;">
                        Forecast Date: {forecast_date if pd.notna(forecast_date) else 'N/A'} | 
                        Target Date: {target_date if pd.notna(target_date) else 'N/A'}
                    </p>
                </div>
                """, unsafe_allow_html=True)

                # ========== FORECAST HISTORY TABLE (WITH ALL COLUMNS) ==========
                st.markdown("---")
                st.markdown("### 📈 Forecast History")

                # Prepare display dataframe with ALL columns
                display_cols = ['forecast_date', 'target_date', 'target_price', 'recommendation',
                                'confidence_score', 'expected_return_pct', 'eps_estimate', 'revenue_estimate']

                # Use available columns
                available_display_cols = [col for col in display_cols if col in forecasts.columns]
                display_forecasts = forecasts[available_display_cols].copy()

                # Format dates
                if 'forecast_date' in display_forecasts.columns:
                    display_forecasts['forecast_date'] = pd.to_datetime(display_forecasts['forecast_date']).dt.date
                if 'target_date' in display_forecasts.columns:
                    display_forecasts['target_date'] = pd.to_datetime(display_forecasts['target_date']).dt.date

                st.dataframe(
                    display_forecasts,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'target_price': st.column_config.NumberColumn("Target Price", format="$%.2f"),
                        'confidence_score': st.column_config.ProgressColumn("Confidence", format="%.1%", min_value=0, max_value=1),
                        'expected_return_pct': st.column_config.NumberColumn("Expected Return", format="%.2f%%"),
                        'eps_estimate': st.column_config.NumberColumn("EPS Forecast", format="%.2f"),
                        'revenue_estimate': st.column_config.NumberColumn("Revenue Forecast", format="$%d"),
                        'forecast_date': st.column_config.DateColumn("Forecast Date", format="YYYY-MM-DD"),
                        'target_date': st.column_config.DateColumn("Target Date", format="YYYY-MM-DD")
                    }
                )

                # ========== RECOMMENDATION TIMELINE ==========
                st.markdown("---")
                st.markdown("### ⏰ Trading Signal Timeline")

                timeline_data = forecasts[['forecast_date', 'recommendation', 'confidence_score', 'expected_return_pct']].copy()
                timeline_data = timeline_data.sort_values('forecast_date')
                timeline_data['forecast_date'] = pd.to_datetime(timeline_data['forecast_date'])

                fig_timeline = go.Figure()

                color_map = {
                    'Strong Buy': '#00cc00',
                    'Buy': '#7fff00',
                    'Hold': '#ffff00',
                    'Sell': '#ff9999',
                    'Strong Sell': '#ff0000'
                }

                for rec in timeline_data['recommendation'].unique():
                    rec_data = timeline_data[timeline_data['recommendation'] == rec].copy()

                    # Convert to lists
                    dates_list = rec_data['forecast_date'].tolist()
                    confidence_list = rec_data['confidence_score'].astype(float).tolist()
                    recommendation_list = rec_data['recommendation'].tolist()
                    expected_return_list = rec_data['expected_return_pct'].astype(float).tolist() if 'expected_return_pct' in rec_data.columns else [0] * len(rec_data)

                    marker_sizes = [float(c) * 30 + 10 for c in confidence_list]

                    fig_timeline.add_trace(go.Scatter(
                        x=dates_list,
                        y=[rec] * len(rec_data),
                        mode='markers',
                        name=rec,
                        marker=dict(
                            size=marker_sizes,
                            color=color_map.get(rec, '#cccccc'),
                            opacity=0.7,
                            line=dict(width=2, color='white')
                        ),
                        text=[f"<b>{r}</b><br>Confidence: {c:.1%}<br>Expected Return: {e:.2f}%"
                              for r, c, e in zip(recommendation_list, confidence_list, expected_return_list)],
                        hovertemplate='<b>Forecast Date:</b> %{x}<br>%{text}<extra></extra>'
                    ))

                fig_timeline.update_layout(
                    title=f'{selected_stock} - Trading Signal Timeline',
                    xaxis_title='Forecast Date',
                    yaxis_title='Recommendation',
                    height=400,
                    hovermode='closest',
                    template='plotly_white'
                )

                st.plotly_chart(fig_timeline, use_container_width=True, key='forecast_timeline_scatter')

                # ========== EXPECTED RETURN OVER TIME ==========
                st.markdown("---")
                st.markdown("### 📈 Expected Return Over Time")

                if 'expected_return_pct' in forecasts.columns:
                    return_data = forecasts[['forecast_date', 'expected_return_pct']].copy()
                    return_data = return_data.dropna(subset=['expected_return_pct'])
                    return_data = return_data.sort_values('forecast_date')
                    return_data['forecast_date'] = pd.to_datetime(return_data['forecast_date'])

                    if not return_data.empty:
                        # Convert to lists
                        dates_list = return_data['forecast_date'].tolist()
                        returns_list = return_data['expected_return_pct'].astype(float).tolist()

                        fig_returns = go.Figure()

                        fig_returns.add_trace(go.Scatter(
                            x=dates_list,
                            y=returns_list,
                            mode='lines+markers',
                            name='Expected Return %',
                            fill='tozeroy',
                            line=dict(color='#1f77b4', width=2),
                            marker=dict(size=8),
                            hovertemplate='<b>Date:</b> %{x}<br><b>Expected Return:</b> %{y:.2f}%<extra></extra>'
                        ))

                        fig_returns.add_hline(y=0, line_dash="dash", line_color="red", opacity=0.5)

                        fig_returns.update_layout(
                            title=f'{selected_stock} - Expected Return Forecast',
                            xaxis_title='Forecast Date',
                            yaxis_title='Expected Return (%)',
                            height=400,
                            template='plotly_white'
                        )

                        st.plotly_chart(fig_returns, use_container_width=True, key='forecast_expected_return_line')
                    else:
                        st.info("No expected return data available")
                else:
                    st.info("Expected return data not available")

                # ========== BUY/HOLD/SELL ACTION CALENDAR ==========
                st.markdown("---")
                st.markdown("### 📅 Buy/Hold/Sell Action Calendar")

                action_calendar = forecasts[['forecast_date', 'target_date', 'recommendation',
                                             'confidence_score', 'expected_return_pct', 'target_price']].copy()
                action_calendar = action_calendar.sort_values('forecast_date')
                action_calendar['forecast_date'] = pd.to_datetime(action_calendar['forecast_date']).dt.date
                action_calendar['target_date'] = pd.to_datetime(action_calendar['target_date']).dt.date
                action_calendar['action'] = action_calendar['recommendation']

                # Color mapping
                action_colors = {
                    'Strong Buy': '#00cc00',
                    'Buy': '#7fff00',
                    'Hold': '#ffff00',
                    'Sell': '#ff9999',
                    'Strong Sell': '#ff0000'
                }

                st.subheader("Action Calendar by Forecast Date")

                # Prepare calendar display
                calendar_display = []
                for _, row in action_calendar.iterrows():
                    action = row['action']
                    calendar_display.append({
                        'Forecast Date': row['forecast_date'],
                        'Target Date': row['target_date'],
                        'Action': action,
                        'Confidence': f"{row['confidence_score']:.1%}" if pd.notna(row['confidence_score']) else 'N/A',
                        'Expected Return': f"{row['expected_return_pct']:.2f}%" if pd.notna(row.get('expected_return_pct')) else 'N/A',
                        'Target Price': f"${row['target_price']:.2f}" if pd.notna(row['target_price']) else 'N/A'
                    })

                calendar_df = pd.DataFrame(calendar_display)

                # Display with styling
                st.dataframe(
                    calendar_df,
                    use_container_width=True,
                    hide_index=True
                )

                # ========== TRADING SIGNAL HEATMAP ==========
                st.markdown("---")
                st.subheader("Trading Signal Heatmap (Confidence x Date)")

                # Create pivot for heatmap
                heatmap_data = action_calendar.copy()
                heatmap_data['Date'] = heatmap_data['forecast_date'].astype(str)

                # Pivot table
                heatmap_pivot = heatmap_data.pivot_table(
                    values='confidence_score',
                    index='action',
                    columns='Date',
                    aggfunc='first'
                )

                # Reorder by signal strength
                signal_order = ['Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell']
                heatmap_pivot = heatmap_pivot.reindex([s for s in signal_order if s in heatmap_pivot.index])

                if not heatmap_pivot.empty:
                    fig_heatmap = go.Figure(data=go.Heatmap(
                        z=heatmap_pivot.values,
                        x=heatmap_pivot.columns.tolist(),
                        y=heatmap_pivot.index.tolist(),
                        colorscale=[
                            [0, '#ffffff'],
                            [1, '#0066ff']
                        ],
                        hovertemplate='<b>Date:</b> %{x}<br><b>Signal:</b> %{y}<br><b>Confidence:</b> %{z:.1%}<extra></extra>',
                        colorbar=dict(title='Confidence')
                    ))

                    fig_heatmap.update_layout(
                        title=f'{selected_stock} - Trading Signal Confidence Heatmap',
                        xaxis_title='Forecast Date',
                        yaxis_title='Trading Signal',
                        height=400,
                        template='plotly_white'
                    )

                    st.plotly_chart(fig_heatmap, use_container_width=True, key='forecast_heatmap')
                else:
                    st.info("Not enough data for heatmap")

                # ========== ACTION TIMELINE (Detailed) ==========
                st.markdown("---")
                st.subheader("Action Timeline (When to Buy, Hold, Sell)")

                timeline_html = "<div style='background-color: #f8f9fa; padding: 20px; border-radius: 10px;'>"
                timeline_html += "<h4 style='text-align: center; color: #2c3e50; margin-bottom: 20px;'>Trading Actions by Date</h4>"

                for _, row in action_calendar.iterrows():
                    action = row['action']
                    color = action_colors.get(action, '#cccccc')
                    icon = '🟢' if 'Buy' in action else ('🟡' if action == 'Hold' else '🔴')

                    expected_ret = row.get('expected_return_pct')
                    expected_ret_str = f"{expected_ret:.2f}%" if pd.notna(expected_ret) else 'N/A'

                    conf = row.get('confidence_score')
                    conf_str = f"{conf:.1%}" if pd.notna(conf) else 'N/A'

                    target_p = row.get('target_price')
                    target_p_str = f"${target_p:.2f}" if pd.notna(target_p) else 'N/A'

                    timeline_html += f"<div style='background-color: {color}; padding: 12px 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid {color}; opacity: 0.8;'>"
                    timeline_html += f"<div style='font-weight: bold; color: #000;'>"
                    timeline_html += f"{icon} <b>{action}</b>"
                    timeline_html += "</div>"
                    timeline_html += "<div style='font-size: 0.9em; color: #333; margin-top: 5px;'>"
                    timeline_html += f"<b>Forecast Date:</b> {row['forecast_date']} | <b>Target Date:</b> {row['target_date']}"
                    timeline_html += f"</div>"
                    timeline_html += "<div style='font-size: 0.9em; color: #333;'>"
                    timeline_html += f"<b>Target Price:</b> ${row['target_price']:.2f} |  <b>Expected Return:</b> {row['expected_return_pct']:.2f}% | <b>Confidence:</b> {row['confidence_score']:.1%}"
                    timeline_html += "</div>"
                    timeline_html += "</div>"

                timeline_html += "</div>"
                st.markdown(timeline_html, unsafe_allow_html=True)

                # ========== RECOMMENDATION DISTRIBUTION ==========
                st.markdown("---")
                st.markdown("### 📊 Recommendation Distribution")

                rec_counts = forecasts['recommendation'].value_counts()

                fig_rec = px.bar(
                    x=rec_counts.index.tolist(),
                    y=rec_counts.values.tolist(),
                    color=rec_counts.index.tolist(),
                    color_discrete_map=action_colors,
                    title=f'{selected_stock} - Recommendation Distribution',
                    labels={'x': 'Recommendation', 'y': 'Count'}
                )

                st.plotly_chart(fig_rec, use_container_width=True, key='forecast_recommendation_bar')

                # ========== SUMMARY STATISTICS ==========
                st.markdown("---")
                st.markdown("### 📊 Summary Statistics")

                col1, col2, col3 = st.columns(3)

                with col1:
                    avg_confidence = forecasts['confidence_score'].mean()
                    st.metric("Average Confidence", f"{avg_confidence:.1%}" if pd.notna(avg_confidence) else "N/A")

                with col2:
                    if 'expected_return_pct' in forecasts.columns:
                        avg_return = forecasts['expected_return_pct'].mean()
                        st.metric("Average Expected Return", f"{avg_return:.2f}%" if pd.notna(avg_return) else "N/A")
                    else:
                        st.metric("Average Expected Return", "N/A")

                with col3:
                    bullish_count = len(forecasts[forecasts['recommendation'].isin(['Strong Buy', 'Buy'])])
                    bearish_count = len(forecasts[forecasts['recommendation'].isin(['Sell', 'Strong Sell'])])
                    st.metric("Bullish vs Bearish Signals", f"{bullish_count} : {bearish_count}")

            else:
                st.warning(f"No forecasts available for {selected_stock}")

    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        import traceback
        with st.expander("Show Error Details"):
            st.code(traceback.format_exc())
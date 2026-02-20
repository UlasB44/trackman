import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="TrackMan Financial & Marketing",
    page_icon="📊",
    layout="wide"
)

session = get_active_session()

def run_query(query):
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

COLORS = {
    "primary": "#1a1a2e",
    "secondary": "#4a5568",
    "accent": "#3182ce",
    "success": "#38a169",
    "warning": "#d69e2e",
    "background": "#f7fafc",
    "text": "#2d3748"
}

COLOR_PALETTE = ["#3182ce", "#38a169", "#d69e2e", "#e53e3e", "#805ad5", "#319795"]

CHART_LAYOUT = {
    "plot_bgcolor": "rgba(0,0,0,0)",
    "paper_bgcolor": "rgba(0,0,0,0)",
    "font": {"family": "Inter, system-ui, sans-serif", "color": COLORS["text"]},
    "margin": {"l": 40, "r": 40, "t": 40, "b": 40}
}

CHART_CONFIG = {"displayModeBar": False}

st.markdown("""
<style>
    .main { background-color: #f7fafc; }
    .stMetric { background: white; padding: 1rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    h1, h2, h3 { color: #1a1a2e; font-weight: 500; }
    .stTabs [data-baseweb="tab-list"] { gap: 2rem; }
    .stTabs [data-baseweb="tab"] { font-weight: 500; color: #4a5568; }
</style>
""", unsafe_allow_html=True)

st.title("Financial & Marketing Analytics")
st.caption("TrackMan Business Intelligence Dashboard")

tab1, tab2, tab3, tab4 = st.tabs(["Revenue Overview", "Utilization Metrics", "Marketing Insights", "Forecasting"])

with tab1:
    st.markdown("### Revenue Overview")
    
    daily_revenue = run_query("""
        SELECT 
            DATE_TRUNC('day', START_TIME) as DATE,
            COUNT(DISTINCT SESSION_ID) as SESSIONS,
            COUNT(DISTINCT BAY_ID) as UNIQUE_BAYS,
            AVG(DURATION_MINS) as AVG_DURATION
        FROM TRACKMAN_DW.SILVER.SLV_SESSIONS
        GROUP BY 1
        ORDER BY 1
    """)
    
    if not daily_revenue.empty:
        estimated_revenue = daily_revenue.copy()
        estimated_revenue['ESTIMATED_REVENUE'] = estimated_revenue['SESSIONS'] * 45
        
        total_revenue = estimated_revenue['ESTIMATED_REVENUE'].sum()
        avg_daily = estimated_revenue['ESTIMATED_REVENUE'].mean()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Est. Total Revenue", f"${total_revenue:,.0f}")
        col2.metric("Avg Daily Revenue", f"${avg_daily:,.0f}")
        col3.metric("Total Sessions", f"{daily_revenue['SESSIONS'].sum():,}")
        col4.metric("Avg Session Duration", f"{daily_revenue['AVG_DURATION'].mean():.0f} min")
        
        st.markdown("---")
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=estimated_revenue['DATE'],
            y=estimated_revenue['ESTIMATED_REVENUE'],
            mode='lines+markers',
            name='Daily Revenue',
            line=dict(color=COLOR_PALETTE[0], width=2),
            marker=dict(size=6)
        ))
        fig.update_layout(
            **CHART_LAYOUT,
            xaxis_title="Date",
            yaxis_title="Estimated Revenue ($)",
            yaxis=dict(tickprefix="$")
        )
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        col1, col2 = st.columns(2)
        with col1:
            weekly_revenue = estimated_revenue.copy()
            weekly_revenue['WEEK'] = pd.to_datetime(weekly_revenue['DATE']).dt.isocalendar().week
            weekly_agg = weekly_revenue.groupby('WEEK').agg({
                'ESTIMATED_REVENUE': 'sum',
                'SESSIONS': 'sum'
            }).reset_index()
            
            fig = px.bar(
                weekly_agg,
                x='WEEK',
                y='ESTIMATED_REVENUE',
                color_discrete_sequence=[COLOR_PALETTE[1]]
            )
            fig.update_layout(**CHART_LAYOUT, xaxis_title="Week", yaxis_title="Revenue ($)")
            st.markdown("#### Weekly Revenue")
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        with col2:
            st.markdown("#### Revenue by Day of Week")
            estimated_revenue['DAY_NAME'] = pd.to_datetime(estimated_revenue['DATE']).dt.day_name()
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day_revenue = estimated_revenue.groupby('DAY_NAME')['ESTIMATED_REVENUE'].mean().reindex(day_order)
            
            fig = px.bar(
                x=day_revenue.index,
                y=day_revenue.values,
                color_discrete_sequence=[COLOR_PALETTE[2]]
            )
            fig.update_layout(**CHART_LAYOUT, xaxis_title="Day", yaxis_title="Avg Revenue ($)")
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

with tab2:
    st.markdown("### Utilization Metrics")
    
    bay_utilization = run_query("""
        SELECT 
            BAY_ID,
            COUNT(*) as TOTAL_SESSIONS,
            SUM(DURATION_MINS) as TOTAL_MINUTES,
            AVG(DURATION_MINS) as AVG_SESSION_LENGTH
        FROM TRACKMAN_DW.SILVER.SLV_SESSIONS
        GROUP BY BAY_ID
        ORDER BY TOTAL_SESSIONS DESC
    """)
    
    if not bay_utilization.empty:
        total_capacity_hours = len(bay_utilization) * 24 * 30 * 60
        actual_usage = bay_utilization['TOTAL_MINUTES'].sum()
        utilization_rate = (actual_usage / total_capacity_hours) * 100
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Overall Utilization", f"{utilization_rate:.1f}%")
        col2.metric("Total Bays", len(bay_utilization))
        col3.metric("Avg Session Length", f"{bay_utilization['AVG_SESSION_LENGTH'].mean():.0f} min")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Sessions by Bay")
            fig = px.bar(
                bay_utilization.head(10),
                x='BAY_ID',
                y='TOTAL_SESSIONS',
                color_discrete_sequence=[COLOR_PALETTE[0]]
            )
            fig.update_layout(**CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        with col2:
            st.markdown("#### Usage Distribution")
            fig = px.pie(
                bay_utilization,
                values='TOTAL_MINUTES',
                names='BAY_ID',
                color_discrete_sequence=COLOR_PALETTE
            )
            fig.update_layout(**CHART_LAYOUT)
            fig.update_traces(textposition='inside', textinfo='percent')
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        hourly_pattern = run_query("""
            SELECT 
                HOUR_OF_DAY,
                AVG(OCCUPANCY_PCT) as AVG_OCCUPANCY
            FROM TRACKMAN_DW.GOLD.GLD_BAY_OCCUPATION_HOURLY
            GROUP BY HOUR_OF_DAY
            ORDER BY HOUR_OF_DAY
        """)
        
        if not hourly_pattern.empty:
            st.markdown("#### Peak Hours Analysis")
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=hourly_pattern['HOUR_OF_DAY'],
                y=hourly_pattern['AVG_OCCUPANCY'],
                mode='lines+markers',
                fill='tozeroy',
                line=dict(color=COLOR_PALETTE[0], width=2),
                fillcolor='rgba(49, 130, 206, 0.2)'
            ))
            fig.update_layout(
                **CHART_LAYOUT,
                xaxis_title="Hour of Day",
                yaxis_title="Avg Occupancy %"
            )
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

with tab3:
    st.markdown("### Marketing Insights")
    
    game_performance = run_query("""
        SELECT 
            GAME_NAME,
            SESSIONS_PLAYED,
            TOTAL_PLAYERS,
            AVG_DURATION_MINS,
            AVG_SCORE
        FROM TRACKMAN_DW.GOLD.GLD_GAME_ANALYTICS
        ORDER BY SESSIONS_PLAYED DESC
    """)
    
    if not game_performance.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Game Popularity")
            fig = px.bar(
                game_performance,
                x='GAME_NAME',
                y='SESSIONS_PLAYED',
                color='TOTAL_PLAYERS',
                color_continuous_scale='Blues'
            )
            fig.update_layout(**CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        with col2:
            st.markdown("#### Player Engagement")
            game_performance['ENGAGEMENT_SCORE'] = (
                game_performance['AVG_DURATION_MINS'] * game_performance['TOTAL_PLAYERS'] / 100
            )
            fig = px.scatter(
                game_performance,
                x='AVG_DURATION_MINS',
                y='TOTAL_PLAYERS',
                size='SESSIONS_PLAYED',
                color='GAME_NAME',
                color_discrete_sequence=COLOR_PALETTE
            )
            fig.update_layout(**CHART_LAYOUT, xaxis_title="Avg Duration (min)", yaxis_title="Total Players")
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        st.markdown("#### Campaign Performance Indicators")
        col1, col2, col3, col4 = st.columns(4)
        
        total_players = game_performance['TOTAL_PLAYERS'].sum()
        avg_engagement = game_performance['AVG_DURATION_MINS'].mean()
        top_game = game_performance.iloc[0]['GAME_NAME']
        conversion_proxy = (total_players / game_performance['SESSIONS_PLAYED'].sum()) * 100
        
        col1.metric("Total Unique Players", f"{total_players:,}")
        col2.metric("Avg Engagement Time", f"{avg_engagement:.0f} min")
        col3.metric("Top Performing Game", top_game)
        col4.metric("Players per Session", f"{conversion_proxy:.1f}")

with tab4:
    st.markdown("### Forecasting & Trends")
    
    trend_data = run_query("""
        SELECT 
            DATE_TRUNC('day', START_TIME) as DATE,
            COUNT(*) as SESSIONS,
            COUNT(DISTINCT BAY_ID) as UNIQUE_BAYS
        FROM TRACKMAN_DW.SILVER.SLV_SESSIONS
        GROUP BY 1
        ORDER BY 1
    """)
    
    if not trend_data.empty:
        trend_data['ROLLING_AVG'] = trend_data['SESSIONS'].rolling(window=7, min_periods=1).mean()
        
        recent_avg = trend_data['SESSIONS'].tail(7).mean()
        prior_avg = trend_data['SESSIONS'].head(7).mean()
        growth_rate = ((recent_avg - prior_avg) / prior_avg) * 100 if prior_avg > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("7-Day Avg Sessions", f"{recent_avg:.0f}")
        col2.metric("Growth Trend", f"{growth_rate:+.1f}%")
        col3.metric("Projected Monthly", f"{recent_avg * 30:.0f}")
        
        st.markdown("---")
        
        st.markdown("#### Session Trend with Moving Average")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=trend_data['DATE'],
            y=trend_data['SESSIONS'],
            name='Daily Sessions',
            marker_color=COLOR_PALETTE[0],
            opacity=0.6
        ))
        fig.add_trace(go.Scatter(
            x=trend_data['DATE'],
            y=trend_data['ROLLING_AVG'],
            name='7-Day Moving Avg',
            line=dict(color=COLOR_PALETTE[3], width=3)
        ))
        fig.update_layout(**CHART_LAYOUT, barmode='overlay')
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        st.markdown("#### Capacity Planning")
        col1, col2 = st.columns(2)
        
        with col1:
            if 'UNIQUE_BAYS' in trend_data.columns:
                fig = px.line(
                    trend_data,
                    x='DATE',
                    y='UNIQUE_BAYS',
                    markers=True
                )
                fig.update_traces(line_color=COLOR_PALETTE[1])
                fig.update_layout(**CHART_LAYOUT, xaxis_title="Date", yaxis_title="Active Bays")
                st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        with col2:
            st.markdown("##### Key Metrics Summary")
            summary_df = pd.DataFrame({
                'Metric': ['Peak Sessions/Day', 'Avg Sessions/Day', 'Min Sessions/Day', 'Total Sessions'],
                'Value': [
                    f"{trend_data['SESSIONS'].max():,}",
                    f"{trend_data['SESSIONS'].mean():.0f}",
                    f"{trend_data['SESSIONS'].min():,}",
                    f"{trend_data['SESSIONS'].sum():,}"
                ]
            })
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption("TrackMan Financial Analytics | Data refreshed from Snowflake")

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Trackman Analytics", 
    layout="wide", 
    page_icon="T",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    .block-container {padding-top: 2rem; padding-bottom: 2rem;}
    h1 {font-weight: 300; letter-spacing: -0.5px; color: #1a1a1a;}
    h2, h3 {font-weight: 400; color: #333;}
    .stTabs [data-baseweb="tab-list"] {gap: 8px; background-color: transparent;}
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 4px;
        color: #666;
        font-weight: 400;
        padding: 8px 16px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #f5f5f5;
        color: #1a1a1a;
    }
    .stMetric {background-color: #fafafa; padding: 1rem; border-radius: 4px;}
    div[data-testid="stMetricValue"] {font-weight: 300; font-size: 2rem;}
    div[data-testid="stMetricLabel"] {font-weight: 400; color: #666; text-transform: uppercase; font-size: 0.75rem; letter-spacing: 0.5px;}
</style>
""", unsafe_allow_html=True)

session = get_active_session()

@st.cache_data(ttl=300)
def run_query(query):
    return session.sql(query).to_pandas()

CHART_CONFIG = {
    'displayModeBar': False
}

CHART_LAYOUT = {
    'font': {'family': 'Inter, system-ui, sans-serif', 'color': '#333'},
    'paper_bgcolor': 'rgba(0,0,0,0)',
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'margin': {'t': 40, 'b': 40, 'l': 40, 'r': 40},
}

COLOR_PALETTE = ['#2563eb', '#64748b', '#0891b2', '#059669', '#d97706', '#dc2626']

st.title("Trackman Analytics")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Overview", "Scorecards", "Bay Occupation", "Course Play", "Games", "Practice"
])

with tab1:
    st.markdown("### Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    sessions_data = run_query("SELECT COUNT(*) as cnt FROM TRACKMAN_DW.BRONZE.BRZ_SESSIONS")
    scorecards_data = run_query("SELECT COUNT(*) as cnt FROM TRACKMAN_DW.BRONZE.BRZ_SCORECARDS")
    players_data = run_query("SELECT COUNT(DISTINCT player_id) as cnt FROM TRACKMAN_DW.BRONZE.BRZ_SESSIONS")
    shots_data = run_query("SELECT COUNT(*) as cnt FROM TRACKMAN_DW.BRONZE.BRZ_SHOTS")
    
    col1.metric("Sessions", f"{sessions_data['CNT'].iloc[0]:,}")
    col2.metric("Scorecards", f"{scorecards_data['CNT'].iloc[0]:,}")
    col3.metric("Players", f"{players_data['CNT'].iloc[0]:,}")
    col4.metric("Shots", f"{shots_data['CNT'].iloc[0]:,}")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Weekly Trends")
        trends = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_WEEKLY_TRENDS ORDER BY WEEK_START")
        if not trends.empty:
            fig = px.line(trends, x='WEEK_START', y='SESSIONS', color='SESSION_CATEGORY',
                         color_discrete_sequence=COLOR_PALETTE)
            fig.update_layout(**CHART_LAYOUT, showlegend=True, legend=dict(orientation='h', y=-0.2))
            fig.update_traces(line=dict(width=2))
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
    
    with col2:
        st.markdown("### Session Distribution")
        overview = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_SESSION_OVERVIEW")
        if not overview.empty:
            fig = px.bar(overview, x='SESSION_TYPE', y='TOTAL_SESSIONS', color='SESSION_CATEGORY',
                        color_discrete_sequence=COLOR_PALETTE)
            fig.update_layout(**CHART_LAYOUT, showlegend=True, legend=dict(orientation='h', y=-0.2))
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

with tab2:
    st.markdown("### Scorecard Analysis")
    
    courses = run_query("SELECT DISTINCT course_name FROM TRACKMAN_DW.BRONZE.BRZ_SCORECARDS ORDER BY 1")
    if not courses.empty:
        selected_course = st.selectbox("Course", courses['COURSE_NAME'].tolist(), label_visibility="collapsed")
        
        scorecards_df = run_query(f"""
            SELECT player_name as "Player", tee as "Tee", holes_played as "Holes", 
                   total_strokes as "Strokes", score_vs_par as "vs Par", 
                   ROUND(gir_percentage, 1) as "GIR %", ROUND(putts_per_hole, 2) as "Putts/Hole",
                   round_date as "Date"
            FROM TRACKMAN_DW.BRONZE.BRZ_SCORECARDS 
            WHERE course_name = '{selected_course}'
            ORDER BY round_date DESC LIMIT 50
        """)
        
        if not scorecards_df.empty:
            col1, col2, col3 = st.columns(3)
            col1.metric("Average Score", f"{scorecards_df['Strokes'].mean():.1f}")
            avg_vs_par = scorecards_df['vs Par'].mean()
            col2.metric("Average vs Par", f"+{avg_vs_par:.1f}" if avg_vs_par > 0 else f"{avg_vs_par:.1f}")
            col3.metric("Rounds Played", f"{len(scorecards_df)}")
            
            st.markdown("---")
            st.dataframe(scorecards_df, use_container_width=True, hide_index=True)

with tab3:
    st.markdown("### Bay Occupation")
    
    heatmap_data = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_BAY_OCCUPATION_HEATMAP")
    
    if not heatmap_data.empty and heatmap_data['HOUR'].notna().any():
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        pivot = heatmap_data.pivot(index='DAY_OF_WEEK', columns='HOUR', values='OCCUPIED_COUNT')
        pivot = pivot.reindex(day_order)
        
        fig = px.imshow(pivot, 
                        labels=dict(x="Hour", y="", color="Bookings"),
                        color_continuous_scale=[[0, '#f8fafc'], [0.5, '#93c5fd'], [1, '#1d4ed8']],
                        aspect="auto")
        fig.update_layout(
            **CHART_LAYOUT,
            height=350,
            coloraxis_colorbar=dict(title="", thickness=15, len=0.6)
        )
        fig.update_xaxes(side="bottom", tickmode='linear', dtick=2)
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
    else:
        st.info("No hourly data available for heatmap visualization.")
    
    st.markdown("---")
    st.markdown("### Bay Performance")
    bay_insights = run_query("""
        SELECT BAY_ID as "Bay", FACILITY_NAME as "Facility", 
               TOTAL_BOOKED_HOURS as "Hours Booked", 
               AVG_PLAYERS_PER_BOOKING as "Avg Players",
               DAYS_WITH_ACTIVITY as "Active Days"
        FROM TRACKMAN_DW.GOLD.GLD_BAY_INSIGHTS 
        ORDER BY TOTAL_BOOKED_HOURS DESC LIMIT 20
    """)
    if not bay_insights.empty:
        st.dataframe(bay_insights, use_container_width=True, hide_index=True)

with tab4:
    st.markdown("### Course Performance")
    
    course_stats = run_query("""
        SELECT COURSE_NAME as "Course", ROUNDS_PLAYED as "Rounds", UNIQUE_PLAYERS as "Players",
               AVG_SCORE as "Avg Score", AVG_VS_PAR as "vs Par", 
               AVG_GIR_PCT as "GIR %", COMPLETION_PCT as "Completion %"
        FROM TRACKMAN_DW.GOLD.GLD_COURSE_ANALYTICS 
        ORDER BY ROUNDS_PLAYED DESC
    """)
    
    if not course_stats.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(course_stats.head(10), x='Course', y='Rounds',
                        color_discrete_sequence=[COLOR_PALETTE[0]])
            fig.update_layout(**CHART_LAYOUT, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        with col2:
            fig = px.scatter(course_stats, x='Avg Score', y='Completion %', 
                            size='Rounds', hover_name='Course',
                            color_discrete_sequence=[COLOR_PALETTE[0]])
            fig.update_layout(**CHART_LAYOUT)
            fig.update_traces(marker=dict(opacity=0.7))
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        st.markdown("---")
        st.dataframe(course_stats, use_container_width=True, hide_index=True)

with tab5:
    st.markdown("### Games Overview")
    
    games = run_query("""
        SELECT GAME_NAME as "Game", SESSIONS_PLAYED as "Sessions", 
               TOTAL_PLAYERS as "Players", AVG_DURATION as "Avg Duration (min)"
        FROM TRACKMAN_DW.GOLD.GLD_GAME_ANALYTICS 
        ORDER BY SESSIONS_PLAYED DESC
    """)
    
    if not games.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Sessions", f"{games['Sessions'].sum():,}")
        col2.metric("Total Players", f"{games['Players'].sum():,}")
        col3.metric("Most Popular", games.iloc[0]['Game'])
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(games, values='Sessions', names='Game',
                        color_discrete_sequence=COLOR_PALETTE)
            fig.update_layout(**CHART_LAYOUT)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        with col2:
            st.dataframe(games, use_container_width=True, hide_index=True)

with tab6:
    st.markdown("### Shot Analysis by Club")
    
    clubs = run_query("""
        SELECT CLUB_ID as "Club", TOTAL_SHOTS as "Shots", 
               AVG_BALL_SPEED as "Ball Speed", AVG_CLUB_SPEED as "Club Speed",
               AVG_SMASH_FACTOR as "Smash Factor", AVG_CARRY as "Carry (yds)",
               AVG_TOTAL_DISTANCE as "Total (yds)"
        FROM TRACKMAN_DW.GOLD.GLD_CLUB_PERFORMANCE 
        ORDER BY TOTAL_SHOTS DESC
    """)
    
    if not clubs.empty:
        fig = px.bar(clubs, x='Club', y='Carry (yds)',
                    color_discrete_sequence=[COLOR_PALETTE[0]])
        fig.update_layout(**CHART_LAYOUT, title='Average Carry Distance')
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.scatter(clubs, x='Club Speed', y='Ball Speed', 
                            size='Shots', hover_name='Club',
                            color_discrete_sequence=[COLOR_PALETTE[0]])
            fig.update_layout(**CHART_LAYOUT, title='Speed Correlation')
            fig.update_traces(marker=dict(opacity=0.7))
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        with col2:
            fig = px.bar(clubs, x='Club', y='Smash Factor',
                        color_discrete_sequence=[COLOR_PALETTE[2]])
            fig.update_layout(**CHART_LAYOUT, title='Smash Factor')
            st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        
        st.markdown("---")
        st.dataframe(clubs, use_container_width=True, hide_index=True)

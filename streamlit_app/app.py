import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.connector import connect
import os

st.set_page_config(page_title="Trackman Portal", layout="wide", page_icon="⛳")

@st.cache_resource
def get_connection():
    return connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "DEMO_USWEST")

@st.cache_data(ttl=300)
def run_query(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

st.title("⛳ Trackman Analytics Portal")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Overview", "🎯 Scorecards", "📅 Bay Occupation", "🏌️ Course Play", "🎮 Games", "📈 Practice"
])

with tab1:
    st.header("Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    sessions = run_query("SELECT COUNT(*) as cnt FROM TRACKMAN_DW.BRONZE.BRZ_SESSIONS")
    scorecards = run_query("SELECT COUNT(*) as cnt FROM TRACKMAN_DW.BRONZE.BRZ_SCORECARDS")
    players = run_query("SELECT COUNT(DISTINCT player_id) as cnt FROM TRACKMAN_DW.BRONZE.BRZ_SESSIONS")
    shots = run_query("SELECT COUNT(*) as cnt FROM TRACKMAN_DW.BRONZE.BRZ_SHOTS")
    
    col1.metric("Total Sessions", f"{sessions['CNT'].iloc[0]:,}")
    col2.metric("Scorecards", f"{scorecards['CNT'].iloc[0]:,}")
    col3.metric("Active Players", f"{players['CNT'].iloc[0]:,}")
    col4.metric("Total Shots", f"{shots['CNT'].iloc[0]:,}")
    
    st.subheader("Weekly Trends")
    trends = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_WEEKLY_TRENDS ORDER BY WEEK_START")
    fig = px.line(trends, x='WEEK_START', y='SESSIONS', color='SESSION_CATEGORY', markers=True)
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Session Distribution")
    overview = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_SESSION_OVERVIEW")
    fig2 = px.bar(overview, x='SESSION_TYPE', y='TOTAL_SESSIONS', color='SESSION_CATEGORY')
    st.plotly_chart(fig2, use_container_width=True)

with tab2:
    st.header("Scorecards")
    
    courses = run_query("SELECT DISTINCT course_name FROM TRACKMAN_DW.BRONZE.BRZ_SCORECARDS ORDER BY 1")
    selected_course = st.selectbox("Select Course", courses['COURSE_NAME'].tolist())
    
    scorecards_df = run_query(f"""
        SELECT player_name, tee, holes_played, total_strokes, score_vs_par, 
               gir_percentage, putts_per_hole, round_date
        FROM TRACKMAN_DW.BRONZE.BRZ_SCORECARDS 
        WHERE course_name = '{selected_course}'
        ORDER BY round_date DESC LIMIT 50
    """)
    
    col1, col2 = st.columns(2)
    with col1:
        avg_score = scorecards_df['TOTAL_STROKES'].mean()
        st.metric("Avg Score", f"{avg_score:.1f}")
    with col2:
        avg_vs_par = scorecards_df['SCORE_VS_PAR'].mean()
        st.metric("Avg vs Par", f"+{avg_vs_par:.1f}" if avg_vs_par > 0 else f"{avg_vs_par:.1f}")
    
    st.dataframe(scorecards_df, use_container_width=True)
    
    st.subheader("Hole-by-Hole Analysis")
    if st.checkbox("Show hole details"):
        holes = run_query(f"""
            SELECT h.hole_number, h.par, h.yardage, h.stroke_index,
                   ROUND(AVG(hs.strokes), 2) as avg_strokes,
                   ROUND(AVG(hs.vs_par), 2) as avg_vs_par,
                   ROUND(SUM(CASE WHEN hs.gir THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) as gir_pct
            FROM TRACKMAN_DW.BRONZE.BRZ_HOLE_SCORES hs
            JOIN TRACKMAN_DW.BRONZE.BRZ_SCORECARDS s ON hs.scorecard_id = s.scorecard_id
            JOIN (SELECT DISTINCT course_id, course_name FROM TRACKMAN_DW.BRONZE.BRZ_SCORECARDS) c 
                ON s.course_id = c.course_id
            JOIN (SELECT $1:course_id::VARCHAR as course_id, $1:hole_number::INT as hole_number,
                         $1:par::INT as par, $1:yardage::INT as yardage, $1:stroke_index::INT as stroke_index
                  FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/dimensions/dim_course_holes.parquet
                  (FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')) h
                ON hs.course_id = h.course_id AND hs.hole_number = h.hole_number
            WHERE c.course_name = '{selected_course}'
            GROUP BY h.hole_number, h.par, h.yardage, h.stroke_index
            ORDER BY h.hole_number
        """)
        st.dataframe(holes, use_container_width=True)

with tab3:
    st.header("Bay Occupation Heatmap")
    
    heatmap_data = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_BAY_OCCUPATION_HEATMAP")
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot = heatmap_data.pivot(index='DAY_OF_WEEK', columns='HOUR', values='OCCUPIED_COUNT')
    pivot = pivot.reindex(day_order)
    
    fig = px.imshow(pivot, 
                    labels=dict(x="Hour", y="Day", color="Bookings"),
                    color_continuous_scale="Blues",
                    aspect="auto")
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Bay Insights")
    bay_insights = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_BAY_INSIGHTS ORDER BY TOTAL_BOOKED_HOURS DESC LIMIT 20")
    st.dataframe(bay_insights, use_container_width=True)

with tab4:
    st.header("Course Play Analytics")
    
    course_stats = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_COURSE_ANALYTICS ORDER BY ROUNDS_PLAYED DESC")
    
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(course_stats, x='COURSE_NAME', y='ROUNDS_PLAYED', title="Rounds by Course")
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.scatter(course_stats, x='AVG_SCORE', y='COMPLETION_PCT', 
                        size='ROUNDS_PLAYED', hover_name='COURSE_NAME',
                        title="Avg Score vs Completion Rate")
        st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(course_stats, use_container_width=True)

with tab5:
    st.header("Games Analytics")
    
    games = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_GAME_ANALYTICS ORDER BY SESSIONS_PLAYED DESC")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Game Sessions", f"{games['SESSIONS_PLAYED'].sum():,}")
    col2.metric("Total Players", f"{games['TOTAL_PLAYERS'].sum():,}")
    col3.metric("Most Popular", games.iloc[0]['GAME_NAME'])
    
    fig = px.pie(games, values='SESSIONS_PLAYED', names='GAME_NAME', title="Games Distribution")
    st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(games, use_container_width=True)

with tab6:
    st.header("Practice & Shot Analysis")
    
    clubs = run_query("SELECT * FROM TRACKMAN_DW.GOLD.GLD_CLUB_PERFORMANCE ORDER BY TOTAL_SHOTS DESC")
    
    fig = px.bar(clubs, x='CLUB_ID', y='AVG_CARRY', title="Average Carry Distance by Club")
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        fig = px.scatter(clubs, x='AVG_CLUB_SPEED', y='AVG_BALL_SPEED', 
                        size='TOTAL_SHOTS', hover_name='CLUB_ID',
                        title="Club Speed vs Ball Speed")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(clubs, x='CLUB_ID', y='AVG_SMASH_FACTOR', title="Smash Factor by Club")
        st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(clubs, use_container_width=True)

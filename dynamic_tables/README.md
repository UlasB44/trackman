# Dynamic Tables

This directory contains Snowflake Dynamic Table definitions that automatically refresh and maintain materialized data.

## Structure

```
dynamic_tables/
├── silver/          # Cleaned dimension tables
│   ├── slv_courses.sql
│   ├── slv_facilities.sql
│   └── slv_players.sql
└── gold/            # Analytics aggregations
    ├── dt_bay_insights.sql
    ├── dt_bay_occupation_heatmap.sql
    ├── dt_club_performance.sql
    ├── dt_course_analytics.sql
    ├── dt_game_analytics.sql
    ├── dt_session_overview.sql
    └── dt_weekly_trends.sql
```

## Configuration

All dynamic tables use:
- **Target Lag**: 30 minutes
- **Refresh Mode**: AUTO
- **Initialize**: ON_CREATE
- **Warehouse**: COMPUTE_WH

## Silver Layer Dynamic Tables

### SLV_COURSES
- **Source**: BRONZE.BASE_COURSES
- **Purpose**: Cleaned course dimension with formatted names and ratings
- **Refresh**: FULL (contains CURRENT_TIMESTAMP)

### SLV_FACILITIES
- **Source**: BRONZE.BASE_FACILITIES
- **Purpose**: Facility dimension with operating hours and bay counts
- **Refresh**: FULL (contains CURRENT_TIMESTAMP)

### SLV_PLAYERS
- **Source**: BRONZE.BASE_PLAYERS
- **Purpose**: Player dimension with handicaps and membership info
- **Refresh**: FULL (contains CURRENT_TIMESTAMP)

## Gold Layer Dynamic Tables

### DT_SESSION_OVERVIEW
- **Source**: SILVER.SLV_SESSIONS
- **Purpose**: Session metrics by category and type
- **Metrics**: Total sessions, players, duration, unique players

### DT_BAY_INSIGHTS
- **Sources**: SILVER.SLV_BAY_BOOKINGS, SILVER.SLV_FACILITIES
- **Purpose**: Bay utilization and booking patterns
- **Metrics**: Booked hours, avg players, activity days

### DT_BAY_OCCUPATION_HEATMAP
- **Source**: SILVER.SLV_BAY_BOOKINGS
- **Purpose**: Day/hour heatmap of bay occupancy
- **Metrics**: Bookings, occupied slots, occupancy %, avg players

### DT_COURSE_ANALYTICS
- **Source**: SILVER.SLV_SCORECARDS
- **Purpose**: Course performance and completion metrics
- **Metrics**: Rounds played, scores, GIR%, FIR%, completion rate

### DT_CLUB_PERFORMANCE
- **Source**: SILVER.SLV_SHOTS
- **Purpose**: Club-by-club shot analytics
- **Metrics**: Ball speed, club speed, smash factor, distances

### DT_WEEKLY_TRENDS
- **Source**: SILVER.SLV_SESSIONS
- **Purpose**: Weekly session trends by category
- **Metrics**: Sessions, unique players, total hours

### DT_GAME_ANALYTICS
- **Source**: SILVER.SLV_SESSIONS
- **Purpose**: Game session analytics by category and time
- **Metrics**: Total sessions, avg duration, unique players, group size

## Deployment

To deploy these dynamic tables to Snowflake:

```sql
-- Deploy Silver Layer
@dynamic_tables/silver/slv_courses.sql
@dynamic_tables/silver/slv_facilities.sql
@dynamic_tables/silver/slv_players.sql

-- Deploy Gold Layer
@dynamic_tables/gold/dt_session_overview.sql
@dynamic_tables/gold/dt_bay_insights.sql
@dynamic_tables/gold/dt_course_analytics.sql
@dynamic_tables/gold/dt_club_performance.sql
@dynamic_tables/gold/dt_weekly_trends.sql
@dynamic_tables/gold/dt_bay_occupation_heatmap.sql
@dynamic_tables/gold/dt_game_analytics.sql
```

Or use the Snowflake CLI:
```bash
snow sql -f dynamic_tables/silver/slv_courses.sql --connection DEMO_USWEST
```

## Monitoring

Check dynamic table status:
```sql
SHOW DYNAMIC TABLES IN DATABASE TRACKMAN_DW;

SELECT 
    name,
    schema_name,
    scheduling_state,
    target_lag,
    refresh_mode,
    data_timestamp
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY schema_name, name;
```

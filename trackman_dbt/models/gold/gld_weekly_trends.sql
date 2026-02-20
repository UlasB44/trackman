SELECT
    DATE_TRUNC('week', session_date) AS week_start,
    session_category,
    COUNT(*) AS sessions,
    COUNT(DISTINCT player_id) AS unique_players,
    SUM(duration_minutes) AS total_hours
FROM {{ ref('slv_sessions') }}
GROUP BY DATE_TRUNC('week', session_date), session_category
ORDER BY week_start

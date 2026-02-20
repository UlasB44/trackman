SELECT
    session_category,
    session_type,
    COUNT(*) AS total_sessions,
    SUM(num_players) AS total_players,
    ROUND(AVG(duration_minutes), 1) AS avg_duration_mins,
    COUNT(DISTINCT player_id) AS unique_players
FROM {{ ref('slv_sessions') }}
GROUP BY session_category, session_type

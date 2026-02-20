SELECT
    g.game_type_id,
    g.game_name,
    COUNT(*) AS sessions_played,
    SUM(g.num_players) AS total_players,
    ROUND(AVG(g.score), 1) AS avg_score,
    ROUND(AVG(g.num_shots), 1) AS avg_shots,
    ROUND(AVG(g.duration_minutes), 1) AS avg_duration_mins
FROM {{ ref('slv_game_sessions') }} g
GROUP BY g.game_type_id, g.game_name

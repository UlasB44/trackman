SELECT
    course_id,
    course_name,
    COUNT(DISTINCT scorecard_id) AS rounds_played,
    COUNT(DISTINCT player_id) AS unique_players,
    ROUND(AVG(total_strokes), 1) AS avg_score,
    ROUND(AVG(score_vs_par), 1) AS avg_vs_par,
    ROUND(AVG(gir_percentage), 1) AS avg_gir_pct,
    ROUND(AVG(fir_percentage), 1) AS avg_fir_pct,
    ROUND(AVG(putts_per_hole), 2) AS avg_putts_per_hole,
    COUNT(CASE WHEN is_complete THEN 1 END) AS completed_rounds,
    ROUND(COUNT(CASE WHEN is_complete THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS completion_pct
FROM {{ ref('slv_scorecards') }}
GROUP BY course_id, course_name

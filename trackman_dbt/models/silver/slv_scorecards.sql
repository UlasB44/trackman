{{
    config(
        materialized='incremental',
        unique_key='scorecard_id'
    )
}}

SELECT
    scorecard_id,
    session_id,
    player_id,
    player_name,
    course_id,
    course_name,
    tee,
    holes_played,
    total_strokes,
    front_nine,
    back_nine,
    total_par,
    score_vs_par,
    gross_score,
    net_score,
    handicap,
    gir_count,
    gir_percentage,
    fir_percentage,
    putts_total,
    putts_per_hole,
    is_complete,
    round_date,
    round_datetime,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ ref('brz_scorecards') }}
{% if is_incremental() %}
WHERE round_date > (SELECT MAX(round_date) FROM {{ this }})
{% endif %}

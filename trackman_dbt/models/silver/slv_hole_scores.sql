{{
    config(
        materialized='incremental',
        unique_key='hole_score_id'
    )
}}

SELECT
    hole_score_id,
    scorecard_id,
    session_id,
    player_id,
    course_id,
    hole_number,
    par,
    yardage,
    stroke_index,
    strokes,
    putts,
    gir,
    fir,
    score_type,
    vs_par,
    score_date,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ ref('brz_hole_scores') }}
{% if is_incremental() %}
WHERE score_date > (SELECT MAX(score_date) FROM {{ this }})
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key='session_id'
    )
}}

SELECT
    session_id,
    player_id,
    facility_id,
    bay_id,
    session_type,
    session_category,
    started_at,
    ended_at,
    duration_minutes,
    session_date,
    day_of_week,
    hour_of_day,
    num_players,
    is_logged_in,
    is_guest,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ ref('brz_sessions') }}
{% if is_incremental() %}
WHERE session_date > (SELECT MAX(session_date) FROM {{ this }})
{% endif %}

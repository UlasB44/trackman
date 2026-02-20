{{
    config(
        materialized='incremental',
        unique_key='game_session_id'
    )
}}

SELECT
    game_session_id,
    session_id,
    game_type_id,
    game_name,
    num_players,
    num_shots,
    total_strokes,
    score,
    duration_minutes,
    game_date,
    started_at,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ ref('brz_game_sessions') }}
{% if is_incremental() %}
WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
{% endif %}

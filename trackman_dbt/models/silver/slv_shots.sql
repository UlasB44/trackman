{{
    config(
        materialized='incremental',
        unique_key='shot_id'
    )
}}

SELECT
    shot_id,
    session_id,
    player_id,
    bay_id,
    club_id,
    shot_number,
    shot_timestamp,
    shot_date,
    ball_speed,
    club_speed,
    smash_factor,
    launch_angle,
    spin_rate,
    spin_axis,
    carry_distance,
    total_distance,
    apex_height,
    attack_angle,
    face_angle,
    club_path,
    face_to_path,
    dynamic_loft,
    lateral_deviation,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ ref('brz_shots') }}
{% if is_incremental() %}
WHERE shot_date > (SELECT MAX(shot_date) FROM {{ this }})
{% endif %}

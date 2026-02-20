{{
    config(
        materialized='incremental',
        unique_key='booking_id'
    )
}}

SELECT
    booking_id,
    bay_id,
    facility_id,
    booking_date,
    hour,
    day_of_week,
    is_weekend,
    is_booked,
    is_occupied,
    session_type,
    num_players,
    revenue,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ ref('brz_bay_bookings') }}
{% if is_incremental() %}
WHERE booking_date > (SELECT MAX(booking_date) FROM {{ this }})
{% endif %}

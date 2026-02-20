SELECT
    b.hour,
    b.day_of_week,
    COUNT(CASE WHEN b.is_occupied THEN 1 END) AS occupied_count,
    COUNT(*) AS total_slots,
    ROUND(COUNT(CASE WHEN b.is_occupied THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS occupancy_pct
FROM {{ ref('slv_bay_bookings') }} b
GROUP BY b.hour, b.day_of_week

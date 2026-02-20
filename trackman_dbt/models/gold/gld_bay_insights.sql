SELECT
    b.bay_id,
    f.facility_name,
    COUNT(CASE WHEN b.is_occupied THEN 1 END) AS total_booked_hours,
    ROUND(SUM(b.revenue), 2) AS total_revenue,
    ROUND(AVG(b.num_players), 1) AS avg_players_per_booking,
    COUNT(DISTINCT b.booking_date) AS days_with_activity
FROM {{ ref('slv_bay_bookings') }} b
JOIN {{ ref('brz_facilities') }} f ON b.facility_id = f.facility_id
GROUP BY b.bay_id, f.facility_name

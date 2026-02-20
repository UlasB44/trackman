SELECT
    $1:booking_id::VARCHAR AS booking_id,
    $1:bay_id::VARCHAR AS bay_id,
    $1:facility_id::VARCHAR AS facility_id,
    $1:booking_date::DATE AS booking_date,
    $1:hour::INT AS hour,
    $1:day_of_week::VARCHAR AS day_of_week,
    $1:is_weekend::BOOLEAN AS is_weekend,
    $1:is_booked::BOOLEAN AS is_booked,
    $1:is_occupied::BOOLEAN AS is_occupied,
    $1:session_type::VARCHAR AS session_type,
    $1:num_players::INT AS num_players,
    $1:revenue::FLOAT AS revenue
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/facts/fact_bay_bookings.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

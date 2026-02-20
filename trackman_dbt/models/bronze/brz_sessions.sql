SELECT
    $1:session_id::VARCHAR AS session_id,
    $1:player_id::VARCHAR AS player_id,
    $1:facility_id::VARCHAR AS facility_id,
    $1:bay_id::VARCHAR AS bay_id,
    $1:session_type::VARCHAR AS session_type,
    $1:session_category::VARCHAR AS session_category,
    $1:started_at::TIMESTAMP AS started_at,
    $1:ended_at::TIMESTAMP AS ended_at,
    $1:duration_minutes::INT AS duration_minutes,
    $1:session_date::DATE AS session_date,
    $1:day_of_week::VARCHAR AS day_of_week,
    $1:hour_of_day::INT AS hour_of_day,
    $1:num_players::INT AS num_players,
    $1:is_logged_in::BOOLEAN AS is_logged_in,
    $1:is_guest::BOOLEAN AS is_guest
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/facts/fact_sessions.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

SELECT
    $1:game_session_id::VARCHAR AS game_session_id,
    $1:session_id::VARCHAR AS session_id,
    $1:game_type_id::VARCHAR AS game_type_id,
    $1:game_name::VARCHAR AS game_name,
    $1:num_players::INT AS num_players,
    $1:num_shots::INT AS num_shots,
    $1:total_strokes::INT AS total_strokes,
    $1:score::INT AS score,
    $1:duration_minutes::INT AS duration_minutes,
    $1:game_date::DATE AS game_date,
    $1:started_at::TIMESTAMP AS started_at
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/facts/fact_game_sessions.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

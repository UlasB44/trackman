SELECT
    $1:hole_score_id::VARCHAR AS hole_score_id,
    $1:scorecard_id::VARCHAR AS scorecard_id,
    $1:session_id::VARCHAR AS session_id,
    $1:player_id::VARCHAR AS player_id,
    $1:course_id::VARCHAR AS course_id,
    $1:hole_number::INT AS hole_number,
    $1:par::INT AS par,
    $1:yardage::INT AS yardage,
    $1:stroke_index::INT AS stroke_index,
    $1:strokes::INT AS strokes,
    $1:putts::INT AS putts,
    $1:gir::BOOLEAN AS gir,
    $1:fir::BOOLEAN AS fir,
    $1:score_type::VARCHAR AS score_type,
    $1:vs_par::INT AS vs_par,
    $1:score_date::DATE AS score_date
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/facts/fact_hole_scores.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

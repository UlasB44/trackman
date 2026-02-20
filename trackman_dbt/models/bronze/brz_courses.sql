SELECT
    $1:course_id::VARCHAR AS course_id,
    $1:course_name::VARCHAR AS course_name,
    $1:country::VARCHAR AS country,
    $1:par::INT AS par,
    $1:yardage::INT AS yardage,
    $1:course_rating::FLOAT AS course_rating,
    $1:slope_rating::INT AS slope_rating,
    $1:green_speed_stimp::FLOAT AS green_speed_stimp,
    $1:is_premium::BOOLEAN AS is_premium
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/dimensions/dim_courses.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

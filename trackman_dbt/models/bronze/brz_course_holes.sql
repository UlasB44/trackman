SELECT
    $1:course_id::VARCHAR AS course_id,
    $1:hole_number::INT AS hole_number,
    $1:par::INT AS par,
    $1:yardage::INT AS yardage,
    $1:stroke_index::INT AS stroke_index,
    $1:has_water::BOOLEAN AS has_water,
    $1:has_bunker::BOOLEAN AS has_bunker
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/dimensions/dim_course_holes.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

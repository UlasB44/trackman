SELECT
    $1:facility_id::VARCHAR AS facility_id,
    $1:facility_name::VARCHAR AS facility_name,
    $1:facility_type::VARCHAR AS facility_type,
    $1:country::VARCHAR AS country,
    $1:region::VARCHAR AS region,
    $1:city::VARCHAR AS city,
    $1:num_bays::INT AS num_bays,
    $1:operating_hours_start::INT AS operating_hours_start,
    $1:operating_hours_end::INT AS operating_hours_end,
    $1:is_commercial::BOOLEAN AS is_commercial,
    $1:opening_date::DATE AS opening_date,
    $1:is_active::BOOLEAN AS is_active
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/dimensions/dim_facilities.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

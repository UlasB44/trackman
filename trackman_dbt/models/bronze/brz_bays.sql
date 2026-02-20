SELECT
    $1:bay_id::VARCHAR AS bay_id,
    $1:facility_id::VARCHAR AS facility_id,
    $1:bay_name::VARCHAR AS bay_name,
    $1:bay_number::INT AS bay_number,
    $1:simulator_model::VARCHAR AS simulator_model,
    $1:simulator_name::VARCHAR AS simulator_name,
    $1:serial_number::VARCHAR AS serial_number,
    $1:installation_date::DATE AS installation_date,
    $1:is_active::BOOLEAN AS is_active,
    $1:hourly_rate::FLOAT AS hourly_rate
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/dimensions/dim_bays.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

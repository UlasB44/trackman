SELECT
    $1:game_type_id::VARCHAR AS game_type_id,
    $1:name::VARCHAR AS game_name,
    $1:description::VARCHAR AS description,
    $1:min_shots::INT AS min_shots,
    $1:max_shots::INT AS max_shots
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/dimensions/dim_game_types.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

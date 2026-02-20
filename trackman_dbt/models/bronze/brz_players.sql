SELECT
    $1:player_id::VARCHAR AS player_id,
    $1:player_name::VARCHAR AS player_name,
    $1:first_name::VARCHAR AS first_name,
    $1:last_name::VARCHAR AS last_name,
    $1:email::VARCHAR AS email,
    $1:country::VARCHAR AS country,
    $1:region::VARCHAR AS region,
    $1:handicap_index::FLOAT AS handicap_index,
    $1:skill_factor::FLOAT AS skill_factor,
    $1:club_speed_base::FLOAT AS club_speed_base,
    $1:consistency_rating::FLOAT AS consistency_rating,
    $1:accuracy_rating::FLOAT AS accuracy_rating,
    $1:age::INT AS age,
    $1:gender::VARCHAR AS gender,
    $1:membership_tier::VARCHAR AS membership_tier,
    $1:tee_preference::VARCHAR AS tee_preference,
    $1:created_at::TIMESTAMP AS created_at,
    $1:is_active::BOOLEAN AS is_active,
    $1:is_guest::BOOLEAN AS is_guest
FROM @TRACKMAN_DW.PUBLIC.S3_SOURCE/sample_data/dimensions/dim_players.parquet
(FILE_FORMAT => 'TRACKMAN_DW.STAGING.PARQUET_FORMAT')

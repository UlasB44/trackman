SELECT
    club_id,
    COUNT(*) AS total_shots,
    ROUND(AVG(ball_speed), 1) AS avg_ball_speed,
    ROUND(AVG(club_speed), 1) AS avg_club_speed,
    ROUND(AVG(smash_factor), 3) AS avg_smash_factor,
    ROUND(AVG(launch_angle), 1) AS avg_launch_angle,
    ROUND(AVG(spin_rate), 0) AS avg_spin_rate,
    ROUND(AVG(carry_distance), 1) AS avg_carry,
    ROUND(AVG(total_distance), 1) AS avg_total_distance
FROM {{ ref('slv_shots') }}
GROUP BY club_id

#!/usr/bin/env python3
"""
Trackman Golf Simulator - Enhanced Synthetic Data Generator
Supports: Scorecards, Bay Occupation, Course Play, Games, Practice, Tournaments
"""

import os
import json
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import pandas as pd
import numpy as np
from pathlib import Path

np.random.seed(42)
random.seed(42)

OUTPUT_DIR = Path(__file__).parent.parent / "sample_data"

VIRTUAL_COURSES = [
    {"course_id": "pebble_beach", "name": "Pebble Beach Golf Links", "country": "USA", "par": 72, "yardage": 6828, "rating": 4.8, "slope": 145},
    {"course_id": "st_andrews", "name": "St Andrews Old Course", "country": "Scotland", "par": 72, "yardage": 6721, "rating": 4.7, "slope": 132},
    {"course_id": "augusta_national", "name": "Augusta National Golf Club", "country": "USA", "par": 72, "yardage": 7475, "rating": 4.9, "slope": 148},
    {"course_id": "links_spanish_bay", "name": "The Links at Spanish Bay", "country": "USA", "par": 72, "yardage": 6821, "rating": 4.5, "slope": 140},
    {"course_id": "bethpage_black", "name": "Bethpage Black", "country": "USA", "par": 71, "yardage": 7468, "rating": 4.5, "slope": 155},
    {"course_id": "valhalla", "name": "Valhalla Golf Club", "country": "USA", "par": 72, "yardage": 7458, "rating": 4.6, "slope": 150},
    {"course_id": "adare_manor", "name": "Adare Manor", "country": "Ireland", "par": 72, "yardage": 7509, "rating": 4.7, "slope": 147},
    {"course_id": "cabot_cliffs", "name": "Cabot Cliffs", "country": "Canada", "par": 72, "yardage": 6764, "rating": 4.8, "slope": 142},
    {"course_id": "torrey_pines", "name": "Torrey Pines South", "country": "USA", "par": 72, "yardage": 7698, "rating": 4.4, "slope": 144},
    {"course_id": "innisbrook", "name": "Innisbrook Copperhead", "country": "USA", "par": 71, "yardage": 7340, "rating": 4.3, "slope": 141},
    {"course_id": "hong_kong_gc", "name": "Hong Kong Golf Club", "country": "Hong Kong", "par": 70, "yardage": 6703, "rating": 4.2, "slope": 138},
    {"course_id": "lofoten_links", "name": "Lofoten Links", "country": "Norway", "par": 71, "yardage": 6590, "rating": 4.4, "slope": 135},
    {"course_id": "abu_dhabi", "name": "Abu Dhabi Golf Club - National Course", "country": "UAE", "par": 72, "yardage": 7583, "rating": 4.3, "slope": 143},
    {"course_id": "barnbougle", "name": "Barnbougle Dunes", "country": "Australia", "par": 71, "yardage": 6586, "rating": 4.6, "slope": 137},
    {"course_id": "bellerive", "name": "Bellerive CC", "country": "USA", "par": 71, "yardage": 7547, "rating": 4.4, "slope": 146},
    {"course_id": "black_desert", "name": "Black Desert Resort", "country": "USA", "par": 72, "yardage": 7432, "rating": 4.5, "slope": 144},
    {"course_id": "carnoustie", "name": "Carnoustie Golf Links", "country": "Scotland", "par": 72, "yardage": 7412, "rating": 4.5, "slope": 149},
    {"course_id": "conway_farms", "name": "Conway Farms GC", "country": "USA", "par": 71, "yardage": 7195, "rating": 4.3, "slope": 140},
    {"course_id": "cabot_links", "name": "Cabot Links", "country": "Canada", "par": 70, "yardage": 6810, "rating": 4.5, "slope": 136},
    {"course_id": "achmer_gc", "name": "Achmer GC", "country": "Germany", "par": 72, "yardage": 6450, "rating": 4.1, "slope": 130},
]

GAME_TYPES = [
    {"game_type_id": "bulls_eye", "name": "Bulls Eye", "description": "Accuracy challenge - hit targets", "min_shots": 10, "max_shots": 30},
    {"game_type_id": "capture_flag", "name": "Capture The Flag", "description": "Strategic target capture game", "min_shots": 15, "max_shots": 40},
    {"game_type_id": "closest_pin", "name": "Closest To The Pin", "description": "Precision approach shots", "min_shots": 10, "max_shots": 25},
    {"game_type_id": "streets_neon", "name": "Streets of Neon", "description": "Virtual putting adventure", "min_shots": 18, "max_shots": 36},
    {"game_type_id": "magic_pond", "name": "Magic Pond", "description": "Target creature capture game", "min_shots": 15, "max_shots": 35},
    {"game_type_id": "mystic_sands", "name": "Mystic Sands", "description": "Desert monster challenge", "min_shots": 15, "max_shots": 35},
    {"game_type_id": "cannon_bowl", "name": "Cannon Bowl", "description": "Bowling with golf balls", "min_shots": 10, "max_shots": 21},
    {"game_type_id": "hit_it", "name": "Hit It!", "description": "Long drive competition", "min_shots": 5, "max_shots": 15},
]

SESSION_TYPES = [
    {"type_id": "course_play", "name": "Course Play", "category": "courses", "avg_duration": 120},
    {"type_id": "practice_range", "name": "Practice Range", "category": "practice", "avg_duration": 45},
    {"type_id": "on_course_practice", "name": "On Course Practice", "category": "practice", "avg_duration": 60},
    {"type_id": "combine_test", "name": "Combine Test", "category": "practice", "avg_duration": 50},
    {"type_id": "game", "name": "Game", "category": "game", "avg_duration": 30},
    {"type_id": "tournament", "name": "Tournament", "category": "tournament", "avg_duration": 150},
    {"type_id": "lesson", "name": "Lesson/Coaching", "category": "practice", "avg_duration": 60},
    {"type_id": "fitting", "name": "Club Fitting", "category": "practice", "avg_duration": 90},
]

CLUB_SPECS = {
    "Driver": {"loft": 10.5, "typical_distance": (220, 280), "spin_range": (2000, 3000), "smash_target": 1.48},
    "3-Wood": {"loft": 15.0, "typical_distance": (200, 250), "spin_range": (3000, 4500), "smash_target": 1.45},
    "5-Wood": {"loft": 18.0, "typical_distance": (180, 230), "spin_range": (3500, 5000), "smash_target": 1.43},
    "Hybrid": {"loft": 21.0, "typical_distance": (170, 210), "spin_range": (4000, 5500), "smash_target": 1.40},
    "4-Iron": {"loft": 24.0, "typical_distance": (170, 200), "spin_range": (4500, 5500), "smash_target": 1.36},
    "5-Iron": {"loft": 27.0, "typical_distance": (160, 190), "spin_range": (5000, 6000), "smash_target": 1.34},
    "6-Iron": {"loft": 30.0, "typical_distance": (150, 180), "spin_range": (5500, 6500), "smash_target": 1.32},
    "7-Iron": {"loft": 34.0, "typical_distance": (140, 170), "spin_range": (6000, 7500), "smash_target": 1.30},
    "8-Iron": {"loft": 38.0, "typical_distance": (130, 160), "spin_range": (7000, 8500), "smash_target": 1.28},
    "9-Iron": {"loft": 42.0, "typical_distance": (120, 145), "spin_range": (8000, 9500), "smash_target": 1.26},
    "PW": {"loft": 46.0, "typical_distance": (100, 130), "spin_range": (9000, 10500), "smash_target": 1.24},
    "GW": {"loft": 50.0, "typical_distance": (90, 115), "spin_range": (9500, 11000), "smash_target": 1.22},
    "SW": {"loft": 54.0, "typical_distance": (70, 100), "spin_range": (10000, 12000), "smash_target": 1.20},
    "LW": {"loft": 58.0, "typical_distance": (50, 80), "spin_range": (10500, 13000), "smash_target": 1.18},
    "Putter": {"loft": 3.0, "typical_distance": (1, 60), "spin_range": (0, 500), "smash_target": 1.0},
}

SIMULATOR_MODELS = [
    ("TM4", "Trackman 4", "Outdoor/Indoor dual radar", 25000),
    ("iO", "Trackman iO", "Indoor ceiling-mounted", 18000),
    ("iO_DUO", "Trackman iO DUO", "Indoor dual-handed narrow bay", 22000),
]

FACILITY_TYPES = ["home_residential", "commercial_indoor", "golf_range", "country_club", "resort", "retail_fitting"]

SUBSCRIPTION_TIERS = [
    ("basic", "Basic", 29.99, ["course_play", "shot_analysis"]),
    ("performance", "Performance", 59.99, ["course_play", "shot_analysis", "combine_tests", "map_my_bag"]),
    ("pro", "Pro", 99.99, ["course_play", "shot_analysis", "combine_tests", "map_my_bag", "ai_coaching", "video_analysis"]),
    ("facility", "Facility License", 299.99, ["unlimited_users", "tournament_hosting", "booking_system", "all_features"]),
]

REGIONS = {
    "North America": {"countries": ["USA", "Canada"], "weight": 0.45},
    "Europe": {"countries": ["UK", "Germany", "Sweden", "Netherlands", "France", "Spain", "Ireland", "Norway"], "weight": 0.30},
    "Asia Pacific": {"countries": ["Japan", "South Korea", "Australia", "China", "Singapore", "Hong Kong"], "weight": 0.20},
    "Middle East": {"countries": ["UAE", "Saudi Arabia"], "weight": 0.05},
}

FIRST_NAMES = ["James", "John", "Michael", "David", "Robert", "William", "Thomas", "Charles", "Daniel", "Matthew",
               "Emma", "Olivia", "Sophia", "Isabella", "Mia", "Charlotte", "Amelia", "Harper", "Evelyn", "Abigail",
               "Liam", "Noah", "Oliver", "Lucas", "Mason", "Ethan", "Alexander", "Henry", "Sebastian", "Jack",
               "Jin", "Wei", "Kenji", "Takeshi", "Min", "Sven", "Lars", "Hans", "Pierre", "Marco", "Haydon", "Carl"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White",
              "Kim", "Park", "Chen", "Wang", "Tanaka", "Yamamoto", "Mueller", "Schmidt", "Johansson", "Eriksson"]


def generate_course_holes(course: Dict) -> List[Dict]:
    """Generate 18 holes for a course with par, yardage, stroke index"""
    holes = []
    total_par = course["par"]
    total_yardage = course["yardage"]
    
    par_distribution = []
    if total_par == 72:
        par_distribution = [4, 5, 4, 3, 4, 4, 3, 4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 5]
    elif total_par == 71:
        par_distribution = [4, 4, 4, 3, 4, 4, 3, 4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 5]
    elif total_par == 70:
        par_distribution = [4, 4, 4, 3, 4, 4, 3, 4, 4, 4, 4, 3, 5, 4, 4, 3, 4, 5]
    else:
        par_distribution = [4] * 18
        
    random.shuffle(par_distribution)
    
    stroke_indices = list(range(1, 19))
    random.shuffle(stroke_indices)
    
    avg_yardage = total_yardage / 18
    yardage_ranges = {3: (140, 220), 4: (350, 470), 5: (480, 620)}
    
    for i in range(18):
        par = par_distribution[i]
        yds_range = yardage_ranges[par]
        yardage = random.randint(yds_range[0], yds_range[1])
        
        holes.append({
            "course_id": course["course_id"],
            "hole_number": i + 1,
            "par": par,
            "yardage": yardage,
            "stroke_index": stroke_indices[i],
            "has_water": random.random() > 0.7,
            "has_bunker": random.random() > 0.3,
        })
    
    return holes


def generate_player_skill_profile() -> Dict[str, float]:
    handicap = np.random.choice(
        [np.random.uniform(-2, 5), np.random.uniform(5, 15), np.random.uniform(15, 25), np.random.uniform(25, 36)],
        p=[0.05, 0.35, 0.45, 0.15]
    )
    skill_factor = max(0.3, 1.0 - (handicap / 36))
    
    return {
        "handicap": round(handicap, 1),
        "skill_factor": skill_factor,
        "club_speed_base": 70 + skill_factor * 45,
        "consistency": 0.4 + skill_factor * 0.5,
        "accuracy": 0.3 + skill_factor * 0.6,
    }


def generate_players(n: int = 500) -> pd.DataFrame:
    players = []
    region_weights = [r["weight"] for r in REGIONS.values()]
    region_names = list(REGIONS.keys())
    
    for i in range(n):
        region = np.random.choice(region_names, p=region_weights)
        country = random.choice(REGIONS[region]["countries"])
        skill = generate_player_skill_profile()
        
        created_at = datetime.now() - timedelta(days=random.randint(30, 365))
        
        player = {
            "player_id": str(uuid.uuid4()),
            "player_name": f"{random.choice(FIRST_NAMES)}{random.choice(LAST_NAMES)[0]}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "email": f"player_{i+1}@example.com",
            "country": country,
            "region": region,
            "handicap_index": skill["handicap"],
            "skill_factor": round(skill["skill_factor"], 3),
            "club_speed_base": round(skill["club_speed_base"], 1),
            "consistency_rating": round(skill["consistency"], 3),
            "accuracy_rating": round(skill["accuracy"], 3),
            "age": random.randint(18, 75),
            "gender": random.choice(["M", "F"]) if random.random() > 0.15 else "M",
            "membership_tier": random.choices(
                ["basic", "performance", "pro", "facility"],
                weights=[0.40, 0.35, 0.20, 0.05]
            )[0],
            "tee_preference": random.choice(["Gold", "Blue", "White", "Red"]),
            "created_at": created_at.isoformat(),
            "is_active": random.random() > 0.1,
            "is_guest": random.random() > 0.7,
        }
        players.append(player)
    
    return pd.DataFrame(players)


def generate_courses_and_holes() -> Tuple[pd.DataFrame, pd.DataFrame]:
    courses = []
    all_holes = []
    
    for c in VIRTUAL_COURSES:
        course = {
            "course_id": c["course_id"],
            "course_name": c["name"],
            "country": c["country"],
            "par": c["par"],
            "total_yardage": c["yardage"],
            "course_rating": c["rating"],
            "slope_rating": c["slope"],
            "num_holes": 18,
            "difficulty_tier": "championship" if c["yardage"] > 7200 else "standard",
            "green_speed_stimp": round(random.uniform(10, 14), 1),
            "is_premium": random.random() > 0.6,
        }
        courses.append(course)
        all_holes.extend(generate_course_holes(c))
    
    return pd.DataFrame(courses), pd.DataFrame(all_holes)


def generate_clubs() -> pd.DataFrame:
    clubs = []
    for club_name, specs in CLUB_SPECS.items():
        club = {
            "club_id": club_name.lower().replace("-", "_"),
            "club_name": club_name,
            "club_category": "wood" if "Wood" in club_name or "Driver" in club_name 
                           else "iron" if "Iron" in club_name 
                           else "wedge" if any(w in club_name for w in ["PW", "GW", "SW", "LW"])
                           else "hybrid" if "Hybrid" in club_name
                           else "putter",
            "standard_loft": specs["loft"],
            "typical_distance_min": specs["typical_distance"][0],
            "typical_distance_max": specs["typical_distance"][1],
            "typical_spin_min": specs["spin_range"][0],
            "typical_spin_max": specs["spin_range"][1],
            "target_smash_factor": specs["smash_target"],
        }
        clubs.append(club)
    return pd.DataFrame(clubs)


def generate_facilities(n: int = 50) -> pd.DataFrame:
    facilities = []
    region_weights = [r["weight"] for r in REGIONS.values()]
    region_names = list(REGIONS.keys())
    
    facility_names = [
        "Carl's Place", "Golf Lab", "Swing Studio", "Indoor Golf Center", "Pro Golf Academy",
        "The Golf House", "Precision Golf", "Urban Golf", "Golf Zone", "Fairway Indoor",
        "Links Indoor", "Drive Zone", "Golf Performance Center", "Virtual Golf Club", "Tour Golf"
    ]
    
    for i in range(n):
        region = np.random.choice(region_names, p=region_weights)
        country = random.choice(REGIONS[region]["countries"])
        facility_type = random.choices(
            FACILITY_TYPES,
            weights=[0.20, 0.35, 0.20, 0.10, 0.10, 0.05]
        )[0]
        
        num_bays = random.randint(1, 8) if facility_type != "home_residential" else 1
        
        facility = {
            "facility_id": str(uuid.uuid4()),
            "facility_name": f"{random.choice(facility_names)} {random.choice(['', country[:3], str(i+1)])}".strip(),
            "facility_type": facility_type,
            "country": country,
            "region": region,
            "city": f"City_{i+1}",
            "num_bays": num_bays,
            "operating_hours_start": random.choice([6, 7, 8, 9]),
            "operating_hours_end": random.choice([20, 21, 22, 23]),
            "is_commercial": facility_type != "home_residential",
            "opening_date": (datetime.now() - timedelta(days=random.randint(30, 1000))).date().isoformat(),
            "is_active": random.random() > 0.05,
        }
        facilities.append(facility)
    return pd.DataFrame(facilities)


def generate_bays(facilities_df: pd.DataFrame) -> pd.DataFrame:
    bays = []
    
    for _, facility in facilities_df.iterrows():
        num_bays = facility["num_bays"]
        for j in range(num_bays):
            model = random.choices(SIMULATOR_MODELS, weights=[0.35, 0.50, 0.15])[0]
            
            bay = {
                "bay_id": str(uuid.uuid4()),
                "facility_id": facility["facility_id"],
                "bay_name": f"{facility['facility_name']} Bay" if num_bays == 1 else f"Bay {j+1}",
                "bay_number": j + 1,
                "simulator_model": model[0],
                "simulator_name": model[1],
                "serial_number": f"TM-{random.randint(100000, 999999)}",
                "installation_date": (datetime.now() - timedelta(days=random.randint(30, 730))).date().isoformat(),
                "is_active": random.random() > 0.02,
                "hourly_rate": round(random.uniform(30, 80), 2) if facility["is_commercial"] else 0,
            }
            bays.append(bay)
    
    return pd.DataFrame(bays)


def generate_shot_data(player: Dict, club_name: str) -> Dict[str, Any]:
    specs = CLUB_SPECS[club_name]
    skill = player["skill_factor"]
    consistency = player["consistency_rating"]
    
    club_speed = player["club_speed_base"] * (specs["smash_target"] / 1.48)
    club_speed *= np.random.normal(1.0, 0.05 * (1 - consistency))
    club_speed = max(40, min(130, club_speed))
    
    quality = np.random.beta(2 + skill * 3, 2)
    smash = specs["smash_target"] * (0.85 + 0.15 * quality)
    smash *= np.random.normal(1.0, 0.02)
    smash = min(smash, 1.52)
    
    ball_speed = club_speed * smash
    
    attack_angle = np.random.normal(
        -2 if "Iron" in club_name or club_name in ["PW", "GW", "SW", "LW"] 
        else 2 if club_name == "Driver" 
        else 0,
        2 * (1 - consistency)
    )
    
    optimal_launch = specs["loft"] * 0.75
    launch_angle = optimal_launch + np.random.normal(0, 3 * (1 - consistency))
    launch_angle = max(0, min(45, launch_angle))
    
    club_path = np.random.normal(0, max(0.1, 4 * (1 - skill)))
    face_angle = np.random.normal(0, max(0.1, 3 * (1 - skill)))
    face_to_path = face_angle - club_path
    
    spin_base = np.mean(specs["spin_range"])
    spin_rate = spin_base * np.random.normal(1.0, 0.15)
    spin_rate = max(specs["spin_range"][0] * 0.7, min(specs["spin_range"][1] * 1.3, spin_rate))
    
    spin_axis = face_to_path * 8
    spin_axis = max(-30, min(30, spin_axis))
    
    base_carry = np.mean(specs["typical_distance"])
    speed_factor = ball_speed / (club_speed * specs["smash_target"])
    carry = base_carry * speed_factor * quality
    carry *= np.random.normal(1.0, 0.08 * (1 - consistency))
    carry = max(specs["typical_distance"][0] * 0.6, min(specs["typical_distance"][1] * 1.15, carry))
    
    roll = carry * np.random.uniform(0.02, 0.12) if club_name not in ["SW", "LW", "Putter"] else carry * np.random.uniform(0.01, 0.05)
    total_distance = carry + roll
    
    apex_height = carry * np.sin(np.radians(launch_angle)) * 0.4
    apex_height = max(5, min(150, apex_height))
    
    return {
        "club_speed": round(club_speed, 1),
        "ball_speed": round(ball_speed, 1),
        "smash_factor": round(smash, 3),
        "attack_angle": round(attack_angle, 1),
        "club_path": round(club_path, 1),
        "face_angle": round(face_angle, 1),
        "face_to_path": round(face_to_path, 1),
        "spin_rate": int(spin_rate),
        "spin_axis": round(spin_axis, 1),
        "launch_angle": round(launch_angle, 1),
        "apex_height": round(apex_height, 1),
        "carry_distance": round(carry, 1),
        "total_distance": round(total_distance, 1),
    }


def generate_hole_score(player: Dict, hole: Dict) -> Dict:
    """Generate a realistic hole score based on player skill and hole difficulty"""
    skill = player["skill_factor"]
    par = hole["par"]
    
    score_probs = {
        3: {"eagle": 0.01 * skill, "birdie": 0.15 * skill, "par": 0.35 + 0.2 * skill, "bogey": 0.35 - 0.1 * skill, "double": 0.14 - 0.05 * skill},
        4: {"eagle": 0.005 * skill, "birdie": 0.12 * skill, "par": 0.40 + 0.15 * skill, "bogey": 0.33 - 0.08 * skill, "double": 0.12 - 0.04 * skill},
        5: {"eagle": 0.03 * skill, "birdie": 0.18 * skill, "par": 0.38 + 0.15 * skill, "bogey": 0.30 - 0.08 * skill, "double": 0.11 - 0.04 * skill},
    }
    
    probs = score_probs.get(par, score_probs[4])
    total = sum(probs.values())
    probs = {k: v/total for k, v in probs.items()}
    
    outcome = np.random.choice(list(probs.keys()), p=list(probs.values()))
    score_map = {"eagle": -2, "birdie": -1, "par": 0, "bogey": 1, "double": 2}
    strokes = par + score_map[outcome]
    
    if random.random() > 0.95:
        strokes += random.choice([1, 2])
    
    putts = min(strokes, random.choices([1, 2, 3], weights=[0.15 + 0.1*skill, 0.70, 0.15 - 0.1*skill])[0])
    gir = strokes - putts <= par - 2
    fir = random.random() < (0.4 + 0.4 * skill) if par >= 4 else None
    
    return {
        "strokes": max(1, strokes),
        "putts": putts,
        "gir": gir,
        "fir": fir,
        "score_type": outcome,
        "vs_par": strokes - par,
    }


def generate_sessions_comprehensive(
    players_df: pd.DataFrame,
    bays_df: pd.DataFrame,
    courses_df: pd.DataFrame,
    holes_df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    target_sessions: int = 8000
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
    sessions = []
    scorecards = []
    hole_scores = []
    shots = []
    game_sessions = []
    
    session_weights = [0.25, 0.20, 0.10, 0.08, 0.18, 0.10, 0.05, 0.04]
    
    date_range = (end_date - start_date).days
    players_list = players_df.to_dict('records')
    bays_list = bays_df.to_dict('records')
    courses_list = courses_df.to_dict('records')
    club_names = [c for c in CLUB_SPECS.keys() if c != "Putter"]
    
    print(f"Generating {target_sessions} sessions...")
    
    for session_idx in range(target_sessions):
        if session_idx % 1000 == 0:
            print(f"  Progress: {session_idx}/{target_sessions} sessions")
        
        num_players = random.choices([1, 2, 3, 4], weights=[0.35, 0.35, 0.20, 0.10])[0]
        session_players = random.sample(players_list, min(num_players, len(players_list)))
        player = session_players[0]
        bay = random.choice(bays_list)
        session_type = random.choices(SESSION_TYPES, weights=session_weights)[0]
        
        session_date = start_date + timedelta(days=random.randint(0, date_range))
        hour_weights = [0.01]*6 + [0.03, 0.05, 0.08, 0.10, 0.10, 0.12, 0.10, 0.08, 0.06, 0.08, 0.06, 0.04, 0.02, 0.01, 0.01, 0.01, 0.01, 0.01]
        hour = np.random.choice(range(24), p=[w/sum(hour_weights[:24]) for w in hour_weights[:24]])
        session_start = session_date.replace(hour=hour, minute=random.randint(0, 59))
        
        duration = session_type["avg_duration"] + random.randint(-20, 40)
        duration = max(15, duration)
        
        session_id = str(uuid.uuid4())
        is_logged_in = not player["is_guest"]
        
        session = {
            "session_id": session_id,
            "facility_id": bay["facility_id"],
            "bay_id": bay["bay_id"],
            "bay_name": bay["bay_name"],
            "session_type": session_type["type_id"],
            "session_category": session_type["category"],
            "started_at": session_start.isoformat(),
            "ended_at": (session_start + timedelta(minutes=duration)).isoformat(),
            "duration_minutes": duration,
            "session_date": session_start.date().isoformat(),
            "day_of_week": session_start.strftime("%A"),
            "hour_of_day": hour,
            "num_players": num_players,
            "is_logged_in": is_logged_in,
            "is_guest": not is_logged_in,
        }
        sessions.append(session)
        
        if session_type["type_id"] == "course_play":
            course = random.choice(courses_list)
            holes_played = random.choices([18, 9], weights=[0.55, 0.45])[0]
            
            for sp in session_players:
                scorecard_id = str(uuid.uuid4())
                course_holes = holes_df[holes_df["course_id"] == course["course_id"]].to_dict('records')
                
                total_strokes = 0
                total_putts = 0
                gir_count = 0
                fir_count = 0
                fir_holes = 0
                
                for hole in course_holes[:holes_played]:
                    score_data = generate_hole_score(sp, hole)
                    total_strokes += score_data["strokes"]
                    total_putts += score_data["putts"]
                    if score_data["gir"]:
                        gir_count += 1
                    if score_data["fir"] is not None:
                        fir_holes += 1
                        if score_data["fir"]:
                            fir_count += 1
                    
                    hole_scores.append({
                        "hole_score_id": str(uuid.uuid4()),
                        "scorecard_id": scorecard_id,
                        "session_id": session_id,
                        "player_id": sp["player_id"],
                        "course_id": course["course_id"],
                        "hole_number": hole["hole_number"],
                        "par": hole["par"],
                        "yardage": hole["yardage"],
                        "stroke_index": hole["stroke_index"],
                        "strokes": score_data["strokes"],
                        "putts": score_data["putts"],
                        "gir": score_data["gir"],
                        "fir": score_data["fir"],
                        "score_type": score_data["score_type"],
                        "vs_par": score_data["vs_par"],
                        "score_date": session_start.date().isoformat(),
                    })
                
                expected_par = sum(h["par"] for h in course_holes[:holes_played])
                front_nine = sum(hs["strokes"] for hs in hole_scores[-holes_played:][:9]) if holes_played >= 9 else None
                back_nine = sum(hs["strokes"] for hs in hole_scores[-holes_played:][9:]) if holes_played == 18 else None
                
                scorecards.append({
                    "scorecard_id": scorecard_id,
                    "session_id": session_id,
                    "player_id": sp["player_id"],
                    "player_name": sp["player_name"],
                    "course_id": course["course_id"],
                    "course_name": course["course_name"],
                    "tee": sp.get("tee_preference", "Gold"),
                    "holes_played": holes_played,
                    "total_strokes": total_strokes,
                    "front_nine": front_nine,
                    "back_nine": back_nine,
                    "total_par": expected_par,
                    "score_vs_par": total_strokes - expected_par,
                    "gross_score": total_strokes,
                    "net_score": total_strokes - int(sp["handicap_index"] * holes_played / 18),
                    "handicap": sp["handicap_index"],
                    "gir_count": gir_count,
                    "gir_percentage": round(gir_count / holes_played * 100, 1),
                    "fir_percentage": round(fir_count / fir_holes * 100, 1) if fir_holes > 0 else None,
                    "putts_total": total_putts,
                    "putts_per_hole": round(total_putts / holes_played, 2),
                    "is_complete": holes_played == 18,
                    "round_date": session_start.date().isoformat(),
                    "round_datetime": session_start.isoformat(),
                })
        
        elif session_type["type_id"] == "game":
            game = random.choice(GAME_TYPES)
            num_shots = random.randint(game["min_shots"], game["max_shots"])
            score = random.randint(50, 100) * num_players
            
            game_sessions.append({
                "game_session_id": str(uuid.uuid4()),
                "session_id": session_id,
                "game_type_id": game["game_type_id"],
                "game_name": game["name"],
                "num_players": num_players,
                "num_shots": num_shots,
                "total_strokes": num_shots,
                "score": score,
                "duration_minutes": duration,
                "game_date": session_start.date().isoformat(),
                "started_at": session_start.isoformat(),
            })
        
        elif session_type["category"] == "practice":
            num_shots = random.randint(80, 150)
            for i in range(num_shots):
                club = random.choice(club_names)
                shot_data = generate_shot_data(player, club)
                shots.append({
                    "shot_id": str(uuid.uuid4()),
                    "session_id": session_id,
                    "player_id": player["player_id"],
                    "bay_id": bay["bay_id"],
                    "club_id": club.lower().replace("-", "_"),
                    "shot_number": i + 1,
                    "shot_timestamp": (session_start + timedelta(seconds=i*30)).isoformat(),
                    "shot_date": session_start.date().isoformat(),
                    **shot_data,
                })
        
        elif session_type["type_id"] == "tournament":
            course = random.choice(courses_list)
            for sp in session_players:
                scorecard_id = str(uuid.uuid4())
                course_holes = holes_df[holes_df["course_id"] == course["course_id"]].to_dict('records')
                
                total_strokes = 0
                total_putts = 0
                
                for hole in course_holes:
                    score_data = generate_hole_score(sp, hole)
                    total_strokes += score_data["strokes"]
                    total_putts += score_data["putts"]
                    
                    hole_scores.append({
                        "hole_score_id": str(uuid.uuid4()),
                        "scorecard_id": scorecard_id,
                        "session_id": session_id,
                        "player_id": sp["player_id"],
                        "course_id": course["course_id"],
                        "hole_number": hole["hole_number"],
                        "par": hole["par"],
                        "yardage": hole["yardage"],
                        "stroke_index": hole["stroke_index"],
                        "strokes": score_data["strokes"],
                        "putts": score_data["putts"],
                        "gir": score_data["gir"],
                        "fir": score_data["fir"],
                        "score_type": score_data["score_type"],
                        "vs_par": score_data["vs_par"],
                        "score_date": session_start.date().isoformat(),
                    })
                
                scorecards.append({
                    "scorecard_id": scorecard_id,
                    "session_id": session_id,
                    "player_id": sp["player_id"],
                    "player_name": sp["player_name"],
                    "course_id": course["course_id"],
                    "course_name": course["course_name"],
                    "tee": sp.get("tee_preference", "Gold"),
                    "holes_played": 18,
                    "total_strokes": total_strokes,
                    "front_nine": sum(hs["strokes"] for hs in hole_scores[-18:][:9]),
                    "back_nine": sum(hs["strokes"] for hs in hole_scores[-18:][9:]),
                    "total_par": course["par"],
                    "score_vs_par": total_strokes - course["par"],
                    "gross_score": total_strokes,
                    "net_score": total_strokes - int(sp["handicap_index"]),
                    "handicap": sp["handicap_index"],
                    "gir_count": sum(1 for hs in hole_scores[-18:] if hs["gir"]),
                    "gir_percentage": round(sum(1 for hs in hole_scores[-18:] if hs["gir"]) / 18 * 100, 1),
                    "fir_percentage": None,
                    "putts_total": total_putts,
                    "putts_per_hole": round(total_putts / 18, 2),
                    "is_complete": True,
                    "is_tournament": True,
                    "round_date": session_start.date().isoformat(),
                    "round_datetime": session_start.isoformat(),
                })
    
    print(f"Generated: {len(sessions)} sessions, {len(scorecards)} scorecards, {len(hole_scores)} hole scores, {len(shots)} shots, {len(game_sessions)} game sessions")
    
    return (
        pd.DataFrame(sessions),
        pd.DataFrame(scorecards),
        pd.DataFrame(hole_scores),
        pd.DataFrame(shots),
        pd.DataFrame(game_sessions)
    )


def generate_bay_bookings(
    bays_df: pd.DataFrame,
    sessions_df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame:
    """Generate bay occupancy/booking data for heatmap analysis"""
    bookings = []
    
    for _, bay in bays_df.iterrows():
        bay_sessions = sessions_df[sessions_df["bay_id"] == bay["bay_id"]]
        
        current = start_date
        while current <= end_date:
            for hour in range(6, 24):
                is_occupied = random.random() < (0.15 + 0.25 * (1 if current.weekday() >= 5 else 0) + 0.1 * (1 if 16 <= hour <= 20 else 0))
                
                if is_occupied:
                    bookings.append({
                        "booking_id": str(uuid.uuid4()),
                        "bay_id": bay["bay_id"],
                        "facility_id": bay["facility_id"],
                        "booking_date": current.date().isoformat(),
                        "hour_of_day": hour,
                        "day_of_week": current.strftime("%A"),
                        "is_weekend": current.weekday() >= 5,
                        "duration_hours": random.choice([1, 1.5, 2, 2.5, 3]),
                        "num_players": random.randint(1, 4),
                        "is_occupied": True,
                    })
            current += timedelta(days=1)
    
    return pd.DataFrame(bookings)


def generate_subscription_events(players_df: pd.DataFrame, start_date: datetime, end_date: datetime) -> List[Dict]:
    events = []
    players_list = players_df.to_dict('records')
    
    for player in players_list:
        created = datetime.fromisoformat(player["created_at"])
        current_tier = player["membership_tier"]
        
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_type": "subscription_started",
            "player_id": player["player_id"],
            "event_timestamp": created.isoformat(),
            "event_date": created.date().isoformat(),
            "subscription_tier": current_tier,
            "amount_usd": next(t[2] for t in SUBSCRIPTION_TIERS if t[0] == current_tier),
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay"]),
        })
        
        current = created
        while current < end_date:
            current += timedelta(days=30)
            if current > end_date:
                break
                
            if random.random() > 0.96:
                events.append({
                    "event_id": str(uuid.uuid4()),
                    "event_type": "subscription_cancelled",
                    "player_id": player["player_id"],
                    "event_timestamp": current.isoformat(),
                    "event_date": current.date().isoformat(),
                    "subscription_tier": current_tier,
                    "amount_usd": 0,
                    "cancellation_reason": random.choice(["price", "not_using", "competitor", "other"]),
                })
                break
            
            events.append({
                "event_id": str(uuid.uuid4()),
                "event_type": "subscription_renewed",
                "player_id": player["player_id"],
                "event_timestamp": current.isoformat(),
                "event_date": current.date().isoformat(),
                "subscription_tier": current_tier,
                "amount_usd": next(t[2] for t in SUBSCRIPTION_TIERS if t[0] == current_tier),
            })
    
    return events


def generate_marketing_events(players_df: pd.DataFrame, start_date: datetime, end_date: datetime) -> List[Dict]:
    events = []
    players_list = players_df.to_dict('records')
    
    campaigns = [
        ("winter_promo_2025", "Winter Training Promo", "email", 0.25, 0.08),
        ("new_course_launch", "New Course Announcement", "email", 0.35, 0.12),
        ("upgrade_offer", "Upgrade Your Experience", "email", 0.22, 0.05),
        ("feature_release", "New AI Coaching Feature", "in_app", 0.45, 0.15),
        ("tournament_invite", "Virtual Tournament", "push", 0.30, 0.10),
    ]
    
    for campaign_id, campaign_name, channel, open_rate, click_rate in campaigns:
        campaign_date = start_date + timedelta(days=random.randint(0, 60))
        recipients = random.sample(players_list, int(len(players_list) * random.uniform(0.3, 0.8)))
        
        for player in recipients:
            send_time = campaign_date + timedelta(hours=random.randint(8, 12))
            
            events.append({
                "event_id": str(uuid.uuid4()),
                "event_type": "campaign_sent",
                "player_id": player["player_id"],
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "channel": channel,
                "event_timestamp": send_time.isoformat(),
                "event_date": send_time.date().isoformat(),
            })
            
            if random.random() < open_rate:
                open_time = send_time + timedelta(hours=random.randint(1, 48))
                events.append({
                    "event_id": str(uuid.uuid4()),
                    "event_type": "campaign_opened",
                    "player_id": player["player_id"],
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "channel": channel,
                    "event_timestamp": open_time.isoformat(),
                    "event_date": open_time.date().isoformat(),
                })
                
                if random.random() < (click_rate / open_rate):
                    click_time = open_time + timedelta(minutes=random.randint(1, 30))
                    events.append({
                        "event_id": str(uuid.uuid4()),
                        "event_type": "campaign_clicked",
                        "player_id": player["player_id"],
                        "campaign_id": campaign_id,
                        "campaign_name": campaign_name,
                        "channel": channel,
                        "event_timestamp": click_time.isoformat(),
                        "event_date": click_time.date().isoformat(),
                    })
    
    return events


def save_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"  Saved {len(df):,} rows to {path.name}")


def save_json_lines(data: List[Dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')
    print(f"  Saved {len(data):,} records to {path.name}")


def main():
    print("=" * 70)
    print("Trackman Golf Simulator - Enhanced Data Generator")
    print("Supports: Scorecards, Bay Occupation, Course Play, Games, Practice")
    print("=" * 70)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    print(f"\nDate range: {start_date.date()} to {end_date.date()}")
    print(f"Output directory: {OUTPUT_DIR}")
    
    print("\n[1/9] Generating players...")
    players_df = generate_players(500)
    save_parquet(players_df, OUTPUT_DIR / "dimensions" / "dim_players.parquet")
    
    print("\n[2/9] Generating courses and holes...")
    courses_df, holes_df = generate_courses_and_holes()
    save_parquet(courses_df, OUTPUT_DIR / "dimensions" / "dim_courses.parquet")
    save_parquet(holes_df, OUTPUT_DIR / "dimensions" / "dim_course_holes.parquet")
    
    print("\n[3/9] Generating clubs...")
    clubs_df = generate_clubs()
    save_parquet(clubs_df, OUTPUT_DIR / "dimensions" / "dim_clubs.parquet")
    
    print("\n[4/9] Generating facilities and bays...")
    facilities_df = generate_facilities(50)
    bays_df = generate_bays(facilities_df)
    save_parquet(facilities_df, OUTPUT_DIR / "dimensions" / "dim_facilities.parquet")
    save_parquet(bays_df, OUTPUT_DIR / "dimensions" / "dim_bays.parquet")
    
    print("\n[5/9] Generating game types...")
    game_types_df = pd.DataFrame(GAME_TYPES)
    save_parquet(game_types_df, OUTPUT_DIR / "dimensions" / "dim_game_types.parquet")
    
    print("\n[6/9] Generating sessions, scorecards, and shots...")
    sessions_df, scorecards_df, hole_scores_df, shots_df, game_sessions_df = generate_sessions_comprehensive(
        players_df, bays_df, courses_df, holes_df, start_date, end_date, target_sessions=10000
    )
    save_parquet(sessions_df, OUTPUT_DIR / "facts" / "fact_sessions.parquet")
    save_parquet(scorecards_df, OUTPUT_DIR / "facts" / "fact_scorecards.parquet")
    save_parquet(hole_scores_df, OUTPUT_DIR / "facts" / "fact_hole_scores.parquet")
    save_parquet(shots_df, OUTPUT_DIR / "facts" / "fact_shots.parquet")
    save_parquet(game_sessions_df, OUTPUT_DIR / "facts" / "fact_game_sessions.parquet")
    
    print("\n[7/9] Generating bay bookings...")
    bay_bookings_df = generate_bay_bookings(bays_df, sessions_df, start_date, end_date)
    save_parquet(bay_bookings_df, OUTPUT_DIR / "facts" / "fact_bay_bookings.parquet")
    
    print("\n[8/9] Generating subscription events...")
    subscription_events = generate_subscription_events(players_df, start_date, end_date)
    save_json_lines(subscription_events, OUTPUT_DIR / "events" / "subscription_events.json")
    
    print("\n[9/9] Generating marketing events...")
    marketing_events = generate_marketing_events(players_df, start_date, end_date)
    save_json_lines(marketing_events, OUTPUT_DIR / "events" / "marketing_events.json")
    
    tiers_df = pd.DataFrame([
        {"tier_id": t[0], "tier_name": t[1], "monthly_price": t[2], "features": json.dumps(t[3])}
        for t in SUBSCRIPTION_TIERS
    ])
    save_parquet(tiers_df, OUTPUT_DIR / "dimensions" / "dim_subscription_tiers.parquet")
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"{'Dimension Tables:':<30}")
    print(f"  {'Players:':<25} {len(players_df):>10,}")
    print(f"  {'Courses:':<25} {len(courses_df):>10,}")
    print(f"  {'Course Holes:':<25} {len(holes_df):>10,}")
    print(f"  {'Clubs:':<25} {len(clubs_df):>10,}")
    print(f"  {'Facilities:':<25} {len(facilities_df):>10,}")
    print(f"  {'Bays:':<25} {len(bays_df):>10,}")
    print(f"  {'Game Types:':<25} {len(game_types_df):>10,}")
    print(f"\n{'Fact Tables:':<30}")
    print(f"  {'Sessions:':<25} {len(sessions_df):>10,}")
    print(f"  {'Scorecards:':<25} {len(scorecards_df):>10,}")
    print(f"  {'Hole Scores:':<25} {len(hole_scores_df):>10,}")
    print(f"  {'Shots:':<25} {len(shots_df):>10,}")
    print(f"  {'Game Sessions:':<25} {len(game_sessions_df):>10,}")
    print(f"  {'Bay Bookings:':<25} {len(bay_bookings_df):>10,}")
    print(f"\n{'Event Streams:':<30}")
    print(f"  {'Subscriptions:':<25} {len(subscription_events):>10,}")
    print(f"  {'Marketing:':<25} {len(marketing_events):>10,}")
    
    total_size = sum(f.stat().st_size for f in OUTPUT_DIR.rglob("*") if f.is_file()) / (1024 * 1024)
    print(f"\n{'Total Data Size:':<25} {total_size:>10.2f} MB")
    print("=" * 70)
    print("Data generation complete! Ready for S3 upload.")
    print("=" * 70)


if __name__ == "__main__":
    main()

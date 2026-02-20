#!/usr/bin/env python3
"""
Deploy dbt models to Snowflake using Snow CLI connection
"""
import subprocess
import sys
import os

def main():
    # Change to dbt project directory
    dbt_dir = "/Users/ulasbulut/Desktop/CoCo/Trackman/trackman_dbt"
    os.chdir(dbt_dir)
    
    # Get connection details from Snow CLI
    result = subprocess.run(
        ["snow", "connection", "list", "--format", "json"],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error getting connections: {result.stderr}")
        sys.exit(1)
    
    # For now, let's just compile the models to show what would be deployed
    print("Compiling dbt models...")
    compile_cmd = [
        "dbt", "compile",
        "--profiles-dir", dbt_dir,
        "--profile", "trackman_dbt",
        "--target", "dev"
    ]
    
    result = subprocess.run(compile_cmd, env={
        **os.environ,
        "DBT_SNOWFLAKE_QUERY_TAG": "dbt_trackman_deployment"
    })
    
    if result.returncode != 0:
        print("dbt compile failed")
        sys.exit(1)
    
    print("\n" + "="*50)
    print("dbt models compiled successfully!")
    print("="*50)

if __name__ == "__main__":
    main()

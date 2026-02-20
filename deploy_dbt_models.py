#!/usr/bin/env python3
"""
Deploy dbt models to Snowflake manually
This script creates views and tables in the correct order: bronze -> silver -> gold
"""
import os
from pathlib import Path
import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "DEMO_USWEST"
)

def execute_sql(sql, model_name):
    """Execute SQL and handle errors"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        print(f"✓ Created {model_name}")
        return True
    except Exception as e:
        print(f"✗ Failed to create {model_name}: {str(e)}")
        return False
    finally:
        cursor.close()

def deploy_bronze_models():
    """Deploy bronze layer views"""
    print("\n" + "="*50)
    print("BRONZE LAYER - Creating Views")
    print("="*50)
    
    bronze_dir = Path("/Users/ulasbulut/Desktop/CoCo/Trackman/trackman_dbt/models/bronze")
    for sql_file in sorted(bronze_dir.glob("*.sql")):
        model_name = sql_file.stem
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        create_view_sql = f"""
        CREATE OR REPLACE VIEW TRACKMAN_DW.BRONZE.{model_name.upper()} AS
        {sql_content}
        """
        execute_sql(create_view_sql, f"BRONZE.{model_name}")

def deploy_silver_models():
    """Deploy silver layer tables"""
    print("\n" + "="*50)
    print("SILVER LAYER - Creating Tables")
    print("="*50)
    
    silver_dir = Path("/Users/ulasbulut/Desktop/CoCo/Trackman/trackman_dbt/models/silver")
    for sql_file in sorted(silver_dir.glob("*.sql")):
        model_name = sql_file.stem
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Remove dbt config blocks
        import re
        sql_content = re.sub(r'\{\{[^}]*config\([^}]*\)[^}]*\}\}', '', sql_content, flags=re.DOTALL)
        
        # Remove incremental blocks (we're doing full refresh)
        sql_content = re.sub(r'\{\%\s*if\s+is_incremental\(\).*?\{\%\s*endif\s*\%\}', '', sql_content, flags=re.DOTALL)
        
        # Replace dbt ref() with actual table references
        sql_content = sql_content.replace("{{ ref('", "TRACKMAN_DW.BRONZE.")
        sql_content = sql_content.replace("') }}", "")
        
        create_table_sql = f"""
        CREATE OR REPLACE TABLE TRACKMAN_DW.SILVER.{model_name.upper()} AS
        {sql_content}
        """
        execute_sql(create_table_sql, f"SILVER.{model_name}")

def deploy_gold_models():
    """Deploy gold layer tables"""
    print("\n" + "="*50)
    print("GOLD LAYER - Creating Tables")
    print("="*50)
    
    gold_dir = Path("/Users/ulasbulut/Desktop/CoCo/Trackman/trackman_dbt/models/gold")
    for sql_file in sorted(gold_dir.glob("*.sql")):
        model_name = sql_file.stem
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Replace dbt ref() with actual table references
        sql_content = sql_content.replace("{{ ref('slv_", "TRACKMAN_DW.SILVER.SLV_")
        sql_content = sql_content.replace("{{ ref('brz_", "TRACKMAN_DW.BRONZE.BRZ_")
        sql_content = sql_content.replace("') }}", "")
        
        create_table_sql = f"""
        CREATE OR REPLACE TABLE TRACKMAN_DW.GOLD.{model_name.upper()} AS
        {sql_content}
        """
        execute_sql(create_table_sql, f"GOLD.{model_name}")

if __name__ == "__main__":
    print("Starting dbt deployment to Snowflake...")
    print(f"Using connection: {os.getenv('SNOWFLAKE_CONNECTION_NAME') or 'DEMO_USWEST'}")
    
    deploy_bronze_models()
    deploy_silver_models()
    deploy_gold_models()
    
    conn.close()
    print("\n" + "="*50)
    print("DEPLOYMENT COMPLETE!")
    print("="*50)

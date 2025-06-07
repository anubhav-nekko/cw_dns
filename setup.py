import os
import json
import sqlite3
import shutil
from pdf_processor_fixed import create_tables, add_sample_data

def setup_environment():
    """Set up the environment for the DNS application"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create necessary directories
    os.makedirs(os.path.join(current_dir, 'schemes'), exist_ok=True)
    os.makedirs(os.path.join(current_dir, 'raw_texts'), exist_ok=True)
    os.makedirs(os.path.join(current_dir, 'uploads'), exist_ok=True)
    
    # Create secrets.json if it doesn't exist
    secrets_path = os.path.join(current_dir, 'secrets.json')
    if not os.path.exists(secrets_path):
        default_secrets = {
            "aws_access_key_id": "REPLACE",
            "aws_secret_access_key": "REPLACE",
            "INFERENCE_PROFILE_CLAUDE": "arn:aws:bedrock:ap-south-1:273354629305:inference-profile/apac.anthropic.claude-3-5-sonnet-20241022-v2:0",
            "REGION": "ap-south-1",
            "TAVILY_API": "tvly-dev-REPLACE",
            "FAISS_INDEX_PATH": "faiss_index.bin",
            "METADATA_STORE_PATH": "metadata_store.pkl",
            "s3_bucket_name": "cw-dns-v1",
            "container_name": "cw-dns-v1"
        }
        
        with open(secrets_path, 'w') as f:
            json.dump(default_secrets, f, indent=4)
        print("Created secrets.json file")
    
    # Set up database
    db_path = os.path.join(current_dir, 'dns_database.db')
    
    # Check if we need to migrate the database
    if os.path.exists(db_path):
        # Create backup of existing database
        backup_path = os.path.join(current_dir, 'dns_database.db.bak')
        shutil.copy2(db_path, backup_path)
        print(f"Created database backup at {backup_path}")
        
        # Check if migration is needed
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check if free_item_description column exists in scheme_products table
        cursor.execute("PRAGMA table_info(scheme_products)")
        columns = cursor.fetchall()
        column_names = [col['name'] for col in columns]
        
        if 'free_item_description' not in column_names:
            print("Migrating database: Adding free_item_description column to scheme_products table")
            try:
                cursor.execute("ALTER TABLE scheme_products ADD COLUMN free_item_description TEXT")
                conn.commit()
                print("Database migration successful")
            except sqlite3.Error as e:
                print(f"Database migration error: {e}")
                # If migration fails, use the new schema
                conn.close()
                os.remove(db_path)
                print("Recreating database with updated schema")
                create_tables()
                add_sample_data()
                return
        
        conn.close()
    else:
        # Create new database with tables
        create_tables()
        add_sample_data()

if __name__ == "__main__":
    setup_environment()

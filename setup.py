import os
import json
import sqlite3
import shutil
import sys

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
            "TAVILY_API": "REPLACE",
            "FAISS_INDEX_PATH": "faiss_index.bin",
            "METADATA_STORE_PATH": "metadata_store.pkl",
            "s3_bucket_name": "cw-dns-v1",
            "container_name": "cw-dns-v1"
        }
        
        with open(secrets_path, 'w') as f:
            json.dump(default_secrets, f, indent=4)
        
        print("Created secrets.json file")
    
    # Create database
    db_path = os.path.join(current_dir, 'dns_database.db')
    
    # Import the create_tables function from pdf_processor_fixed
    # and process PDFs if needed
    try:
        # Add the current directory to sys.path if needed
        if current_dir not in sys.path:
            sys.path.append(current_dir)
            
        # Import functions from pdf_processor_fixed
        from pdf_processor_fixed import create_tables, add_sample_data, process_multiple_pdfs
        
        # Create tables
        create_tables()
        
        # Add sample data
        add_sample_data()
        
        # Process PDFs in the schemes directory
        schemes_dir = os.path.join(current_dir, 'schemes')
        if os.path.exists(schemes_dir) and os.listdir(schemes_dir):
            process_multiple_pdfs('schemes')
        
    except Exception as e:
        print(f"Error setting up database: {e}")
        print("Please run pdf_processor_fixed.py separately after setup")

if __name__ == "__main__":
    setup_environment()

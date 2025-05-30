import os
import sqlite3
import json
import fitz  # PyMuPDF
import tempfile
import random
import datetime
import boto3
from PIL import Image
import io
import re
import uuid
import streamlit as st

# Database connection
def connect_db():
    """Connect to the SQLite database"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'dns_database.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# Create database tables
def create_tables():
    """Create all necessary database tables"""
    conn = connect_db()
    cursor = conn.cursor()
    
    # Create schemes table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS schemes (
        scheme_id INTEGER PRIMARY KEY AUTOINCREMENT,
        scheme_name TEXT NOT NULL,
        scheme_type TEXT,
        scheme_period_start TEXT DEFAULT '2023-01-01',
        scheme_period_end TEXT DEFAULT '2023-12-31',
        applicable_region TEXT,
        dealer_type_eligibility TEXT,
        scheme_document_name TEXT,
        raw_extracted_text_path TEXT,
        deal_status TEXT DEFAULT 'Active',
        approval_status TEXT DEFAULT 'Pending',
        approved_by TEXT,
        approval_timestamp TIMESTAMP,
        notes TEXT,
        upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create products table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_name TEXT NOT NULL,
        product_code TEXT,
        product_category TEXT,
        product_subcategory TEXT,
        ram TEXT,
        storage TEXT,
        connectivity TEXT,
        color TEXT,
        display_size TEXT,
        processor TEXT,
        dealer_price_dp REAL,
        mrp REAL,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create scheme_products table (junction table)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scheme_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scheme_id INTEGER,
        product_id INTEGER,
        support_type TEXT,
        payout_type TEXT,
        payout_amount REAL,
        payout_unit TEXT,
        dealer_contribution REAL DEFAULT 0,
        total_payout REAL,
        is_dealer_incentive INTEGER DEFAULT 1,
        is_bundle_offer INTEGER DEFAULT 0,
        bundle_price REAL,
        is_upgrade_offer INTEGER DEFAULT 0,
        is_slab_based INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (scheme_id) REFERENCES schemes (scheme_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    )
    ''')
    
    # Create payout_slabs table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payout_slabs (
        slab_id INTEGER PRIMARY KEY AUTOINCREMENT,
        scheme_product_id INTEGER,
        min_quantity INTEGER,
        max_quantity INTEGER,
        payout_amount REAL,
        dealer_contribution REAL DEFAULT 0,
        total_payout REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (scheme_product_id) REFERENCES scheme_products (id)
    )
    ''')
    
    # Create scheme_rules table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scheme_rules (
        rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
        scheme_id INTEGER,
        rule_type TEXT,
        rule_description TEXT,
        rule_value TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (scheme_id) REFERENCES schemes (scheme_id)
    )
    ''')
    
    # Create scheme_parameters table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scheme_parameters (
        parameter_id INTEGER PRIMARY KEY AUTOINCREMENT,
        scheme_id INTEGER,
        parameter_name TEXT,
        parameter_description TEXT,
        parameter_criteria TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (scheme_id) REFERENCES schemes (scheme_id)
    )
    ''')
    
    # Create dealers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dealers (
        dealer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        dealer_name TEXT NOT NULL,
        dealer_code TEXT,
        dealer_type TEXT,
        region TEXT,
        state TEXT,
        city TEXT,
        address TEXT,
        contact_person TEXT,
        contact_email TEXT,
        contact_phone TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create sales_transactions table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales_transactions (
        sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
        dealer_id INTEGER,
        scheme_id INTEGER,
        product_id INTEGER,
        quantity_sold INTEGER DEFAULT 1,
        dealer_price_dp REAL,
        earned_dealer_incentive_amount REAL,
        imei_serial TEXT,
        verification_status TEXT DEFAULT 'Pending',
        verified_by TEXT,
        verification_timestamp TIMESTAMP,
        sale_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (dealer_id) REFERENCES dealers (dealer_id),
        FOREIGN KEY (scheme_id) REFERENCES schemes (scheme_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    )
    ''')
    
    # Create bundle_offers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bundle_offers (
        bundle_id INTEGER PRIMARY KEY AUTOINCREMENT,
        scheme_id INTEGER,
        primary_product_id INTEGER,
        bundle_product_id INTEGER,
        bundle_price REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (scheme_id) REFERENCES schemes (scheme_id),
        FOREIGN KEY (primary_product_id) REFERENCES products (product_id),
        FOREIGN KEY (bundle_product_id) REFERENCES products (product_id)
    )
    ''')
    
    # Create scheme_approvals table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scheme_approvals (
        approval_id INTEGER PRIMARY KEY AUTOINCREMENT,
        scheme_id INTEGER,
        requested_by TEXT,
        approval_status TEXT,
        approved_by TEXT,
        approved_at TIMESTAMP,
        approval_notes TEXT,
        FOREIGN KEY (scheme_id) REFERENCES schemes (scheme_id)
    )
    ''')
    
    conn.commit()
    conn.close()
    
    print("All tables created successfully.")

# Initialize AWS clients
def initialize_aws_clients(secrets):
    """Initialize AWS clients for Bedrock and Textract"""
    try:
        # Initialize Bedrock client
        bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name=secrets.get('REGION', 'ap-south-1'),
            aws_access_key_id=secrets.get('aws_access_key_id'),
            aws_secret_access_key=secrets.get('aws_secret_access_key')
        )
        
        # Initialize Textract client
        textract_client = boto3.client(
            'textract',
            region_name=secrets.get('REGION', 'ap-south-1'),
            aws_access_key_id=secrets.get('aws_access_key_id'),
            aws_secret_access_key=secrets.get('aws_secret_access_key')
        )
        
        return bedrock_client, textract_client
    except Exception as e:
        print(f"Error initializing AWS clients: {e}")
        return None, None

# Extract text from PDF
def extract_text_from_pdf(file_path, textract_client=None):
    """Extract text from PDF using PyMuPDF and optionally AWS Textract"""
    try:
        doc = fitz.open(file_path)
        pages_text = []
        
        # Create a progress placeholder if in Streamlit context
        processing_message_placeholder = st.empty() if 'st' in globals() else None
        progress_bar = st.progress(0) if 'st' in globals() else None
        
        for page_num in range(len(doc)):
            if processing_message_placeholder:
                processing_message_placeholder.write(f"Processing page {page_num + 1}/{len(doc)}...")
            
            temp_image_path = None
            
            try:
                # First try direct text extraction with PyMuPDF
                page = doc.load_page(page_num)
                text = page.get_text()
                
                # If text is too short, try OCR with Textract
                if len(text.strip()) < 100 and textract_client:
                    # Render page as an image
                    pix = page.get_pixmap()
                    temp_image_path = os.path.join(tempfile.gettempdir(), f"page_{page_num}.png")
                    pix.save(temp_image_path)
                    
                    with open(temp_image_path, "rb") as image_file:
                        image_bytes = image_file.read()
                    
                    # Use Textract for OCR
                    try:
                        response = textract_client.detect_document_text(
                            Document={'Bytes': image_bytes}
                        )
                        
                        # Extract text from OCR results
                        text_lines = []
                        for block in response.get('Blocks', []):
                            if block['BlockType'] == 'LINE' and 'Text' in block:
                                text_lines.append(block['Text'])
                        
                        text = "\n".join(text_lines)
                    except Exception as e:
                        print(f"Textract error on page {page_num + 1}: {str(e)}")
                        # Fall back to PyMuPDF text
                
                pages_text.append((page_num + 1, text, text))
            
            except Exception as e:
                if 'st' in globals():
                    st.error(f"Error processing page {page_num + 1}: {str(e)}")
                else:
                    print(f"Error processing page {page_num + 1}: {str(e)}")
                continue
            
            finally:
                if temp_image_path and os.path.exists(temp_image_path):
                    os.remove(temp_image_path)
            
            if progress_bar:
                progress_bar.progress((page_num + 1) / len(doc))
        
        return pages_text
    
    except Exception as e:
        if 'st' in globals():
            st.error(f"Error extracting text from PDF: {str(e)}")
        else:
            print(f"Error extracting text from PDF: {str(e)}")
        return []

# Normalize field types for database insertion
def normalize_field(field, field_type=str, default=None):
    """Normalize field to the correct type for database insertion"""
    if field is None:
        return default
    
    try:
        # Handle lists by joining with comma
        if isinstance(field, list):
            if field_type == str:
                return ', '.join(str(item) for item in field)
            elif field_type == float or field_type == int:
                # For numeric types, take the first item if available
                return field_type(field[0]) if field else default
        
        # Handle dictionaries by converting to JSON string
        if isinstance(field, dict):
            if field_type == str:
                return json.dumps(field)
            else:
                return default
        
        # Convert to the specified type
        return field_type(field)
    except (ValueError, TypeError):
        return default

# Extract structured data from text
def extract_structured_data_from_text(text, document_name, bedrock_client=None, inference_profile_arn=None):
    """Extract structured data from text using Claude API or fallback to rule-based extraction"""
    try:
        # If Bedrock client is available, use Claude API
        if bedrock_client and inference_profile_arn:
            prompt = f"""
            You are a specialized AI for extracting structured data from mobile phone scheme documents.
            
            Document: {document_name}
            
            Extract the following information in JSON format:
            1. scheme_name: The name of the scheme
            2. scheme_type: Type of scheme (e.g., Special Support, RCM, Upgrade Program)
            3. scheme_period_start: Start date in YYYY-MM-DD format
            4. scheme_period_end: End date in YYYY-MM-DD format
            5. applicable_region: Region where the scheme is applicable
            6. dealer_type_eligibility: Types of dealers eligible for this scheme
            7. products: Array of products with these fields:
               - product_name: Full name of the product
               - product_code: Product code if available
               - product_category: Category (e.g., Mobile, Tablet)
               - product_subcategory: Subcategory (e.g., S Series, A Series)
               - ram: RAM specification if available
               - storage: Storage specification if available
               - connectivity: Connectivity options if available
               - support_type: Type of support (e.g., Cashback, Exchange)
               - payout_type: Type of payout (Fixed, Percentage)
               - payout_amount: Amount of payout
               - payout_unit: Unit of payout (e.g., INR, %)
               - dealer_contribution: Dealer's contribution if any
               - total_payout: Total payout amount
               - is_bundle_offer: Boolean indicating if it's a bundle offer
               - bundle_price: Price of the bundle if applicable
               - is_upgrade_offer: Boolean indicating if it's an upgrade offer
            8. scheme_rules: Array of rules with these fields:
               - rule_type: Type of rule
               - rule_description: Description of the rule
               - rule_value: Value or threshold for the rule
            
            Here's the document text:
            {text}
            
            Return only the JSON object without any additional text.
            """
            
            payload = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            try:
                response = bedrock_client.invoke_model(
                    modelId=inference_profile_arn,
                    contentType='application/json',
                    accept='application/json',
                    body=json.dumps(payload)
                )
                response_body = json.loads(response['body'].read())
                result_text = response_body['content'][0]['text'].strip()
                
                # Extract JSON from response
                json_match = re.search(r'```json\s*(.*?)\s*```', result_text, re.DOTALL)
                if json_match:
                    result_text = json_match.group(1)
                
                # Parse JSON
                structured_data = json.loads(result_text)
                return structured_data
            
            except Exception as e:
                print(f"Error calling Claude API: {e}")
                # Fall back to rule-based extraction
        
        # Rule-based extraction as fallback
        return rule_based_extraction(text, document_name)
    
    except Exception as e:
        print(f"Error extracting structured data: {e}")
        return None

# Rule-based extraction fallback
def rule_based_extraction(text, document_name):
    """Extract structured data using rule-based approach"""
    # Basic extraction of scheme name from document name
    scheme_name = document_name.split('_')[0].strip()
    if '.' in scheme_name:
        scheme_name = ' '.join(scheme_name.split('.')[1:]).strip()
    
    # Extract scheme type based on keywords
    scheme_type = "Special Support"  # Default
    if "RCM" in document_name or "RCM" in text:
        scheme_type = "RCM"
    elif "Upgrade Program" in document_name or "Upgrade" in text:
        scheme_type = "Upgrade Program"
    elif "Bundle" in document_name or "Bundle" in text:
        scheme_type = "Bundle Offer"
    
    # Extract dates (simple pattern matching)
    date_pattern = r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
    dates = re.findall(date_pattern, text)
    
    scheme_period_start = "2023-01-01"  # Default
    scheme_period_end = "2023-12-31"    # Default
    
    if len(dates) >= 2:
        # Convert to YYYY-MM-DD format (assuming DD/MM/YYYY or MM/DD/YYYY input)
        try:
            start_date = dates[0]
            end_date = dates[1]
            
            # Handle different date formats
            if '/' in start_date:
                parts = start_date.split('/')
                if len(parts[2]) == 2:
                    parts[2] = '20' + parts[2]
                scheme_period_start = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            elif '-' in start_date:
                parts = start_date.split('-')
                if len(parts[2]) == 2:
                    parts[2] = '20' + parts[2]
                scheme_period_start = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            
            if '/' in end_date:
                parts = end_date.split('/')
                if len(parts[2]) == 2:
                    parts[2] = '20' + parts[2]
                scheme_period_end = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            elif '-' in end_date:
                parts = end_date.split('-')
                if len(parts[2]) == 2:
                    parts[2] = '20' + parts[2]
                scheme_period_end = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        except:
            # Keep defaults if parsing fails
            pass
    
    # Extract region
    region = "All India"  # Default
    if "North" in text and "East" in text:
        region = "North and East"
    elif "South" in text and "West" in text:
        region = "South and West"
    elif "North" in text:
        region = "North"
    elif "South" in text:
        region = "South"
    elif "East" in text:
        region = "East"
    elif "West" in text:
        region = "West"
    
    # Extract dealer eligibility
    dealer_type = "All Dealers"  # Default
    if "MBO" in text:
        dealer_type = "MBO"
    elif "GT" in text:
        dealer_type = "GT"
    elif "SEZ" in text and "Blue Wave" in text:
        dealer_type = "SEZ and Blue Wave"
    
    # Extract products (simplified)
    products = []
    
    # Look for product models
    model_patterns = [
        r'([A-Z]\d+[A-Z]?)',  # e.g., S21, A52s
        r'(Galaxy [A-Za-z0-9]+)',  # e.g., Galaxy S21, Galaxy Tab
        r'(Tab [A-Za-z0-9]+)'  # e.g., Tab S7
    ]
    
    found_models = set()
    for pattern in model_patterns:
        matches = re.findall(pattern, text)
        found_models.update(matches)
    
    # Extract amounts (potential payouts)
    amount_pattern = r'(?:Rs\.?|INR)\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)'
    amounts = re.findall(amount_pattern, text)
    
    # Create product entries
    for i, model in enumerate(found_models):
        product = {
            "product_name": model,
            "product_code": f"CODE-{model.replace(' ', '')}-{random.randint(1000, 9999)}",
            "product_category": "Mobile" if "Tab" not in model else "Tablet",
            "product_subcategory": model[0] + " Series" if model[0].isalpha() else "Other",
            "ram": f"{random.choice([4, 6, 8, 12])}GB",
            "storage": f"{random.choice([64, 128, 256, 512])}GB",
            "connectivity": random.choice(["4G", "5G", "4G/5G"]),
            "support_type": scheme_type,
            "payout_type": "Fixed",
            "payout_amount": float(amounts[i].replace(',', '')) if i < len(amounts) else random.randint(500, 5000),
            "payout_unit": "INR",
            "dealer_contribution": 0,
            "total_payout": float(amounts[i].replace(',', '')) if i < len(amounts) else random.randint(500, 5000),
            "is_bundle_offer": scheme_type == "Bundle Offer",
            "bundle_price": None,
            "is_upgrade_offer": scheme_type == "Upgrade Program"
        }
        products.append(product)
    
    # If no products found, add a default one
    if not products:
        default_model = "Galaxy S21 FE" if "S21 FE" in text else "Galaxy S23"
        products.append({
            "product_name": default_model,
            "product_code": f"CODE-{default_model.replace(' ', '')}-{random.randint(1000, 9999)}",
            "product_category": "Mobile",
            "product_subcategory": "S Series",
            "ram": "8GB",
            "storage": "128GB",
            "connectivity": "5G",
            "support_type": scheme_type,
            "payout_type": "Fixed",
            "payout_amount": 2000.0,
            "payout_unit": "INR",
            "dealer_contribution": 0,
            "total_payout": 2000.0,
            "is_bundle_offer": scheme_type == "Bundle Offer",
            "bundle_price": None,
            "is_upgrade_offer": scheme_type == "Upgrade Program"
        })
    
    # Extract rules
    scheme_rules = [
        {
            "rule_type": "Eligibility",
            "rule_description": f"Applicable for {dealer_type}",
            "rule_value": dealer_type
        },
        {
            "rule_type": "Period",
            "rule_description": f"Valid from {scheme_period_start} to {scheme_period_end}",
            "rule_value": f"{scheme_period_start} to {scheme_period_end}"
        }
    ]
    
    # Construct the structured data
    structured_data = {
        "scheme_name": scheme_name,
        "scheme_type": scheme_type,
        "scheme_period_start": scheme_period_start,
        "scheme_period_end": scheme_period_end,
        "applicable_region": region,
        "dealer_type_eligibility": dealer_type,
        "products": products,
        "scheme_rules": scheme_rules
    }
    
    return structured_data

# Add sample data
def add_sample_data():
    """Add sample data to the database"""
    conn = connect_db()
    cursor = conn.cursor()
    
    # Check if we already have data
    cursor.execute("SELECT COUNT(*) FROM dealers")
    dealer_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM products")
    product_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM schemes")
    scheme_count = cursor.fetchone()[0]
    
    # Add sample dealers if none exist
    if dealer_count == 0:
        dealers = [
            ('Reliance Digital', 'RD001', 'National Chain', 'North', 'Delhi', 'New Delhi'),
            ('Croma', 'CR001', 'National Chain', 'West', 'Maharashtra', 'Mumbai'),
            ('Vijay Sales', 'VS001', 'Regional Chain', 'West', 'Maharashtra', 'Mumbai'),
            ('Sangeetha Mobiles', 'SM001', 'Regional Chain', 'South', 'Karnataka', 'Bangalore'),
            ('The Mobile Store', 'TMS001', 'MBO', 'South', 'Tamil Nadu', 'Chennai'),
            ('Poorvika Mobiles', 'PM001', 'Regional Chain', 'South', 'Tamil Nadu', 'Chennai'),
            ('Bajaj Electronics', 'BE001', 'Regional Chain', 'South', 'Telangana', 'Hyderabad'),
            ('Great Eastern', 'GE001', 'Regional Chain', 'East', 'West Bengal', 'Kolkata'),
            ('Tata Croma', 'TC001', 'National Chain', 'North', 'Uttar Pradesh', 'Lucknow'),
            ('Mobile World', 'MW001', 'MBO', 'South', 'Karnataka', 'Bangalore')
        ]
        
        for dealer in dealers:
            cursor.execute('''
            INSERT INTO dealers (
                dealer_name, dealer_code, dealer_type, region, state, city
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', dealer)
        
        print("Sample dealers added successfully.")
    
    # Add sample products and schemes if none exist
    if product_count == 0 and scheme_count == 0:
        # Add sample products
        products = [
            ('Samsung Galaxy S23 Ultra', 'SM-S918B', 'Mobile', 'S Series', '12GB', '512GB', '5G', 'Phantom Black', '6.8"', 'Snapdragon 8 Gen 2', 124999.0, 149999.0),
            ('Samsung Galaxy S23+', 'SM-S916B', 'Mobile', 'S Series', '8GB', '256GB', '5G', 'Cream', '6.6"', 'Snapdragon 8 Gen 2', 94999.0, 109999.0),
            ('Samsung Galaxy S23', 'SM-S911B', 'Mobile', 'S Series', '8GB', '128GB', '5G', 'Green', '6.1"', 'Snapdragon 8 Gen 2', 74999.0, 89999.0),
            ('Samsung Galaxy S21 FE', 'SM-G990B', 'Mobile', 'S Series', '8GB', '128GB', '5G', 'Olive', '6.4"', 'Exynos 2100', 49999.0, 54999.0),
            ('Samsung Galaxy A54', 'SM-A546B', 'Mobile', 'A Series', '8GB', '128GB', '5G', 'Awesome Violet', '6.4"', 'Exynos 1380', 38999.0, 44999.0),
            ('Samsung Galaxy A34', 'SM-A346B', 'Mobile', 'A Series', '8GB', '128GB', '5G', 'Awesome Silver', '6.6"', 'Dimensity 1080', 30999.0, 36999.0),
            ('Samsung Galaxy A14', 'SM-A145F', 'Mobile', 'A Series', '4GB', '64GB', '4G', 'Black', '6.6"', 'Helio G80', 13999.0, 16999.0),
            ('Samsung Galaxy M34', 'SM-M346B', 'Mobile', 'M Series', '6GB', '128GB', '5G', 'Midnight Blue', '6.5"', 'Exynos 1280', 18999.0, 24999.0),
            ('Samsung Galaxy M14', 'SM-M146B', 'Mobile', 'M Series', '4GB', '64GB', '4G', 'Berry Blue', '6.6"', 'Exynos 850', 13499.0, 15999.0),
            ('Samsung Galaxy F54', 'SM-E546B', 'Mobile', 'F Series', '8GB', '256GB', '5G', 'Stardust Silver', '6.7"', 'Dimensity 1080', 29999.0, 35999.0),
            ('Samsung Galaxy F14', 'SM-E146B', 'Mobile', 'F Series', '4GB', '128GB', '5G', 'GOAT Green', '6.6"', 'Exynos 1330', 14999.0, 17999.0),
            ('Samsung Galaxy Tab S9 Ultra', 'SM-X916B', 'Tablet', 'Tab S Series', '12GB', '256GB', '5G', 'Graphite', '14.6"', 'Snapdragon 8 Gen 2', 109999.0, 129999.0),
            ('Samsung Galaxy Tab S9+', 'SM-X816B', 'Tablet', 'Tab S Series', '12GB', '256GB', '5G', 'Beige', '12.4"', 'Snapdragon 8 Gen 2', 89999.0, 109999.0),
            ('Samsung Galaxy Tab S9', 'SM-X716B', 'Tablet', 'Tab S Series', '8GB', '128GB', '5G', 'Graphite', '11"', 'Snapdragon 8 Gen 2', 74999.0, 89999.0),
            ('Samsung Galaxy Tab A9+', 'SM-X216B', 'Tablet', 'Tab A Series', '8GB', '128GB', '4G', 'Silver', '11"', 'Snapdragon 695', 24999.0, 29999.0),
            ('Samsung Galaxy Tab A9', 'SM-X116B', 'Tablet', 'Tab A Series', '4GB', '64GB', '4G', 'Gray', '8.7"', 'Helio G99', 15999.0, 19999.0),
            ('Samsung Galaxy Book3 Pro 360', 'NP960QFG', 'Laptop', 'Galaxy Book Series', '16GB', '512GB', 'Wi-Fi', 'Graphite', '16"', 'Intel Core i7-13700H', 159999.0, 179999.0),
            ('Samsung Galaxy Book3 Pro', 'NP940XFG', 'Laptop', 'Galaxy Book Series', '16GB', '512GB', 'Wi-Fi', 'Beige', '14"', 'Intel Core i7-13700H', 139999.0, 159999.0),
            ('Samsung Galaxy Book3', 'NP750XFG', 'Laptop', 'Galaxy Book Series', '8GB', '256GB', 'Wi-Fi', 'Silver', '15.6"', 'Intel Core i5-1335U', 74999.0, 89999.0),
            ('Samsung Galaxy Watch6 Classic', 'SM-R960', 'Wearable', 'Watch Series', '2GB', '16GB', 'Bluetooth/LTE', 'Black', '1.5"', 'Exynos W930', 36999.0, 44999.0),
            ('Samsung Galaxy Watch6', 'SM-R930', 'Wearable', 'Watch Series', '2GB', '16GB', 'Bluetooth/LTE', 'Gold', '1.3"', 'Exynos W930', 29999.0, 36999.0),
            ('Samsung Galaxy Buds3 Pro', 'SM-R630', 'Audio', 'Buds Series', None, None, 'Bluetooth', 'White', None, None, 16999.0, 19999.0),
            ('Samsung Galaxy Buds3', 'SM-R530', 'Audio', 'Buds Series', None, None, 'Bluetooth', 'Graphite', None, None, 12999.0, 14999.0)
        ]
        
        for product in products:
            cursor.execute('''
            INSERT INTO products (
                product_name, product_code, product_category, product_subcategory,
                ram, storage, connectivity, color, display_size, processor,
                dealer_price_dp, mrp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', product)
        
        # Add sample schemes
        schemes = [
            ('Special Support - Galaxy S Series', 'Special Support', '2023-08-01', '2023-08-31', 'All India', 'All Dealers'),
            ('RCM Program - A Series', 'RCM', '2023-08-01', '2023-08-31', 'North and East', 'MBO'),
            ('Upgrade Program - S Series', 'Upgrade Program', '2023-08-06', '2023-08-11', 'All India', 'GT'),
            ('Bundle Offer - Galaxy Ecosystem', 'Bundle Offer', '2023-08-01', '2023-08-31', 'All India', 'All Dealers'),
            ('Special Support - Galaxy Tab Series', 'Special Support', '2023-08-01', '2023-08-31', 'All India', 'All Dealers'),
            ('Special Support - Galaxy Book Series', 'Special Support', '2023-08-01', '2023-08-31', 'All India', 'All Dealers'),
            ('S21 FE Bonanza', 'Special Support', '2023-08-01', '2023-08-31', 'All India', 'All Dealers'),
            ('Gears & Wearables Scheme', 'Special Support', '2023-08-07', '2023-08-31', 'All India (excluding Kashmir)', 'MBO')
        ]
        
        for scheme in schemes:
            cursor.execute('''
            INSERT INTO schemes (
                scheme_name, scheme_type, scheme_period_start, scheme_period_end,
                applicable_region, dealer_type_eligibility, approval_status
            ) VALUES (?, ?, ?, ?, ?, ?, 'Approved')
            ''', scheme)
            
            scheme_id = cursor.lastrowid
            
            # Add scheme products
            if 'S Series' in scheme[0]:
                product_ids = [1, 2, 3, 4]  # S Series products
                payout_amounts = [5000, 4000, 3000, 2500]
            elif 'A Series' in scheme[0]:
                product_ids = [5, 6, 7]  # A Series products
                payout_amounts = [2000, 1500, 1000]
            elif 'Tab' in scheme[0]:
                product_ids = [12, 13, 14, 15, 16]  # Tab products
                payout_amounts = [4000, 3500, 3000, 1500, 1000]
            elif 'Book' in scheme[0]:
                product_ids = [17, 18, 19]  # Book products
                payout_amounts = [6000, 5000, 3000]
            elif 'S21 FE' in scheme[0]:
                product_ids = [4]  # S21 FE product
                payout_amounts = [3000]
            elif 'Wearables' in scheme[0]:
                product_ids = [20, 21, 22, 23]  # Wearable products
                payout_amounts = [2000, 1500, 1000, 800]
            elif 'Bundle' in scheme[0]:
                product_ids = [1, 2, 3, 20, 22]  # Mix of products for bundle
                payout_amounts = [3000, 2500, 2000, 1500, 1000]
            else:
                product_ids = [1, 5, 12, 17, 20]  # Mix of products
                payout_amounts = [3000, 2000, 2500, 4000, 1500]
            
            for i, product_id in enumerate(product_ids):
                payout_amount = payout_amounts[i] if i < len(payout_amounts) else 1000
                
                cursor.execute('''
                INSERT INTO scheme_products (
                    scheme_id, product_id, support_type, payout_type,
                    payout_amount, payout_unit, total_payout
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    scheme_id, product_id, scheme[1], 'Fixed',
                    payout_amount, 'INR', payout_amount
                ))
            
            # Add scheme rules
            cursor.execute('''
            INSERT INTO scheme_rules (
                scheme_id, rule_type, rule_description, rule_value
            ) VALUES (?, ?, ?, ?)
            ''', (
                scheme_id, 'Eligibility', f'Applicable for {scheme[5]}', scheme[5]
            ))
            
            cursor.execute('''
            INSERT INTO scheme_rules (
                scheme_id, rule_type, rule_description, rule_value
            ) VALUES (?, ?, ?, ?)
            ''', (
                scheme_id, 'Period', f'Valid from {scheme[2]} to {scheme[3]}', f'{scheme[2]} to {scheme[3]}'
            ))
        
        # Add sample sales data if we have dealers, products, and schemes
        cursor.execute("SELECT dealer_id FROM dealers")
        dealer_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT product_id FROM products")
        product_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT scheme_id FROM schemes")
        scheme_ids = [row[0] for row in cursor.fetchall()]
        
        if dealer_ids and product_ids and scheme_ids:
            # Generate sales for the last 30 days
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=30)
            
            for _ in range(100):  # Generate 100 random sales
                dealer_id = random.choice(dealer_ids)
                product_id = random.choice(product_ids)
                scheme_id = random.choice(scheme_ids)
                
                # Random date in the last 30 days
                days_ago = random.randint(0, 30)
                sale_date = end_date - datetime.timedelta(days=days_ago)
                
                # Random quantity between 1 and 5
                quantity = random.randint(1, 5)
                
                # Get product price
                cursor.execute("SELECT dealer_price_dp FROM products WHERE product_id = ?", (product_id,))
                dealer_price = cursor.fetchone()[0]
                
                # Get scheme payout
                cursor.execute("""
                SELECT payout_amount FROM scheme_products 
                WHERE scheme_id = ? AND product_id = ?
                """, (scheme_id, product_id))
                
                payout_row = cursor.fetchone()
                if payout_row:
                    payout = payout_row[0]
                else:
                    payout = random.randint(500, 3000)  # Default if no specific payout
                
                # Calculate incentive
                incentive = payout * quantity
                
                # Random IMEI
                imei = ''.join([str(random.randint(0, 9)) for _ in range(15)])
                
                # Random verification status
                status = random.choice(['Verified', 'Pending', 'Verified'])
                
                cursor.execute('''
                INSERT INTO sales_transactions (
                    dealer_id, scheme_id, product_id, quantity_sold,
                    dealer_price_dp, earned_dealer_incentive_amount,
                    imei_serial, verification_status, sale_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    dealer_id, scheme_id, product_id, quantity,
                    dealer_price, incentive, imei, status,
                    sale_date.strftime('%Y-%m-%d %H:%M:%S')
                ))
        else:
            print("No dealers, products, or schemes found. Cannot add sample sales data.")
        
        print("Added sample data to database")
    
    conn.commit()
    conn.close()

# Process a single PDF
def process_pdf(pdf_path):
    """Process a single PDF file and add to database"""
    try:
        print(f"Processing {os.path.basename(pdf_path)}...")
        
        # Load secrets
        current_dir = os.path.dirname(os.path.abspath(__file__))
        secrets_path = os.path.join(current_dir, 'secrets.json')
        
        try:
            with open(secrets_path, 'r') as f:
                secrets = json.load(f)
        except:
            secrets = {}
            print("No secrets.json found or error reading it. Using local processing only.")
        
        # Initialize AWS clients
        bedrock_client, textract_client = initialize_aws_clients(secrets)
        
        # Extract text from PDF
        pages_text = extract_text_from_pdf(pdf_path, textract_client)
        
        if not pages_text:
            print(f"Failed to extract text from {os.path.basename(pdf_path)}")
            return False
        
        # Combine text from all pages
        full_text = "\n\n".join([page[1] for page in pages_text])
        
        # Save extracted text
        text_dir = os.path.join(current_dir, 'raw_texts')
        os.makedirs(text_dir, exist_ok=True)
        
        text_filename = f"{os.path.splitext(os.path.basename(pdf_path))[0]}.txt"
        text_path = os.path.join(text_dir, text_filename)
        
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(full_text)
        
        # Extract structured data
        inference_profile_arn = secrets.get('INFERENCE_PROFILE_CLAUDE')
        structured_data = extract_structured_data_from_text(
            full_text,
            os.path.basename(pdf_path),
            bedrock_client,
            inference_profile_arn
        )
        
        if not structured_data:
            print(f"Failed to extract structured data from {os.path.basename(pdf_path)}")
            return False
        
        # Add to database
        conn = connect_db()
        cursor = conn.cursor()
        
        try:
            # Normalize all fields to ensure correct types
            scheme_name = normalize_field(structured_data.get('scheme_name'), str, f"Scheme from {os.path.basename(pdf_path)}")
            scheme_type = normalize_field(structured_data.get('scheme_type'), str, 'Special Support')
            scheme_period_start = normalize_field(structured_data.get('scheme_period_start'), str, '2023-01-01')
            scheme_period_end = normalize_field(structured_data.get('scheme_period_end'), str, '2023-12-31')
            applicable_region = normalize_field(structured_data.get('applicable_region'), str, 'All India')
            dealer_type_eligibility = normalize_field(structured_data.get('dealer_type_eligibility'), str, 'All Dealers')
            
            # Add scheme
            cursor.execute("""
            INSERT INTO schemes (
                scheme_name, scheme_type, scheme_period_start, scheme_period_end,
                applicable_region, dealer_type_eligibility, scheme_document_name,
                raw_extracted_text_path, deal_status, approval_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Active', 'Pending')
            """, (
                scheme_name,
                scheme_type,
                scheme_period_start,
                scheme_period_end,
                applicable_region,
                dealer_type_eligibility,
                os.path.basename(pdf_path),
                text_path
            ))
            
            scheme_id = cursor.lastrowid
            
            # Add products
            for product in structured_data.get('products', []):
                # Normalize product fields
                product_name = normalize_field(product.get('product_name'), str, f"Product {uuid.uuid4().hex[:8]}")
                product_code = normalize_field(product.get('product_code'), str, f"CODE-{uuid.uuid4().hex[:8]}")
                product_category = normalize_field(product.get('product_category'), str, 'Mobile')
                product_subcategory = normalize_field(product.get('product_subcategory'), str, 'Other')
                ram = normalize_field(product.get('ram'), str)
                storage = normalize_field(product.get('storage'), str)
                connectivity = normalize_field(product.get('connectivity'), str)
                dealer_price_dp = normalize_field(product.get('dealer_price_dp'), float, random.randint(10000, 100000))
                mrp = normalize_field(product.get('mrp'), float, random.randint(15000, 120000))
                
                # First check if product exists
                cursor.execute("""
                SELECT product_id FROM products 
                WHERE product_name = ? AND product_code = ?
                """, (product_name, product_code))
                
                existing = cursor.fetchone()
                if existing:
                    product_id = existing[0]
                else:
                    # Add new product
                    cursor.execute("""
                    INSERT INTO products (
                        product_name, product_code, product_category, product_subcategory,
                        ram, storage, connectivity, dealer_price_dp, mrp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        product_name,
                        product_code,
                        product_category,
                        product_subcategory,
                        ram,
                        storage,
                        connectivity,
                        dealer_price_dp,
                        mrp
                    ))
                    product_id = cursor.lastrowid
                
                # Normalize scheme product fields
                support_type = normalize_field(product.get('support_type'), str, scheme_type)
                payout_type = normalize_field(product.get('payout_type'), str, 'Fixed')
                payout_amount = normalize_field(product.get('payout_amount'), float, 1000.0)
                payout_unit = normalize_field(product.get('payout_unit'), str, 'INR')
                dealer_contribution = normalize_field(product.get('dealer_contribution'), float, 0.0)
                total_payout = normalize_field(product.get('total_payout'), float, payout_amount)
                is_dealer_incentive = 1 if product.get('is_dealer_incentive', True) else 0
                is_bundle_offer = 1 if product.get('is_bundle_offer', False) else 0
                bundle_price = normalize_field(product.get('bundle_price'), float)
                is_upgrade_offer = 1 if product.get('is_upgrade_offer', False) else 0
                is_slab_based = 1 if product.get('is_slab_based', False) else 0
                
                # Add scheme product
                cursor.execute("""
                INSERT INTO scheme_products (
                    scheme_id, product_id, support_type, payout_type, payout_amount,
                    payout_unit, dealer_contribution, total_payout, is_dealer_incentive,
                    is_bundle_offer, bundle_price, is_upgrade_offer, is_slab_based
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    scheme_id,
                    product_id,
                    support_type,
                    payout_type,
                    payout_amount,
                    payout_unit,
                    dealer_contribution,
                    total_payout,
                    is_dealer_incentive,
                    is_bundle_offer,
                    bundle_price,
                    is_upgrade_offer,
                    is_slab_based
                ))
            
            # Add rules
            for rule in structured_data.get('scheme_rules', []):
                # Normalize rule fields
                rule_type = normalize_field(rule.get('rule_type'), str, 'General')
                rule_description = normalize_field(rule.get('rule_description'), str, 'No description')
                rule_value = normalize_field(rule.get('rule_value'), str)
                
                cursor.execute("""
                INSERT INTO scheme_rules (
                    scheme_id, rule_type, rule_description, rule_value
                ) VALUES (?, ?, ?, ?)
                """, (
                    scheme_id,
                    rule_type,
                    rule_description,
                    rule_value
                ))
            
            conn.commit()
            print(f"Added scheme from {os.path.basename(pdf_path)} to database")
            return True
        
        except Exception as e:
            conn.rollback()
            print(f"Error adding scheme: {e}")
            return False
        
        finally:
            conn.close()
    
    except Exception as e:
        print(f"Failed to process {os.path.basename(pdf_path)}: {e}")
        return False

# Process multiple PDFs
def process_multiple_pdfs(directory):
    """Process all PDFs in a directory"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    pdf_dir = os.path.join(current_dir, directory)
    
    if not os.path.exists(pdf_dir):
        os.makedirs(pdf_dir, exist_ok=True)
        print(f"Created directory: {pdf_dir}")
        return
    
    pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith('.pdf')]
    
    if not pdf_files:
        print(f"No PDF files found in {pdf_dir}")
        return
    
    # Create tables if they don't exist
    create_tables()
    
    # Add sample dealers
    add_sample_data()
    
    # Process each PDF
    for pdf_file in pdf_files:
        pdf_path = os.path.join(pdf_dir, pdf_file)
        success = process_pdf(pdf_path)
        
        if not success:
            print(f"Failed to process {pdf_file}")

# Main function
if __name__ == "__main__":
    # Create tables
    create_tables()
    
    # Add sample data
    add_sample_data()
    
    # Process PDFs in the schemes directory
    process_multiple_pdfs('schemes')

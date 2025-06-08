import os
import sqlite3
import random
import datetime

def add_sample_data():
    """Add sample data to the database - limited to 1-2 schemes as requested"""
    conn = connect_db()
    cursor = conn.cursor()
    
    # Add sample dealers
    dealers = [
        ('Galaxy Mobile Store', 'GMS001', 'Premium', 'North', 'Delhi', 'New Delhi', 
         'Raj Kumar', 'raj@galaxymobile.com', '9876543210'),
        ('Mobile World', 'MW002', 'Standard', 'South', 'Karnataka', 'Bangalore', 
         'Priya Singh', 'priya@mobileworld.com', '8765432109'),
        ('Tech Hub', 'TH003', 'Premium', 'West', 'Maharashtra', 'Mumbai', 
         'Amit Patel', 'amit@techhub.com', '7654321098'),
        ('Phone Paradise', 'PP004', 'Standard', 'East', 'West Bengal', 'Kolkata', 
         'Sneha Das', 'sneha@phonepar.com', '6543210987'),
        ('Digital Zone', 'DZ005', 'Premium', 'Central', 'Madhya Pradesh', 'Bhopal', 
         'Vikram Singh', 'vikram@digitalzone.com', '5432109876')
    ]
    
    for dealer in dealers:
        cursor.execute("""
        INSERT INTO dealers (
            dealer_name, dealer_code, dealer_type, region, state, city, 
            contact_person, contact_email, contact_phone
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, dealer)
    
    # Add sample products
    products = [
        ('Galaxy S23 Ultra', 'SM-S918', 'Mobile', 'S Series', '12GB', '512GB', '5G', 'Phantom Black', '6.8"', 'Snapdragon 8 Gen 2', 89999, 124999),
        ('Galaxy S23+', 'SM-S916', 'Mobile', 'S Series', '8GB', '256GB', '5G', 'Cream', '6.6"', 'Snapdragon 8 Gen 2', 74999, 94999),
        ('Galaxy S23', 'SM-S911', 'Mobile', 'S Series', '8GB', '128GB', '5G', 'Green', '6.1"', 'Snapdragon 8 Gen 2', 64999, 74999),
        ('Galaxy Z Fold5', 'SM-F946', 'Mobile', 'Fold Series', '12GB', '512GB', '5G', 'Phantom Black', '7.6"', 'Snapdragon 8 Gen 2', 154999, 164999),
        ('Galaxy Z Flip5', 'SM-F731', 'Mobile', 'Flip Series', '8GB', '256GB', '5G', 'Mint', '6.7"', 'Snapdragon 8 Gen 2', 99999, 109999),
        ('Galaxy A54', 'SM-A546', 'Mobile', 'A Series', '8GB', '128GB', '5G', 'Awesome Violet', '6.4"', 'Exynos 1380', 34999, 38999),
        ('Galaxy A34', 'SM-A346', 'Mobile', 'A Series', '8GB', '128GB', '5G', 'Awesome Silver', '6.6"', 'Dimensity 1080', 24999, 28999),
        ('Galaxy Tab S9 Ultra', 'SM-X916', 'Tablet', 'Tab S Series', '12GB', '256GB', '5G', 'Graphite', '14.6"', 'Snapdragon 8 Gen 2', 99999, 109999),
        ('Galaxy Tab S9+', 'SM-X816', 'Tablet', 'Tab S Series', '12GB', '256GB', '5G', 'Beige', '12.4"', 'Snapdragon 8 Gen 2', 84999, 94999),
        ('Galaxy Tab S9', 'SM-X716', 'Tablet', 'Tab S Series', '8GB', '128GB', '5G', 'Beige', '11"', 'Snapdragon 8 Gen 2', 69999, 79999),
        ('Galaxy Book3 Pro', 'NP960', 'Laptop', 'Book Series', '16GB', '512GB', 'Wi-Fi', 'Graphite', '14"', 'Intel Core i7', 114999, 124999),
        ('Galaxy Watch6 Classic', 'SM-R960', 'Wearable', 'Watch Series', '2GB', '16GB', 'Bluetooth', 'Black', '1.5"', 'Exynos W930', 34999, 39999),
        ('Galaxy Buds2 Pro', 'SM-R510', 'Audio', 'Buds Series', '-', '-', 'Bluetooth', 'Graphite', '-', '-', 17999, 19999)
    ]
    
    for product in products:
        cursor.execute("""
        INSERT INTO products (
            product_name, product_code, product_category, product_subcategory,
            ram, storage, connectivity, color, display_size, processor,
            dealer_price_dp, mrp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, product)
    
    # Add only 2 sample schemes as requested
    schemes = [
        ('Samsung Upgrade Program - GT', 'Upgrade Program', '2023-08-01', '2023-08-31', 
         'All India', 'All Dealers', 'Active', 'Approved', 'System Admin', 
         '11. 2023-Aug-Scheme 6-11- Samsung Upgrade Program - GT_(000175731_R 0)_SIEL-41851.pdf', 
         None, 'Upgrade program for Galaxy smartphones'),
        ('Special Support - Galaxy Book', 'Special Support', '2023-08-01', '2023-08-31', 
         'All India', 'All Dealers', 'Active', 'Approved', 'System Admin', 
         '01. Scheme 6.16 - Special Support - Galaxy Book_(000176099_R 0)_SIEL-41900.pdf', 
         None, 'Special support for Galaxy Book series')
    ]
    
    for scheme in schemes:
        cursor.execute("""
        INSERT INTO schemes (
            scheme_name, scheme_type, scheme_period_start, scheme_period_end,
            applicable_region, dealer_type_eligibility, deal_status, approval_status, approved_by,
            scheme_document_name, raw_extracted_text_path, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, scheme)
        
        scheme_id = cursor.lastrowid
        
        # Add scheme products based on scheme type
        if 'Upgrade Program' in scheme[0]:
            # Add products for Upgrade Program
            scheme_products = [
                (scheme_id, 3, 'Exchange Bonus', 'Fixed', 5000, 'INR', 0, 5000, 0, 0, 1, 0, 'Galaxy Buds2 Pro'),
                (scheme_id, 4, 'Exchange Bonus', 'Fixed', 8000, 'INR', 0, 8000, 0, 0, 1, 0, None),
                (scheme_id, 5, 'Exchange Bonus', 'Fixed', 6000, 'INR', 0, 6000, 0, 0, 1, 0, 'Galaxy Watch4')
            ]
        else:
            # Add products for Special Support
            scheme_products = [
                (scheme_id, 11, 'Cashback', 'Fixed', 10000, 'INR', 0, 10000, 1, 0, 0, 0, 'Galaxy Buds2 Pro'),
                (scheme_id, 12, 'Bundle Offer', 'Percentage', 10, '%', 0, 0, 0, 1, 0, 0, None)
            ]
        
        for product in scheme_products:
            cursor.execute("""
            INSERT INTO scheme_products (
                scheme_id, product_id, support_type, payout_type, payout_amount,
                payout_unit, dealer_contribution, total_payout, is_dealer_incentive,
                is_bundle_offer, is_upgrade_offer, is_slab_based, free_item_description
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, product)
    
    # Add sample sales data
    for _ in range(10):
        # Get random dealer
        cursor.execute("SELECT dealer_id FROM dealers ORDER BY RANDOM() LIMIT 1")
        dealer_id = cursor.fetchone()[0]
        
        # Get random scheme
        cursor.execute("SELECT scheme_id FROM schemes ORDER BY RANDOM() LIMIT 1")
        scheme_id = cursor.fetchone()[0]
        
        # Get random product for this scheme
        cursor.execute("""
        SELECT sp.product_id, p.dealer_price_dp, sp.payout_amount, sp.payout_type
        FROM scheme_products sp
        JOIN products p ON sp.product_id = p.product_id
        WHERE sp.scheme_id = ?
        ORDER BY RANDOM() LIMIT 1
        """, (scheme_id,))
        
        product_data = cursor.fetchone()
        
        if product_data:
            product_id = product_data[0]
            dealer_price = product_data[1]
            payout_amount = product_data[2]
            payout_type = product_data[3]
            
            # Calculate incentive
            if payout_type == 'Percentage':
                incentive = dealer_price * payout_amount / 100
            else:
                incentive = payout_amount
            
            # Random quantity between 1 and 3
            quantity = random.randint(1, 3)
            
            # Random date in the last 30 days
            days_ago = random.randint(0, 30)
            sale_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
            
            # Random IMEI
            imei = ''.join([str(random.randint(0, 9)) for _ in range(15)])
            
            cursor.execute("""
            INSERT INTO sales_transactions (
                dealer_id, scheme_id, product_id, quantity_sold, dealer_price_dp,
                earned_dealer_incentive_amount, imei_serial, sale_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (dealer_id, scheme_id, product_id, quantity, dealer_price, incentive * quantity, imei, sale_date))
    
    conn.commit()
    conn.close()
    
    print("Sample data added successfully.")

def connect_db():
    """Connect to the SQLite database"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'dns_database.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

if __name__ == "__main__":
    # Create tables
    from pdf_processor_fixed import create_tables
    create_tables()
    
    # Add sample data
    add_sample_data()

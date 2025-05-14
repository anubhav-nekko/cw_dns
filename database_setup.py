import sqlite3
import os

DATABASE_NAME = "dealer_schemes.db"
DB_PATH = os.path.join(os.path.dirname(__file__), DATABASE_NAME) # Store DB in the same directory as this script

def connect_db():
    """Connects to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row # Access columns by name
    return conn

def create_tables():
    """Creates all necessary tables in the database if they don't already exist."""
    conn = connect_db()
    cursor = conn.cursor()

    # 1. deals Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS deals (
        deal_id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_name TEXT UNIQUE NOT NULL,
        scheme_document_name TEXT,
        scheme_period_start DATE,
        scheme_period_end DATE,
        applicable_region TEXT,
        dealer_type_eligibility TEXT,
        upload_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        raw_extracted_text_path TEXT,
        deal_status TEXT
    );
    """)

    # 2. products Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_code TEXT UNIQUE,
        product_name TEXT NOT NULL,
        product_category TEXT
    );
    """)

    # 3. deal_product_offers Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS deal_product_offers (
        offer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER,
        product_id INTEGER,
        product_group_description TEXT,
        support_type TEXT,
        payout_type TEXT, 
        payout_value REAL,
        payout_unit TEXT,
        target_metric TEXT,
        target_value_description TEXT,
        conditions_exclusions TEXT,
        is_dealer_incentive BOOLEAN,
        FOREIGN KEY (deal_id) REFERENCES deals (deal_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    );
    """)

    # 4. sales_transactions Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales_transactions (
        transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER,
        product_id INTEGER,
        offer_id INTEGER, 
        quantity_sold INTEGER DEFAULT 1,
        sale_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        dealer_price_dp REAL,
        gst_amount REAL,
        net_dp_after_support REAL,
        applied_customer_discount_amount REAL,
        earned_dealer_incentive_amount REAL,
        billing_system_ref_id TEXT,
        FOREIGN KEY (deal_id) REFERENCES deals (deal_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id),
        FOREIGN KEY (offer_id) REFERENCES deal_product_offers (offer_id)
    );
    """)

    # 5. dealer_targets Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dealer_targets (
        target_id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER,
        target_description TEXT,
        target_product_id INTEGER,
        target_product_group_description TEXT,
        target_quantity INTEGER,
        target_value REAL,
        target_metric TEXT,
        is_achieved BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (deal_id) REFERENCES deals (deal_id),
        FOREIGN KEY (target_product_id) REFERENCES products (product_id)
    );
    """)

    conn.commit()
    conn.close()
    print(f"Database tables created/verified in {DB_PATH}")

# --- Data Population Functions (Examples - to be expanded) ---

def add_deal(deal_name, scheme_document_name, scheme_period_start, scheme_period_end, applicable_region, dealer_type_eligibility, raw_extracted_text_path, deal_status):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO deals (deal_name, scheme_document_name, scheme_period_start, scheme_period_end, applicable_region, dealer_type_eligibility, raw_extracted_text_path, deal_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (deal_name, scheme_document_name, scheme_period_start, scheme_period_end, applicable_region, dealer_type_eligibility, raw_extracted_text_path, deal_status))
        conn.commit()
        deal_id = cursor.lastrowid
        print(f"Added deal: {deal_name} with ID: {deal_id}")
        return deal_id
    except sqlite3.IntegrityError as e:
        print(f"Error adding deal {deal_name}: {e} (Likely already exists)")
        # Optionally, fetch existing deal_id if it already exists
        cursor.execute("SELECT deal_id FROM deals WHERE deal_name = ?", (deal_name,))
        existing_deal = cursor.fetchone()
        return existing_deal['deal_id'] if existing_deal else None
    finally:
        conn.close()

def add_product(product_code, product_name, product_category):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO products (product_code, product_name, product_category)
        VALUES (?, ?, ?)
        """, (product_code, product_name, product_category))
        conn.commit()
        product_id = cursor.lastrowid
        print(f"Added product: {product_name} with ID: {product_id}")
        return product_id
    except sqlite3.IntegrityError:
        # Product with this code likely already exists, fetch its ID
        cursor.execute("SELECT product_id FROM products WHERE product_code = ? OR (product_code IS NULL AND product_name = ?)", (product_code, product_name))
        existing_product = cursor.fetchone()
        if existing_product:
            print(f"Product {product_name} (Code: {product_code}) already exists with ID: {existing_product['product_id']}")
            return existing_product['product_id']
        return None # Should not happen if insert failed for other reasons
    finally:
        conn.close()

def add_deal_product_offer(deal_id, product_id, product_group_description, support_type, payout_type, payout_value, payout_unit, target_metric, target_value_description, conditions_exclusions, is_dealer_incentive):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO deal_product_offers (deal_id, product_id, product_group_description, support_type, payout_type, payout_value, payout_unit, target_metric, target_value_description, conditions_exclusions, is_dealer_incentive)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (deal_id, product_id, product_group_description, support_type, payout_type, payout_value, payout_unit, target_metric, target_value_description, conditions_exclusions, is_dealer_incentive))
    conn.commit()
    offer_id = cursor.lastrowid
    print(f"Added offer ID: {offer_id} for deal ID: {deal_id}")
    conn.close()
    return offer_id

# Further functions for sales_transactions and dealer_targets will be added as part of their respective features.

if __name__ == "__main__":
    create_tables()
    # Example usage (can be commented out or removed)
    # test_deal_id = add_deal(
    #     deal_name="August Mega Sale 2023", 
    #     scheme_document_name="aug_mega_sale.pdf", 
    #     scheme_period_start="2023-08-01", 
    #     scheme_period_end="2023-08-31", 
    #     applicable_region="All India", 
    #     dealer_type_eligibility="All Dealers", 
    #     raw_extracted_text_path="/path/to/text.txt", 
    #     deal_status="Active"
    # )
    # test_product_id = add_product(product_code="SM-S900", product_name="Galaxy S23", product_category="Smart Phones")
    # if test_deal_id and test_product_id:
    #     add_deal_product_offer(
    #         deal_id=test_deal_id, 
    #         product_id=test_product_id, 
    #         product_group_description=None, 
    #         support_type="Cashback", 
    #         payout_type="Percentage", 
    #         payout_value=5.0, 
    #         payout_unit="%", 
    #         target_metric=None, 
    #         target_value_description=None, 
    #         conditions_exclusions="Min. purchase of 1 unit", 
    #         is_dealer_incentive=False
    #     )
    print("Database setup script completed.")


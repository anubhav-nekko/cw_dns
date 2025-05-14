import sqlite3
import datetime
import os
from database_setup import connect_db, DB_PATH # Assuming database_setup.py is in the same directory

# Ensure DB_PATH is correctly pointing to the database file
# If database_setup.py creates the DB in its own directory, and this script is in the same dir, it should be fine.

def get_active_deal_for_product(product_id, sale_date_str=None):
    """Finds an active deal and offer for a given product ID on a specific sale date."""
    conn = connect_db()
    cursor = conn.cursor()

    sale_date = datetime.datetime.strptime(sale_date_str, "%Y-%m-%d").date() if sale_date_str else datetime.date.today()

    # Query for offers matching the product_id, within an active deal period
    # This query assumes a direct link or a product group that can be matched.
    # For simplicity, this example focuses on direct product_id matches in deal_product_offers.
    # A more complex query would be needed for product_group_description matching.
    cursor.execute("""
        SELECT dpo.*, d.deal_name, p.product_name, p.dealer_price_dp 
        FROM deal_product_offers dpo
        JOIN deals d ON dpo.deal_id = d.deal_id
        JOIN products p ON dpo.product_id = p.product_id
        WHERE dpo.product_id = ? 
        AND d.deal_status = 'Active'
        AND DATE(d.scheme_period_start) <= ? 
        AND DATE(d.scheme_period_end) >= ?
        ORDER BY d.upload_timestamp DESC, dpo.offer_id DESC  -- Get the latest applicable offer
        LIMIT 1
    """, (product_id, sale_date, sale_date))
    
    offer_details = cursor.fetchone()
    conn.close()
    return offer_details

def record_sale_transaction(deal_id, product_id, offer_id, quantity_sold, dealer_price_dp, 
                            gst_amount, net_dp_after_support, 
                            applied_customer_discount_amount, earned_dealer_incentive_amount, 
                            billing_system_ref_id=None, sale_timestamp_str=None):
    """Records a sales transaction in the database."""
    conn = connect_db()
    cursor = conn.cursor()

    sale_timestamp = sale_timestamp_str if sale_timestamp_str else datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        cursor.execute("""
            INSERT INTO sales_transactions (
                deal_id, product_id, offer_id, quantity_sold, sale_timestamp, 
                dealer_price_dp, gst_amount, net_dp_after_support, 
                applied_customer_discount_amount, earned_dealer_incentive_amount, billing_system_ref_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (deal_id, product_id, offer_id, quantity_sold, sale_timestamp, 
              dealer_price_dp, gst_amount, net_dp_after_support, 
              applied_customer_discount_amount, earned_dealer_incentive_amount, billing_system_ref_id))
        conn.commit()
        transaction_id = cursor.lastrowid
        print(f"Recorded sale transaction ID: {transaction_id} for product ID: {product_id}")
        
        # Optionally, update dealer targets here
        update_dealer_targets(deal_id, product_id, quantity_sold, conn)
        
        return transaction_id
    except sqlite3.Error as e:
        print(f"Database error recording sale: {e}")
        return None
    finally:
        conn.close()

def update_dealer_targets(deal_id, product_id, quantity_sold, db_connection):
    """Updates dealer targets based on a sale. (Simplified)"""
    cursor = db_connection.cursor()
    # Find relevant targets for this deal and product
    cursor.execute("""
        SELECT target_id, target_quantity, target_metric FROM dealer_targets
        WHERE deal_id = ? AND (target_product_id = ? OR target_product_group_description IS NOT NULL) AND is_achieved = 0
    """, (deal_id, product_id))
    
    targets = cursor.fetchall()
    for target in targets:
        if target["target_metric"] == "Units Sold":
            # This is a simplified check. A real system would sum up all sales for this target.
            # For now, let's assume we need to track cumulative sales elsewhere or this is one-off.
            # To properly update, we'd need to fetch current sold quantity for this target and add to it.
            print(f"Target ID {target['target_id']} for deal {deal_id}, product {product_id} might be affected by {quantity_sold} units.")
            # Add logic here to check if target_quantity is met and update is_achieved.
            # For MVP, this might involve querying sales_transactions table to sum up relevant sales.
    # db_connection.commit() # Commit if changes are made
    pass # Placeholder for more complex target update logic

def simulate_billing_api_call(product_id_to_sell, quantity, sale_date_iso="", billing_ref="BILL_MOCK_123"):
    """Simulates a billing system calling to record a sale and get offer details."""
    print(f"\n--- Simulating Billing API Call for Product ID: {product_id_to_sell}, Quantity: {quantity} ---")
    
    # 1. Get product base price (assuming it's in products table or passed in)
    # For this mock, we'll try to get it from the offer details if available, or assume a price.
    # In a real scenario, the billing system would likely know the product's current DP.

    # 2. Find active offer for the product
    active_offer = get_active_deal_for_product(product_id_to_sell, sale_date_iso if sale_date_iso else None)

    if not active_offer:
        print(f"No active offer found for Product ID {product_id_to_sell} on {sale_date_iso if sale_date_iso else 'today'}.")
        # Proceed with sale without scheme benefits or handle as error
        # For now, let's assume a base price and record sale without specific offer benefits
        # This part needs more robust handling based on business rules.
        # For MVP, we might require an offer to be present or have default handling.
        # Let's assume for now we can't proceed without an offer for simplicity of discount calculation.
        return {"status": "error", "message": "No active offer found."}

    deal_id = active_offer["deal_id"]
    offer_id = active_offer["offer_id"]
    product_name = active_offer["product_name"]
    base_dp = active_offer["dealer_price_dp"] if active_offer["dealer_price_dp"] else 10000 # Fallback DP
    
    print(f"Found active offer: '{active_offer['support_type']}' for Deal: '{active_offer['deal_name']}' on Product: '{product_name}' (ID: {product_id_to_sell})")

    # 3. Calculate discounts/incentives based on the offer
    applied_discount = 0.0
    earned_incentive = 0.0
    net_dp = base_dp * quantity

    if active_offer["payout_type"] == "Percentage":
        payout_val = active_offer["payout_value"] / 100.0
        if active_offer["is_dealer_incentive"]:
            earned_incentive = net_dp * payout_val
        else: # Customer discount
            applied_discount = net_dp * payout_val
    elif active_offer["payout_type"] == "Fixed Amount":
        payout_val = active_offer["payout_value"]
        if active_offer["is_dealer_incentive"]:
            earned_incentive = payout_val * quantity # Assuming fixed amount per unit
        else: # Customer discount
            applied_discount = payout_val * quantity # Assuming fixed amount per unit
    
    net_dp_after_scheme_support = net_dp - applied_discount # This is simplified; actual net DP calculation can be complex
    gst_amount_mock = net_dp_after_scheme_support * 0.18 # Assuming 18% GST for mock

    print(f"Base DP: {base_dp}, Quantity: {quantity}, Total DP: {net_dp}")
    print(f"Applied Customer Discount: {applied_discount}")
    print(f"Earned Dealer Incentive: {earned_incentive}")
    print(f"Net DP after Support: {net_dp_after_scheme_support}, GST (mock): {gst_amount_mock}")

    # 4. Record the sale transaction
    transaction_id = record_sale_transaction(
        deal_id=deal_id,
        product_id=product_id_to_sell,
        offer_id=offer_id,
        quantity_sold=quantity,
        sale_timestamp_str=sale_date_iso + " " + datetime.datetime.now().strftime("%H:%M:%S") if sale_date_iso else None,
        dealer_price_dp=base_dp, # Price per unit
        gst_amount=gst_amount_mock,
        net_dp_after_support=net_dp_after_scheme_support / quantity if quantity > 0 else 0, # Per unit
        applied_customer_discount_amount=applied_discount / quantity if quantity > 0 else 0, # Per unit
        earned_dealer_incentive_amount=earned_incentive / quantity if quantity > 0 else 0, # Per unit
        billing_system_ref_id=billing_ref
    )

    if transaction_id:
        print(f"Sale successfully recorded. Transaction ID: {transaction_id}")
        return {
            "status": "success", 
            "transaction_id": transaction_id, 
            "product_name": product_name,
            "deal_name": active_offer["deal_name"],
            "offer_applied": active_offer["support_type"],
            "discount_amount": applied_discount,
            "incentive_earned": earned_incentive
        }
    else:
        print("Failed to record sale.")
        return {"status": "error", "message": "Failed to record sale transaction."}

if __name__ == "__main__":
    # --- Setup for Testing ---
    # Ensure database and tables exist
    from database_setup import create_tables, add_deal, add_product, add_deal_product_offer
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}, creating...")
        create_tables()
    else:
        # For testing, you might want to clear and recreate or just ensure tables exist
        create_tables() # Ensures tables are there
        print(f"Using existing database: {DB_PATH}")

    # Add some sample data if it doesn't exist or for a clean test run
    # This should ideally be idempotent or run on a fresh DB for testing
    print("\n--- Populating Sample Data for Billing Mock Test ---")
    deal1_id = add_deal("Summer Bonanza", "summer_doc.pdf", "2025-05-01", "2025-05-31", "All India", "All Dealers", "/path/to/text1.txt", "Active")
    prod1_id = add_product("SM-G900", "Galaxy SuperPhone", "Smart Phones")
    prod2_id = add_product("SM-T500", "Galaxy TabMax", "Tablets")
    
    # Update product table with a dealer_price_dp if your schema supports it, or manage price elsewhere
    # For this test, we'll assume the get_active_deal_for_product function can fetch it or has a fallback.
    # Let's add a DP to products table for more realistic testing if schema was adjusted
    # (Assuming products table has dealer_price_dp column, which it does not in the current schema proposal)
    # So, the price will be fetched from the offer_details or a fallback in simulate_billing_api_call
    # To make it more robust, let's add a DP to the product table or pass it to simulate_billing_api_call
    # For now, the mock `get_active_deal_for_product` adds a `dealer_price_dp` to the fetched offer details for testing.
    # We need to ensure `products` table has `dealer_price_dp` or the logic in `get_active_deal_for_product` is updated.
    # The schema for `products` does not have `dealer_price_dp`. Let's adjust `get_active_deal_for_product` to add a mock DP.
    # The current `get_active_deal_for_product` already joins with products and selects `p.dealer_price_dp`
    # This means the `products` table *should* have `dealer_price_dp`. Let's assume it's added or mocked.
    # For the purpose of this test, we will rely on the fallback in `simulate_billing_api_call` or ensure the product table has it.
    # Let's assume we add it to the product table for the test scenario.
    conn_test = connect_db()
    cursor_test = conn_test.cursor()
    try:
        cursor_test.execute("ALTER TABLE products ADD COLUMN dealer_price_dp REAL")
        conn_test.commit()
        print("Added dealer_price_dp column to products table for testing.")
    except sqlite3.OperationalError:
        print("dealer_price_dp column likely already exists in products table.")
    
    if prod1_id:
        cursor_test.execute("UPDATE products SET dealer_price_dp = ? WHERE product_id = ?", (25000.00, prod1_id))
    if prod2_id:
        cursor_test.execute("UPDATE products SET dealer_price_dp = ? WHERE product_id = ?", (45000.00, prod2_id))
    conn_test.commit()
    conn_test.close()

    if deal1_id and prod1_id:
        add_deal_product_offer(deal1_id, prod1_id, None, "Discount", "Percentage", 10.0, "%", None, None, "Min 1 unit", False)
    
    # --- Run Simulation ---
    today_iso = datetime.date.today().isoformat()
    sale_result = simulate_billing_api_call(product_id_to_sell=prod1_id, quantity=2, sale_date_iso=today_iso)
    print("\nBilling Simulation Result:", sale_result)

    # Simulate for a product with no specific offer (if any)
    # prod_no_offer_id = add_product("SM-X100", "BasicPhone", "Feature Phones")
    # if prod_no_offer_id:
    #     cursor_test.execute("UPDATE products SET dealer_price_dp = ? WHERE product_id = ?", (5000.00, prod_no_offer_id))
    #     conn_test.commit()
    # sale_result_no_offer = simulate_billing_api_call(product_id_to_sell=prod_no_offer_id, quantity=1, sale_date_iso=today_iso)
    # print("\nBilling Simulation Result (No Offer Expected):", sale_result_no_offer)
    
    print("\n--- Mock Billing System Test Completed ---")


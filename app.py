# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import datetime
import sqlite3

# Import backend modules
from database_setup import create_tables, connect_db, add_deal, add_product, add_deal_product_offer
from pdf_processor import extract_text_from_pdf_pages, extract_structured_data_from_text
from billing_system_mock import simulate_billing_api_call, get_active_deal_for_product, record_sale_transaction

# --- Database Initialization ---
DB_FILE = os.path.join(os.path.dirname(__file__), "dealer_schemes.db")

if not os.path.exists(DB_FILE):
    st.info(f"Database not found at {DB_FILE}, creating tables...")
    create_tables() # This function from database_setup.py creates the DB and tables
    st.success("Database and tables created successfully!")
else:
    # Ensure tables exist even if DB file is present
    create_tables()

# --- Helper Functions for DB Interaction (Streamlit specific) ---
def get_all_deals():
    conn = connect_db()
    deals = pd.read_sql_query("SELECT deal_id, deal_name, scheme_period_start, scheme_period_end, deal_status FROM deals ORDER BY upload_timestamp DESC", conn)
    conn.close()
    return deals

def get_deal_details(deal_id):
    conn = connect_db()
    deal = pd.read_sql_query(f"SELECT * FROM deals WHERE deal_id = {deal_id}", conn)
    offers = pd.read_sql_query(f"SELECT o.*, p.product_name, p.product_code FROM deal_product_offers o LEFT JOIN products p ON o.product_id = p.product_id WHERE o.deal_id = {deal_id}", conn)
    targets = pd.read_sql_query(f"SELECT * FROM dealer_targets WHERE deal_id = {deal_id}", conn)
    conn.close()
    return deal, offers, targets

def get_all_products_for_sale():
    conn = connect_db()
    # Assuming products table has dealer_price_dp, if not, this needs adjustment or mock data
    products = pd.read_sql_query("SELECT product_id, product_name, product_code, product_category, dealer_price_dp FROM products ORDER BY product_name", conn)
    conn.close()
    return products

def get_sales_data(deal_id=None, date_range=None):
    conn = connect_db()
    query = """
    SELECT s.sale_timestamp, p.product_name, s.quantity_sold, s.dealer_price_dp, 
           s.applied_customer_discount_amount, s.earned_dealer_incentive_amount, d.deal_name
    FROM sales_transactions s
    JOIN products p ON s.product_id = p.product_id
    JOIN deals d ON s.deal_id = d.deal_id
    """
    filters = []
    params = []
    if deal_id:
        filters.append("s.deal_id = ?")
        params.append(deal_id)
    if date_range and len(date_range) == 2:
        filters.append("DATE(s.sale_timestamp) BETWEEN ? AND ?")
        params.extend([date_range[0].strftime("%Y-%m-%d"), date_range[1].strftime("%Y-%m-%d")])
    
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY s.sale_timestamp DESC"
    
    sales = pd.read_sql_query(query, conn, params=params)
    conn.close()
    if not sales.empty:
        sales["sale_timestamp"] = pd.to_datetime(sales["sale_timestamp"])
    return sales

# --- Page Configurations ---
st.set_page_config(layout="wide", page_title="Dealer Scheme Management MVP")

# --- Main Application --- 
def main():
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.radio("Choose a screen:",
                                ["Dashboard", "Upload Scheme PDF", "View Deals", "Simulate Sale"])

    if app_mode == "Dashboard":
        render_dashboard()
    elif app_mode == "Upload Scheme PDF":
        render_file_uploader()
    elif app_mode == "View Deals":
        render_view_deals()
    elif app_mode == "Simulate Sale":
        render_simulate_sale()

# --- Screen Rendering Functions ---
def render_dashboard():
    st.title("Sales & Scheme Dashboard")

    deals_df = get_all_deals()
    if deals_df.empty:
        st.warning("No deals found. Please upload a scheme PDF first.")
        return

    # --- Filters ---
    st.sidebar.header("Dashboard Filters")
    selected_deal_names = st.sidebar.multiselect("Select Deals to Compare (Max 4):", 
                                                 deals_df["deal_name"].unique(), 
                                                 default=deals_df["deal_name"].unique()[:1]) # Default to first deal
    
    if not selected_deal_names:
        st.warning("Please select at least one deal to display data.")
        return
    
    if len(selected_deal_names) > 4:
        st.warning("Please select a maximum of 4 deals for comparison.")
        selected_deal_names = selected_deal_names[:4]

    selected_deal_ids = deals_df[deals_df["deal_name"].isin(selected_deal_names)]["deal_id"].tolist()

    date_range_option = st.sidebar.selectbox("Select Date Range:", 
                                             ["Last 7 Days", "Last 15 Days", "Last 30 Days", "Lifetime", "Custom Range"],
                                             index=3) # Default to Lifetime
    
    start_date, end_date = None, None
    today = datetime.date.today()
    if date_range_option == "Last 7 Days":
        start_date = today - datetime.timedelta(days=6)
        end_date = today
    elif date_range_option == "Last 15 Days":
        start_date = today - datetime.timedelta(days=14)
        end_date = today
    elif date_range_option == "Last 30 Days":
        start_date = today - datetime.timedelta(days=29)
        end_date = today
    elif date_range_option == "Custom Range":
        start_date = st.sidebar.date_input("Start Date", today - datetime.timedelta(days=6))
        end_date = st.sidebar.date_input("End Date", today)
    # Lifetime means no date filter unless custom is chosen and then cleared

    # --- Fetch Data based on filters ---
    # For simplicity, we fetch all sales for selected deals and then filter by date in pandas if needed
    # Or, pass date_range to get_sales_data
    date_filter_for_sql = (start_date, end_date) if start_date and end_date else None
    
    combined_sales_df = pd.DataFrame()
    for deal_id in selected_deal_ids:
        sales_df = get_sales_data(deal_id=deal_id, date_range=date_filter_for_sql)
        if not sales_df.empty:
            combined_sales_df = pd.concat([combined_sales_df, sales_df], ignore_index=True)

    if combined_sales_df.empty:
        st.info("No sales data found for the selected deals and date range.")
    else:
        st.subheader("Sales Overview")
        # Ensure sale_timestamp is datetime
        combined_sales_df["sale_timestamp"] = pd.to_datetime(combined_sales_df["sale_timestamp"])
        combined_sales_df["sale_date"] = combined_sales_df["sale_timestamp"].dt.date

        # Bar chart: Handset sales comparison (by product)
        sales_by_product = combined_sales_df.groupby(["product_name", "deal_name"])["quantity_sold"].sum().reset_index()
        if not sales_by_product.empty:
            fig_bar = px.bar(sales_by_product, x="product_name", y="quantity_sold", color="deal_name", 
                             title="Handset Sales by Product and Deal", barmode="group")
            st.plotly_chart(fig_bar, use_container_width=True)
        
        # Line chart: Handset sales by day
        sales_by_day = combined_sales_df.groupby(["sale_date", "deal_name"])["quantity_sold"].sum().reset_index()
        if not sales_by_day.empty:
            fig_line = px.line(sales_by_day, x="sale_date", y="quantity_sold", color="deal_name", 
                               title="Daily Handset Sales by Deal")
            st.plotly_chart(fig_line, use_container_width=True)

        # Pie chart: % target achieved (Placeholder - requires target data and logic)
        st.subheader("Target Achievement (Placeholder)")
        # This requires fetching target data from `dealer_targets` and comparing with sales.
        # For MVP, we can show a placeholder or a simple pie if targets are defined.
        # Example: if a deal has a target_quantity for a product, calculate % achieved.
        # For now, a simple pie chart of sales distribution by product for the first selected deal.
        if selected_deal_ids:
            first_deal_sales = combined_sales_df[combined_sales_df["deal_name"] == selected_deal_names[0]]
            if not first_deal_sales.empty:
                sales_dist_pie = first_deal_sales.groupby("product_name")["quantity_sold"].sum().reset_index()
                if not sales_dist_pie.empty:
                    fig_pie = px.pie(sales_dist_pie, values="quantity_sold", names="product_name", 
                                     title=f"Sales Distribution for {selected_deal_names[0]}")
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.info(f"No sales data to display pie chart for {selected_deal_names[0]}.")
            else:
                st.info(f"No sales data to display pie chart for {selected_deal_names[0]}.")

    st.subheader("Latest Uploaded Deal Status")
    latest_deal = deals_df.iloc[0] if not deals_df.empty else None
    if latest_deal is not None:
        st.write(f"**Latest Deal:** {latest_deal["deal_name"]}")
        st.write(f"**Status:** {latest_deal["deal_status"]}")
        st.write(f"**Period:** {latest_deal["scheme_period_start"]} to {latest_deal["scheme_period_end"]}")
    else:
        st.write("No deals uploaded yet.")

def render_file_uploader():
    st.title("Upload New Scheme PDF")

    # Schema description for Claude - this should be more detailed based on your actual schema
    # For now, using a simplified version. This should match the one in pdf_processor.py or be centralized.
    schema_desc_for_llm = """
    {
        "deal_name": "Name of the deal/scheme (e.g., Summer Bonanza)",
        "scheme_period_start": "Start date of the scheme (YYYY-MM-DD)",
        "scheme_period_end": "End date of the scheme (YYYY-MM-DD)",
        "applicable_region": "Geographical region (e.g., All India, North Zone)",
        "dealer_type_eligibility": "Eligible dealer types (e.g., All Dealers, SEZ Dealers)",
        "products_offers": [
            {
                "product_name": "Name of the product (e.g., Galaxy S23)",
                "product_code": "Product SKU or model code (e.g., SM-G990)",
                "product_category": "Category of product (e.g., Smart Phones, Tablets)",
                "support_type": "Type of support (e.g., Special Support, Cashback, Payout)",
                "payout_type": "How payout is calculated (Percentage, Fixed Amount)",
                "payout_value": "Value of payout (numeric, e.g., 10 for 10%, 500 for Rs 500)",
                "payout_unit": "Unit of payout (e.g., %, INR, Per Unit)",
                "target_metric": "Metric for target-based offer (e.g., GMCS Upload, Sellout Quantity)",
                "target_value_description": "Description of target value (e.g., Achieve 50 units)",
                "conditions_exclusions": "Specific conditions or exclusions for this offer",
                "is_dealer_incentive": "boolean, true if dealer incentive, false if customer discount"
            }
        ],
        "dealer_targets": [
            {
                "target_description": "Overall target description (e.g., Sell 100 premium phones)",
                "target_product_name": "Specific product for target (if any)",
                "target_product_group_description": "Product group for target",
                "target_quantity": "Target quantity (numeric)",
                "target_value": "Target value (numeric, e.g., sales amount)",
                "target_metric": "Metric for the target (e.g., Units Sold, Sales Value)"
            }
        ]
    }
    """

    deal_project_name = st.text_input("Enter Deal/Project Name (e.g., MyDeal-DD-MM-YYYY):", f"Deal-{datetime.date.today().strftime("%d-%m-%Y")}")
    uploaded_file = st.file_uploader("Choose a PDF file", type="pdf")

    if uploaded_file is not None and deal_project_name:
        st.write("Filename:", uploaded_file.name)
        
        # Save uploaded file temporarily to pass its path to processors
        upload_dir = "/home/ubuntu/dealer_app/uploads"
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)
        
        temp_pdf_path = os.path.join(upload_dir, uploaded_file.name)
        with open(temp_pdf_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.success(f"File {uploaded_file.name} uploaded successfully to {temp_pdf_path}")

        if st.button("Process PDF and Save Scheme"): 
            with st.spinner("Processing PDF..."):
                # 1. Extract text from PDF pages
                # The pdf_processor.extract_text_from_pdf_pages uses st.progress internally
                # but we are calling it from here, so it should work.
                extracted_pages_data = extract_text_from_pdf_pages(temp_pdf_path)
                
                if not extracted_pages_data:
                    st.error("Could not extract any text from the PDF.")
                    return

                full_extracted_text = "\n\n---\n\n".join([page["text"] for page in extracted_pages_data if page["text"]])
                
                # Save raw text for reference
                raw_text_dir = "/home/ubuntu/dealer_app/raw_texts"
                if not os.path.exists(raw_text_dir):
                    os.makedirs(raw_text_dir)
                raw_text_file_path = os.path.join(raw_text_dir, f"{deal_project_name.replace(" ", "_")}_raw.txt")
                with open(raw_text_file_path, "w", encoding="utf-8") as f_raw:
                    f_raw.write(full_extracted_text)
                st.info(f"Raw extracted text saved to {raw_text_file_path}")

            with st.spinner("Extracting structured data using LLM..."):
                # 2. Extract structured data using Claude
                structured_data = extract_structured_data_from_text(full_extracted_text, schema_desc_for_llm)

                if not structured_data:
                    st.error("Failed to extract structured data using LLM. Please check logs or try a different PDF.")
                    return
                
                st.success("Structured data extracted successfully!")
                st.json(structured_data) # Display extracted JSON

            with st.spinner("Saving data to database..."):
                # 3. Populate database
                # This is a simplified population logic. It needs to be robust.
                try:
                    conn = connect_db()
                    # Add to deals table
                    deal_id = add_deal(
                        deal_name=structured_data.get("deal_name", deal_project_name),
                        scheme_document_name=uploaded_file.name,
                        scheme_period_start=structured_data.get("scheme_period_start"),
                        scheme_period_end=structured_data.get("scheme_period_end"),
                        applicable_region=structured_data.get("applicable_region"),
                        dealer_type_eligibility=structured_data.get("dealer_type_eligibility"),
                        raw_extracted_text_path=raw_text_file_path,
                        deal_status="Active" # Default to Active, can be changed later
                    )
                    if not deal_id:
                        st.error("Failed to save main deal information. The deal name might already exist.")
                        # Attempt to fetch existing deal_id if name collision
                        cursor_check = conn.cursor()
                        cursor_check.execute("SELECT deal_id FROM deals WHERE deal_name = ?", (structured_data.get("deal_name", deal_project_name),))
                        existing = cursor_check.fetchone()
                        if existing:
                            deal_id = existing["deal_id"]
                            st.warning(f"Deal 	'{structured_data.get("deal_name", deal_project_name)}	' already exists with ID {deal_id}. Offers might be appended if not already present.")
                        else:
                            conn.close()
                            return
                    
                    # Add products and offers
                    for offer_item in structured_data.get("products_offers", []):
                        product_name = offer_item.get("product_name")
                        product_code = offer_item.get("product_code")
                        product_category = offer_item.get("product_category")
                        
                        product_id = None
                        if product_name:
                            product_id = add_product(product_code, product_name, product_category)
                        
                        if deal_id: # product_id can be None if it's a group offer
                            add_deal_product_offer(
                                deal_id=deal_id,
                                product_id=product_id,
                                product_group_description=offer_item.get("product_group_description") if not product_id else None,
                                support_type=offer_item.get("support_type"),
                                payout_type=offer_item.get("payout_type"),
                                payout_value=offer_item.get("payout_value"),
                                payout_unit=offer_item.get("payout_unit"),
                                target_metric=offer_item.get("target_metric"),
                                target_value_description=offer_item.get("target_value_description"),
                                conditions_exclusions=offer_item.get("conditions_exclusions"),
                                is_dealer_incentive=offer_item.get("is_dealer_incentive", False)
                            )
                    
                    # Add dealer targets (simplified)
                    for target_item in structured_data.get("dealer_targets", []):
                        # This part needs more robust product matching for target_product_id
                        cursor = conn.cursor()
                        target_product_id_db = None
                        if target_item.get("target_product_name"):
                            cursor.execute("SELECT product_id FROM products WHERE product_name = ?", (target_item["target_product_name"],))
                            res = cursor.fetchone()
                            if res: target_product_id_db = res["product_id"]

                        cursor.execute("""INSERT INTO dealer_targets 
                                        (deal_id, target_description, target_product_id, target_product_group_description, target_quantity, target_value, target_metric) 
                                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                       (deal_id, target_item.get("target_description"), target_product_id_db, target_item.get("target_product_group_description"), 
                                        target_item.get("target_quantity"), target_item.get("target_value"), target_item.get("target_metric")))
                    conn.commit()
                    st.success(f"Deal '{deal_project_name}' and its components saved to database successfully!")
                except Exception as e:
                    st.error(f"Error saving data to database: {e}")
                finally:
                    if conn:
                        conn.close()


def render_view_deals():
    st.title("View / Manage Deals")
    deals_df = get_all_deals()

    if deals_df.empty:
        st.info("No deals available. Please upload a scheme PDF first.")
        return

    selected_deal_name = st.selectbox("Select a Deal to View Details:", deals_df["deal_name"].unique())

    if selected_deal_name:
        deal_id = deals_df[deals_df["deal_name"] == selected_deal_name]["deal_id"].iloc[0]
        deal_info, offers_info, targets_info = get_deal_details(deal_id)

        st.subheader(f"Details for: {selected_deal_name}")
        if not deal_info.empty:
            st.write("**General Information:**")
            st.dataframe(deal_info, use_container_width=True)
        
        if not offers_info.empty:
            st.write("**Product Offers & Incentives:**")
            # For MVP, view-only. Editable dataframe is more complex.
            st.dataframe(offers_info, use_container_width=True)
        else:
            st.write("No specific product offers found for this deal.")

        if not targets_info.empty:
            st.write("**Dealer Targets:**")
            st.dataframe(targets_info, use_container_width=True)
        else:
            st.write("No specific dealer targets found for this deal.")
        
        # For MVP, editing is out of scope as per initial plan, but can be a future enhancement.
        # st.subheader("Edit Deal Data (Placeholder)")
        # st.info("Editing functionality will be available in a future version.")

def render_simulate_sale():
    st.title("Simulate Phone Sale (Billing API Mock)")

    products_df = get_all_products_for_sale()
    if products_df.empty:
        st.warning("No products found in the database. Please ensure products are added via scheme uploads.")
        return

    product_options = {f"{row['product_name']} (ID: {row['product_id']}, Code: {row['product_code']})": row['product_id'] for index, row in products_df.iterrows()}
    selected_product_display = st.selectbox("Select Product to Sell:", list(product_options.keys()))
    
    quantity = st.number_input("Quantity Sold:", min_value=1, value=1, step=1)
    sale_date = st.date_input("Sale Date (YYYY-MM-DD):", datetime.date.today())
    billing_ref = st.text_input("Billing System Reference ID (Optional):", f"MOCKBILL-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")

    if st.button("Simulate Sale") and selected_product_display:
        product_id_to_sell = product_options[selected_product_display]
        with st.spinner("Simulating sale and applying offers..."):
            sale_result = simulate_billing_api_call(product_id_to_sell, quantity, sale_date.isoformat(), billing_ref)
            
            if sale_result and sale_result.get("status") == "success":
                st.success("Sale Simulated Successfully!")
                st.json(sale_result)
            else:
                st.error(f"Sale simulation failed: {sale_result.get('message', 'Unknown error')}")
                if sale_result:
                    st.json(sale_result) # Show error details if any

if __name__ == "__main__":
    main()


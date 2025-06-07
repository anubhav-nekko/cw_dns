import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sqlite3
import json
import datetime
import uuid
import random
from functools import lru_cache

# Import functions from pdf_processor_fixed
from pdf_processor_fixed import (
    connect_db as get_db_connection, 
    extract_text_from_pdf, 
    extract_structured_data_from_text, 
    add_sample_data, 
    create_tables
)

# --- Configuration & Setup ---
st.set_page_config(layout="wide", page_title="Dealer Nudging System")

# --- Global State & Session Management ---
if "page" not in st.session_state:
    st.session_state.page = "dashboard"
if "current_scheme" not in st.session_state:
    st.session_state.current_scheme = None
if "uploaded_pdf" not in st.session_state:
    st.session_state.uploaded_pdf = None
if "extracted_text" not in st.session_state:
    st.session_state.extracted_text = None
if "structured_data" not in st.session_state:
    st.session_state.structured_data = None
if "simulation_results" not in st.session_state:
    st.session_state.simulation_results = None
if "show_simulation_results" not in st.session_state:
    st.session_state.show_simulation_results = False

# --- Utility Functions ---
def navigate_to(page_name):
    """Navigate to a different page in the application"""
    st.session_state.page = page_name
    st.rerun()

@lru_cache(maxsize=128)
def load_secrets():
    """Load secrets from secrets.json"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_path = os.path.join(current_dir, "secrets.json")
    try:
        with open(secrets_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("secrets.json not found. Please ensure the file exists.")
        return {}
    except json.JSONDecodeError:
        st.error("Error decoding secrets.json. Please check the file format.")
        return {}

def save_uploaded_pdf(uploaded_file):
    """Save uploaded PDF to the uploads directory"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    uploads_dir = os.path.join(current_dir, "uploads")
    os.makedirs(uploads_dir, exist_ok=True)
    
    # Generate a unique filename to avoid overwrites
    unique_filename = f"{uuid.uuid4().hex[:8]}_{uploaded_file.name}"
    pdf_path = os.path.join(uploads_dir, unique_filename)
    
    with open(pdf_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return pdf_path

# --- Database Interaction Functions ---
@lru_cache(maxsize=32)
def get_active_schemes():
    """Get all active and approved schemes from the database"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT * FROM schemes 
            WHERE deal_status = ? AND approval_status = ? 
            ORDER BY scheme_period_end DESC
            LIMIT 2
            """, (
                "Active", "Approved"
            ))
            return cursor.fetchall()
        finally:
            conn.close()
    return []

@lru_cache(maxsize=32)
def get_all_products():
    """Get all products from the database"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM products WHERE is_active = 1 ORDER BY product_name")
            return cursor.fetchall()
        finally:
            conn.close()
    return []

@lru_cache(maxsize=32)
def get_all_dealers():
    """Get all dealers from the database"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dealers WHERE is_active = 1 ORDER BY dealer_name")
            return cursor.fetchall()
        finally:
            conn.close()
    return []

@lru_cache(maxsize=128)
def get_scheme_products(scheme_id):
    """Get products associated with a specific scheme"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT p.*, sp.support_type, sp.payout_type, sp.payout_amount, 
                   sp.payout_unit, sp.dealer_contribution, sp.total_payout,
                   sp.is_bundle_offer, sp.bundle_price, sp.is_upgrade_offer,
                   sp.free_item_description
            FROM products p
            JOIN scheme_products sp ON p.product_id = sp.product_id
            WHERE sp.scheme_id = ? AND p.is_active = 1
            """, (scheme_id,))
            return cursor.fetchall()
        finally:
            conn.close()
    return []

@lru_cache(maxsize=128)
def get_scheme_details(scheme_id):
    """Get details for a specific scheme"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM schemes WHERE scheme_id = ?", (scheme_id,))
            return cursor.fetchone()
        finally:
            conn.close()
    return None

@lru_cache(maxsize=128)
def get_scheme_rules(scheme_id):
    """Get rules for a specific scheme"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM scheme_rules WHERE scheme_id = ?", (scheme_id,))
            return cursor.fetchall()
        finally:
            conn.close()
    return []

@lru_cache(maxsize=128)
def get_payout_slabs(scheme_product_id):
    """Get payout slabs for a specific scheme product"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM payout_slabs WHERE scheme_product_id = ?", (scheme_product_id,))
            return cursor.fetchall()
        finally:
            conn.close()
    return []

@lru_cache(maxsize=32)
def get_pending_approvals():
    """Get schemes pending approval"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT * FROM schemes 
            WHERE approval_status = ? 
            ORDER BY upload_timestamp DESC
            """, (
                "Pending",
            ))
            return cursor.fetchall()
        finally:
            conn.close()
    return []

@lru_cache(maxsize=128)
def get_sales_data(days=30):
    """Get sales data for the last N days"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=days)
            
            query = """
            SELECT 
                st.sale_id, 
                st.sale_timestamp, 
                d.dealer_name, 
                d.region, 
                p.product_name, 
                p.product_category, 
                s.scheme_name, 
                st.quantity_sold, 
                st.dealer_price_dp, 
                st.earned_dealer_incentive_amount, 
                st.verification_status
            FROM sales_transactions st
            JOIN dealers d ON st.dealer_id = d.dealer_id
            JOIN products p ON st.product_id = p.product_id
            JOIN schemes s ON st.scheme_id = s.scheme_id
            WHERE st.sale_timestamp BETWEEN ? AND ?
            ORDER BY st.sale_timestamp DESC
            """
            cursor.execute(query, (start_date.strftime("%Y-%m-%d %H:%M:%S"), end_date.strftime("%Y-%m-%d %H:%M:%S")))
            return cursor.fetchall()
        finally:
            conn.close()
    return []

def add_new_scheme_from_data(structured_data, pdf_path):
    """Add a new scheme to the database from structured data"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Normalize fields from pdf_processor_fixed
            from pdf_processor_fixed import normalize_field
            
            # Add scheme
            scheme_name = normalize_field(structured_data.get("scheme_name"), str, f"Scheme from {os.path.basename(pdf_path)}")
            scheme_type = normalize_field(structured_data.get("scheme_type"), str, "Special Support")
            scheme_period_start = normalize_field(structured_data.get("scheme_period_start"), str, "2023-01-01")
            scheme_period_end = normalize_field(structured_data.get("scheme_period_end"), str, "2023-12-31")
            applicable_region = normalize_field(structured_data.get("applicable_region"), str, "All India")
            dealer_type_eligibility = normalize_field(structured_data.get("dealer_type_eligibility"), str, "All Dealers")
            
            cursor.execute("""
            INSERT INTO schemes (
                scheme_name, scheme_type, scheme_period_start, scheme_period_end,
                applicable_region, dealer_type_eligibility, scheme_document_name,
                raw_extracted_text_path, deal_status, approval_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                scheme_name,
                scheme_type,
                scheme_period_start,
                scheme_period_end,
                applicable_region,
                dealer_type_eligibility,
                os.path.basename(pdf_path),
                pdf_path,  # Assuming raw_extracted_text_path is the PDF path for now
                "Active",
                "Pending"
            ))
            scheme_id = cursor.lastrowid
            
            # Add products
            for product_data in structured_data.get("products", []):
                product_name = normalize_field(product_data.get("product_name"), str, f"Product {uuid.uuid4().hex[:8]}")
                product_code = normalize_field(product_data.get("product_code"), str, f"CODE-{uuid.uuid4().hex[:8]}")
                product_category = normalize_field(product_data.get("product_category"), str, "Mobile")
                
                cursor.execute("SELECT product_id FROM products WHERE product_name = ? AND product_code = ?", (product_name, product_code))
                existing_product = cursor.fetchone()
                
                if existing_product:
                    product_id = existing_product["product_id"]
                else:
                    cursor.execute("""
                    INSERT INTO products (product_name, product_code, product_category) 
                    VALUES (?, ?, ?)
                    """, (product_name, product_code, product_category))
                    product_id = cursor.lastrowid
                
                # Add scheme product
                support_type = normalize_field(product_data.get("support_type"), str, scheme_type)
                payout_type = normalize_field(product_data.get("payout_type"), str, "Fixed")
                payout_amount = normalize_field(product_data.get("payout_amount"), float, 1000.0)
                payout_unit = normalize_field(product_data.get("payout_unit"), str, "INR")
                free_item_description = normalize_field(product_data.get("free_item_description"), str)
                
                cursor.execute("""
                INSERT INTO scheme_products (
                    scheme_id, product_id, support_type, payout_type, payout_amount, 
                    payout_unit, free_item_description
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    scheme_id, product_id, support_type, payout_type, payout_amount, 
                    payout_unit, free_item_description
                ))
            
            # Add rules
            for rule_data in structured_data.get("scheme_rules", []):
                rule_type = normalize_field(rule_data.get("rule_type"), str, "General")
                rule_description = normalize_field(rule_data.get("rule_description"), str, "No description")
                rule_value = normalize_field(rule_data.get("rule_value"), str)
                
                cursor.execute("""
                INSERT INTO scheme_rules (scheme_id, rule_type, rule_description, rule_value) 
                VALUES (?, ?, ?, ?)
                """, (scheme_id, rule_type, rule_description, rule_value))
            
            conn.commit()
            return scheme_id
        except Exception as e:
            conn.rollback()
            st.error(f"Error saving scheme to database: {e}")
            return None
        finally:
            conn.close()
    return None

def update_scheme_status(scheme_id, status, approved_by="Admin"):
    """Update the approval status of a scheme"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE schemes 
            SET approval_status = ?, approved_by = ?, approval_timestamp = CURRENT_TIMESTAMP
            WHERE scheme_id = ?
            """, (status, approved_by, scheme_id))
            conn.commit()
            return True
        except Exception as e:
            st.error(f"Error updating scheme status: {e}")
            return False
        finally:
            conn.close()
    return False

def add_simulated_sale(dealer_id, product_id, scheme_id, quantity, dealer_price, incentive):
    """Add a simulated sale to the database"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            imei = "SIM-" + ",".join(["".join([str(random.randint(0, 9)) for _ in range(15)]) for _ in range(quantity)])
            
            cursor.execute("""
            INSERT INTO sales_transactions (
                dealer_id, product_id, scheme_id, quantity_sold, 
                dealer_price_dp, earned_dealer_incentive_amount, 
                imei_serial, verification_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dealer_id, product_id, scheme_id, quantity, 
                dealer_price, incentive, imei, "Simulated"
            ))
            conn.commit()
            return True
        except Exception as e:
            st.error(f"Error adding simulated sale: {e}")
            return False
        finally:
            conn.close()
    return False

# --- UI Rendering Functions ---

# Custom CSS
def load_custom_css():
    """Load custom CSS for styling"""
    st.markdown("""
    <style>
        /* Main header style */
        .main-header {
            color: #1E90FF; /* DodgerBlue */
            text-align: center;
            padding-bottom: 20px;
            border-bottom: 2px solid #1E90FF;
        }
        /* Sub-header style */
        .sub-header {
            color: #4682B4; /* SteelBlue */
            margin-top: 20px;
            margin-bottom: 10px;
            border-bottom: 1px solid #ADD8E6; /* LightBlue */
            padding-bottom: 5px;
        }
        /* Card style for displaying data */
        .card {
            background-color: #F0F8FF; /* AliceBlue */
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 15px;
            box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
        }
        /* Status indicators */
        .status-approved {
            color: green;
            font-weight: bold;
        }
        .status-pending {
            color: orange;
            font-weight: bold;
        }
        .status-rejected {
            color: red;
            font_weight: bold;
        }
        /* Highlight free items */
        .free-item-highlight {
            background-color: #FFFFE0; /* LightYellow */
            color: #8B4513; /* SaddleBrown */
            padding: 5px;
            border-radius: 5px;
            font-weight: bold;
            display: inline-block;
            margin-top: 5px;
        }
    </style>
    """, unsafe_allow_html=True)

# Sidebar navigation
def render_sidebar():
    """Render the sidebar navigation"""
    st.sidebar.title("Dealer Nudging System")
    st.sidebar.markdown("---   ")
    
    # Navigation buttons
    if st.sidebar.button("Dashboard", key="nav_dashboard"):
        navigate_to("dashboard")
    if st.sidebar.button("View Schemes", key="nav_schemes"):
        navigate_to("schemes")
    if st.sidebar.button("Upload New Scheme", key="nav_upload"):
        navigate_to("upload")
    if st.sidebar.button("Scheme Approvals", key="nav_approvals"):
        navigate_to("approvals")
    if st.sidebar.button("Simulate Sales", key="nav_simulate"):
        navigate_to("simulate")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "Dealer Nudging System (DNS) helps dealers track and optimize incentives "
        "from OEM schemes. Upload scheme PDFs, simulate sales, and get insights."
    )

# Dashboard page
def render_dashboard():
    """Render the dashboard page"""
    st.markdown("<h1 class='main-header'>Dealer Nudging System Dashboard</h1>", unsafe_allow_html=True)
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Active Schemes", len(get_active_schemes()))
    with col2:
        st.metric("Products", len(get_all_products()))
    with col3:
        st.metric("Dealers", len(get_all_dealers()))
    with col4:
        pending_count = len(get_pending_approvals())
        st.metric("Pending Approvals", pending_count)
    
    # Sales data visualization
    st.markdown("<h2 class='sub-header'>Sales Performance</h2>", unsafe_allow_html=True)
    
    sales_data = get_sales_data(days=30)
    if sales_data:
        try:
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(sales_data)
            
            # Check if required columns exist
            required_columns = ['product_category', 'quantity_sold', 'earned_dealer_incentive_amount', 
                               'region', 'sale_timestamp', 'scheme_name']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                st.warning(f"Some required data columns are missing: {', '.join(missing_columns)}. "
                          "Please ensure your sales data is complete.")
                st.info("Try simulating some sales to populate the dashboard with data.")
                return
            
            if len(df) == 0:
                st.info("No sales data available for the selected period. Try simulating some sales first.")
                return
            
            # Sales by product category
            st.markdown("<h3>Sales by Product Category</h3>", unsafe_allow_html=True)
            category_sales = df.groupby('product_category').agg({
                'quantity_sold': 'sum',
                'earned_dealer_incentive_amount': 'sum'
            }).reset_index()
            
            fig1 = px.bar(
                category_sales, 
                x='product_category', 
                y='quantity_sold',
                color='earned_dealer_incentive_amount',
                labels={
                    'product_category': 'Product Category',
                    'quantity_sold': 'Units Sold',
                    'earned_dealer_incentive_amount': 'Incentive Amount'
                },
                title='Sales and Incentives by Product Category'
            )
            st.plotly_chart(fig1, use_container_width=True, key="plot_category_sales")
            
            # Sales by region
            st.markdown("<h3>Sales by Region</h3>", unsafe_allow_html=True)
            region_sales = df.groupby('region').agg({
                'quantity_sold': 'sum',
                'earned_dealer_incentive_amount': 'sum'
            }).reset_index()
            
            fig2 = px.pie(
                region_sales, 
                values='quantity_sold', 
                names='region',
                title='Sales Distribution by Region'
            )
            st.plotly_chart(fig2, use_container_width=True, key="plot_region_sales")
            
            # Sales trend
            st.markdown("<h3>Sales Trend</h3>", unsafe_allow_html=True)
            df['sale_date'] = pd.to_datetime(df['sale_timestamp']).dt.date
            daily_sales = df.groupby('sale_date').agg({
                'quantity_sold': 'sum',
                'earned_dealer_incentive_amount': 'sum'
            }).reset_index()
            
            fig3 = px.line(
                daily_sales, 
                x='sale_date', 
                y=['quantity_sold', 'earned_dealer_incentive_amount'],
                labels={
                    'sale_date': 'Date',
                    'value': 'Value',
                    'variable': 'Metric'
                },
                title='Daily Sales and Incentives'
            )
            st.plotly_chart(fig3, use_container_width=True, key="plot_sales_trend")
            
            # Scheme effectiveness
            st.markdown("<h3>Scheme Effectiveness</h3>", unsafe_allow_html=True)
            scheme_effectiveness = df.groupby('scheme_name').agg({
                'quantity_sold': 'sum',
                'earned_dealer_incentive_amount': 'sum'
            }).reset_index()
            
            # Avoid division by zero
            scheme_effectiveness['incentive_per_unit'] = scheme_effectiveness.apply(
                lambda x: x['earned_dealer_incentive_amount'] / x['quantity_sold'] 
                if x['quantity_sold'] > 0 else 0, axis=1
            )
            
            fig4 = px.bar(
                scheme_effectiveness, 
                x='scheme_name', 
                y='incentive_per_unit',
                color='quantity_sold',
                labels={
                    'scheme_name': 'Scheme',
                    'incentive_per_unit': 'Incentive per Unit',
                    'quantity_sold': 'Units Sold'
                },
                title='Scheme Effectiveness (Incentive per Unit)'
            )
            st.plotly_chart(fig4, use_container_width=True, key="plot_scheme_effectiveness")
        
        except Exception as e:
            st.error(f"Error rendering dashboard visualizations: {str(e)}")
            st.info("This could be due to incomplete or malformed sales data. Try simulating some sales first.")
    else:
        st.info("No sales data available for visualization. Try simulating some sales first.")
        
        # Show sample visualization with dummy data
        st.markdown("<h3>Sample Visualization (Demo Data)</h3>", unsafe_allow_html=True)
        
        # Sample product category data
        sample_categories = pd.DataFrame({
            'product_category': ['Smartphones', 'Tablets', 'Wearables', 'Accessories'],
            'quantity_sold': [120, 45, 78, 210],
            'earned_dealer_incentive_amount': [24000, 13500, 7800, 6300]
        })
        
        fig_sample = px.bar(
            sample_categories,
            x='product_category',
            y='quantity_sold',
            color='earned_dealer_incentive_amount',
            labels={
                'product_category': 'Product Category',
                'quantity_sold': 'Units Sold (Sample)',
                'earned_dealer_incentive_amount': 'Incentive Amount (Sample)'
            },
            title='Sample Sales Visualization (Demo Data)'
        )
        st.plotly_chart(fig_sample, use_container_width=True, key="plot_sample")

# Schemes page
def render_schemes():
    """Render the schemes page"""
    st.markdown("<h1 class='main-header'>Available Schemes</h1>", unsafe_allow_html=True)
    
    # Filter options
    st.markdown("<h2 class='sub-header'>Filter Schemes</h2>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    
    with col1:
        filter_region = st.selectbox(
            "Region",
            ["All"] + list(set(scheme['applicable_region'] for scheme in get_active_schemes())),
            key="filter_region"
        )
    
    with col2:
        filter_type = st.selectbox(
            "Scheme Type",
            ["All"] + list(set(scheme['scheme_type'] for scheme in get_active_schemes())),
            key="filter_type"
        )
    
    # Apply filters
    filtered_schemes = get_active_schemes()
    if filter_region != "All":
        filtered_schemes = [s for s in filtered_schemes if s['applicable_region'] == filter_region]
    if filter_type != "All":
        filtered_schemes = [s for s in filtered_schemes if s['scheme_type'] == filter_type]
    
    # Display schemes as cards
    for scheme in filtered_schemes:
        st.markdown("<div class=\"card\">", unsafe_allow_html=True)
        st.markdown(f"### {scheme['scheme_name']}")
        st.markdown(f"**Type:** {scheme['scheme_type']}")
        st.markdown(f"**Period:** {scheme['scheme_period_start']} to {scheme['scheme_period_end']}")
        st.markdown(f"**Region:** {scheme['applicable_region']}")
        st.markdown(f"**Dealer Eligibility:** {scheme['dealer_type_eligibility']}")
        
        if st.button("View Details", key=f"view_scheme_{scheme['scheme_id']}"):
            st.session_state.current_scheme = scheme["scheme_id"]
            navigate_to("scheme_details")
        
        st.markdown("</div>", unsafe_allow_html=True)
    
    if not filtered_schemes:
        st.info("No schemes match the selected filters. Try adjusting your filter criteria or upload new schemes.")

# Scheme details page
def render_scheme_details():
    """Render the scheme details page"""
    scheme_id = st.session_state.current_scheme
    scheme = get_scheme_details(scheme_id)
    
    if not scheme:
        st.error("Scheme not found.")
        return
    
    st.markdown(f"<h1 class='main-header'>{scheme['scheme_name']}</h1>", unsafe_allow_html=True)
    
    # Scheme details
    st.markdown("<h2 class='sub-header'>Scheme Details</h2>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**Type:** {scheme['scheme_type']}")
        st.markdown(f"**Period:** {scheme['scheme_period_start']} to {scheme['scheme_period_end']}")
        st.markdown(f"**Region:** {scheme['applicable_region']}")
    
    with col2:
        st.markdown(f"**Dealer Eligibility:** {scheme['dealer_type_eligibility']}")
        st.markdown(f"**Status:** {scheme['deal_status']}")
        st.markdown(f"**Approval Status:** {scheme['approval_status']}")
    
    # Products in scheme
    st.markdown("<h2 class='sub-header'>Products</h2>", unsafe_allow_html=True)
    products = get_scheme_products(scheme_id)
    
    for product in products:
        st.markdown("<div class=\"card\">", unsafe_allow_html=True)
        st.markdown(f"### {product['product_name']} ({product['product_code']})")
        st.markdown(f"**Category:** {product['product_category']}")
        st.markdown(f"**Support Type:** {product['support_type']}")
        st.markdown(f"**Payout:** {product['payout_amount']} {product['payout_unit']} ({product['payout_type']})")
        
        # Display free item if available
        if product['free_item_description']:
            st.markdown(f"<div class='free-item-highlight'>🎁 FREE: {product['free_item_description']}</div>", unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Scheme rules
    st.markdown("<h2 class='sub-header'>Rules</h2>", unsafe_allow_html=True)
    rules = get_scheme_rules(scheme_id)
    
    for rule in rules:
        st.markdown(f"**{rule['rule_type']}:** {rule['rule_description']}")
    
    # Back button
    if st.button("Back to Schemes"):
        navigate_to("schemes")

# Upload page
def render_upload():
    """Render the upload page"""
    st.markdown("<h1 class='main-header'>Upload New Scheme</h1>", unsafe_allow_html=True)
    
    # File upload
    uploaded_file = st.file_uploader("Upload Scheme PDF", type=["pdf"])
    
    if uploaded_file:
        st.session_state.uploaded_pdf = save_uploaded_pdf(uploaded_file)
        st.success(f"File uploaded: {uploaded_file.name}")
        
        # Extract text from PDF
        with st.spinner("Extracting text from PDF..."):
            extracted_text = extract_text_from_pdf(st.session_state.uploaded_pdf)
            st.session_state.extracted_text = extracted_text
        
        # Display extracted text
        st.markdown("<h2 class='sub-header'>Extracted Text</h2>", unsafe_allow_html=True)
        with st.expander("Show Extracted Text", expanded=False):
            for page_num, text in extracted_text:
                st.markdown(f"### Page {page_num}")
                st.text(text)
        
        # Extract structured data
        with st.spinner("Extracting structured data..."):
            structured_data = extract_structured_data_from_text(extracted_text)
            st.session_state.structured_data = structured_data
        
        # Display structured data
        st.markdown("<h2 class='sub-header'>Extracted Scheme Data</h2>", unsafe_allow_html=True)
        
        # Scheme details
        st.markdown("<h3>Scheme Details</h3>", unsafe_allow_html=True)
        scheme_name = structured_data.get("scheme_name", "Unknown Scheme")
        scheme_type = structured_data.get("scheme_type", "Unknown Type")
        scheme_period_start = structured_data.get("scheme_period_start", "Unknown")
        scheme_period_end = structured_data.get("scheme_period_end", "Unknown")
        applicable_region = structured_data.get("applicable_region", "Unknown")
        dealer_type_eligibility = structured_data.get("dealer_type_eligibility", "Unknown")
        
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Scheme Name", value=scheme_name, key="scheme_name")
            st.text_input("Scheme Type", value=scheme_type, key="scheme_type")
            st.text_input("Start Date", value=scheme_period_start, key="scheme_period_start")
        with col2:
            st.text_input("End Date", value=scheme_period_end, key="scheme_period_end")
            st.text_input("Region", value=applicable_region, key="applicable_region")
            st.text_input("Dealer Eligibility", value=dealer_type_eligibility, key="dealer_type_eligibility")
        
        # Products
        st.markdown("<h3>Products</h3>", unsafe_allow_html=True)
        products = structured_data.get("products", [])
        
        for i, product in enumerate(products):
            st.markdown(f"#### Product {i+1}")
            col1, col2 = st.columns(2)
            with col1:
                st.text_input("Product Name", value=product.get("product_name", ""), key=f"product_name_{i}")
                st.text_input("Product Code", value=product.get("product_code", ""), key=f"product_code_{i}")
                st.text_input("Category", value=product.get("product_category", ""), key=f"product_category_{i}")
            with col2:
                st.text_input("Support Type", value=product.get("support_type", ""), key=f"support_type_{i}")
                st.text_input("Payout Type", value=product.get("payout_type", ""), key=f"payout_type_{i}")
                st.text_input("Payout Amount", value=str(product.get("payout_amount", "")), key=f"payout_amount_{i}")
                st.text_input("Free Item", value=product.get("free_item_description", ""), key=f"free_item_{i}")
        
        # Rules
        st.markdown("<h3>Rules</h3>", unsafe_allow_html=True)
        rules = structured_data.get("scheme_rules", [])
        
        for i, rule in enumerate(rules):
            col1, col2 = st.columns(2)
            with col1:
                st.text_input("Rule Type", value=rule.get("rule_type", ""), key=f"rule_type_{i}")
            with col2:
                st.text_input("Rule Description", value=rule.get("rule_description", ""), key=f"rule_description_{i}")
        
        # Save to database
        if st.button("Save Scheme"):
            with st.spinner("Saving scheme to database..."):
                # Update structured data with edited values
                structured_data["scheme_name"] = st.session_state.scheme_name
                structured_data["scheme_type"] = st.session_state.scheme_type
                structured_data["scheme_period_start"] = st.session_state.scheme_period_start
                structured_data["scheme_period_end"] = st.session_state.scheme_period_end
                structured_data["applicable_region"] = st.session_state.applicable_region
                structured_data["dealer_type_eligibility"] = st.session_state.dealer_type_eligibility
                
                for i, product in enumerate(products):
                    product["product_name"] = st.session_state[f"product_name_{i}"]
                    product["product_code"] = st.session_state[f"product_code_{i}"]
                    product["product_category"] = st.session_state[f"product_category_{i}"]
                    product["support_type"] = st.session_state[f"support_type_{i}"]
                    product["payout_type"] = st.session_state[f"payout_type_{i}"]
                    product["payout_amount"] = float(st.session_state[f"payout_amount_{i}"])
                    product["free_item_description"] = st.session_state[f"free_item_{i}"]
                
                for i, rule in enumerate(rules):
                    rule["rule_type"] = st.session_state[f"rule_type_{i}"]
                    rule["rule_description"] = st.session_state[f"rule_description_{i}"]
                
                # Save to database
                scheme_id = add_new_scheme_from_data(structured_data, st.session_state.uploaded_pdf)
                
                if scheme_id:
                    st.success("Scheme saved successfully! Awaiting approval.")
                    # Clear session state
                    st.session_state.uploaded_pdf = None
                    st.session_state.extracted_text = None
                    st.session_state.structured_data = None
                    # Navigate to schemes page
                    navigate_to("schemes")
                else:
                    st.error("Failed to save scheme. Please try again.")

# Approvals page
def render_approvals():
    """Render the approvals page"""
    st.markdown("<h1 class='main-header'>Scheme Approvals</h1>", unsafe_allow_html=True)
    
    pending_schemes = get_pending_approvals()
    
    if not pending_schemes:
        st.info("No schemes pending approval.")
        return
    
    for scheme in pending_schemes:
        st.markdown("<div class=\"card\">", unsafe_allow_html=True)
        st.markdown(f"### {scheme['scheme_name']}")
        st.markdown(f"**Type:** {scheme['scheme_type']}")
        st.markdown(f"**Period:** {scheme['scheme_period_start']} to {scheme['scheme_period_end']}")
        st.markdown(f"**Region:** {scheme['applicable_region']}")
        st.markdown(f"**Dealer Eligibility:** {scheme['dealer_type_eligibility']}")
        st.markdown(f"**Uploaded:** {scheme['upload_timestamp']}")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Approve", key=f"approve_{scheme['scheme_id']}"):
                if update_scheme_status(scheme['scheme_id'], "Approved"):
                    st.success("Scheme approved!")
                    # Clear cache to refresh data
                    get_active_schemes.cache_clear()
                    get_pending_approvals.cache_clear()
                    # Rerun to refresh page
                    st.rerun()
        with col2:
            if st.button("Reject", key=f"reject_{scheme['scheme_id']}"):
                if update_scheme_status(scheme['scheme_id'], "Rejected"):
                    st.success("Scheme rejected!")
                    # Clear cache to refresh data
                    get_active_schemes.cache_clear()
                    get_pending_approvals.cache_clear()
                    # Rerun to refresh page
                    st.rerun()
        
        st.markdown("</div>", unsafe_allow_html=True)

# Simulate sales page
def render_simulate_sales():
    """Render the simulate sales page"""
    st.markdown("<h1 class='main-header'>Simulate Sales</h1>", unsafe_allow_html=True)
    
    # Form for simulation
    st.markdown("<h2 class='sub-header'>Enter Sale Details</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Select dealer
        dealers = get_all_dealers()
        dealer_options = {dealer["dealer_id"]: dealer["dealer_name"] for dealer in dealers}
        dealer_id = st.selectbox("Select Dealer", options=list(dealer_options.keys()), format_func=lambda x: dealer_options[x])
        
        # Select scheme
        schemes = get_active_schemes()
        scheme_options = {scheme["scheme_id"]: scheme["scheme_name"] for scheme in schemes}
        scheme_id = st.selectbox("Select Scheme", options=list(scheme_options.keys()), format_func=lambda x: scheme_options[x])
    
    with col2:
        # Select product based on scheme
        products = get_scheme_products(scheme_id) if scheme_id else []
        product_options = {product["product_id"]: product["product_name"] for product in products}
        product_id = st.selectbox("Select Product", options=list(product_options.keys()), format_func=lambda x: product_options[x]) if products else None
        
        # Quantity
        quantity = st.number_input("Quantity", min_value=1, value=1)
    
    # Calculate dealer price and incentive
    dealer_price = None
    incentive = None
    free_item = None
    
    if product_id and scheme_id:
        # Find the selected product in the scheme products
        selected_product = next((p for p in products if p["product_id"] == product_id), None)
        
        if selected_product:
            # Calculate dealer price (simplified for simulation)
            dealer_price = selected_product.get("dealer_price", 10000)  # Default value if not specified
            
            # Calculate incentive based on payout type
            payout_type = selected_product["payout_type"]
            payout_amount = selected_product["payout_amount"]
            
            if payout_type == "Fixed":
                incentive = payout_amount * quantity
            elif payout_type == "Percentage":
                incentive = (dealer_price * payout_amount / 100) * quantity
            else:
                incentive = payout_amount * quantity
            
            # Check for free item
            free_item = selected_product.get("free_item_description")
    
    # Display calculated values
    if dealer_price is not None and incentive is not None:
        st.markdown("<h2 class='sub-header'>Calculation</h2>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Dealer Price", f"₹{dealer_price:,.2f}")
        with col2:
            st.metric("Total Value", f"₹{(dealer_price * quantity):,.2f}")
        with col3:
            st.metric("Incentive", f"₹{incentive:,.2f}")
        
        # Display free item if available
        if free_item:
            st.markdown(f"<div class='free-item-highlight'>🎁 FREE with this purchase: {free_item}</div>", unsafe_allow_html=True)
            st.markdown("**Remember to inform the customer about this free item!**")
        
        # Simulate button
        if st.button("Simulate Sale"):
            if add_simulated_sale(dealer_id, product_id, scheme_id, quantity, dealer_price, incentive):
                # Store simulation results
                st.session_state.simulation_results = {
                    "dealer_name": dealer_options[dealer_id],
                    "scheme_name": scheme_options[scheme_id],
                    "product_name": product_options[product_id],
                    "quantity": quantity,
                    "dealer_price": dealer_price,
                    "total_value": dealer_price * quantity,
                    "incentive": incentive,
                    "free_item": free_item
                }
                st.session_state.show_simulation_results = True
                
                # Clear cache to refresh data
                get_sales_data.cache_clear()
                
                # Show success message
                st.success("Sale simulated successfully!")
                
                # Show simulation results
                st.markdown("<h2 class='sub-header'>Simulation Results</h2>", unsafe_allow_html=True)
                st.markdown("<div class=\"card\">", unsafe_allow_html=True)
                st.markdown(f"### Sale to {st.session_state.simulation_results['dealer_name']}")
                st.markdown(f"**Product:** {st.session_state.simulation_results['product_name']}")
                st.markdown(f"**Quantity:** {st.session_state.simulation_results['quantity']}")
                st.markdown(f"**Total Value:** ₹{st.session_state.simulation_results['total_value']:,.2f}")
                st.markdown(f"**Incentive Earned:** ₹{st.session_state.simulation_results['incentive']:,.2f}")
                
                if st.session_state.simulation_results['free_item']:
                    st.markdown(f"<div class='free-item-highlight'>🎁 FREE: {st.session_state.simulation_results['free_item']}</div>", unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)
                
                # Customer prompt for free item
                if st.session_state.simulation_results['free_item']:
                    st.markdown("### Customer Prompt")
                    st.markdown(f"""
                    <div style="background-color: #E8F4F8; padding: 15px; border-radius: 10px; border-left: 5px solid #1E90FF;">
                        <p style="font-size: 16px;">
                            <strong>Say to customer:</strong><br>
                            "Great choice! I'm happy to let you know that with your purchase of {st.session_state.simulation_results['product_name']}, 
                            you'll also receive <strong>{st.session_state.simulation_results['free_item']}</strong> absolutely free! 
                            This is part of our current promotion."
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.error("Failed to simulate sale. Please try again.")

# Main function
def main():
    """Main function to run the Streamlit app"""
    load_custom_css()
    render_sidebar()
    
    # Render the appropriate page based on session state
    if st.session_state.page == "dashboard":
        render_dashboard()
    elif st.session_state.page == "schemes":
        render_schemes()
    elif st.session_state.page == "scheme_details":
        render_scheme_details()
    elif st.session_state.page == "upload":
        render_upload()
    elif st.session_state.page == "approvals":
        render_approvals()
    elif st.session_state.page == "simulate":
        render_simulate_sales()

if __name__ == "__main__":
    main()

import streamlit as st
import pandas as pd
import sqlite3
import os
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import random
import tempfile
import uuid
from pdf_processor_fixed import connect_db, extract_text_from_pdf, extract_structured_data_from_text

# Set page configuration
st.set_page_config(
    page_title="Dealer Nudging System (DNS)",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.8rem;
        color: #0D47A1;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .card {
        background-color: #f9f9f9;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    .metric-card {
        background-color: #e3f2fd;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        text-align: center;
        margin-bottom: 15px;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1565C0;
    }
    .metric-label {
        font-size: 1rem;
        color: #424242;
    }
    .highlight {
        background-color: #ffecb3;
        padding: 2px 5px;
        border-radius: 3px;
    }
    .status-approved {
        color: #2e7d32;
        font-weight: bold;
    }
    .status-pending {
        color: #f57c00;
        font-weight: bold;
    }
    .status-rejected {
        color: #d32f2f;
        font-weight: bold;
    }
    .footer {
        text-align: center;
        margin-top: 3rem;
        color: #757575;
        font-size: 0.8rem;
    }
    /* Responsive adjustments */
    @media (max-width: 768px) {
        .main-header {
            font-size: 2rem;
        }
        .sub-header {
            font-size: 1.5rem;
        }
        .metric-value {
            font-size: 1.5rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'page' not in st.session_state:
    st.session_state.page = 'dashboard'
if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False
if 'current_scheme' not in st.session_state:
    st.session_state.current_scheme = None
if 'approval_status' not in st.session_state:
    st.session_state.approval_status = {}
if 'simulation_results' not in st.session_state:
    st.session_state.simulation_results = None
if 'show_simulation_results' not in st.session_state:
    st.session_state.show_simulation_results = False
if 'uploaded_pdf' not in st.session_state:
    st.session_state.uploaded_pdf = None
if 'extracted_text' not in st.session_state:
    st.session_state.extracted_text = None
if 'structured_data' not in st.session_state:
    st.session_state.structured_data = None

# Helper functions
def load_secrets():
    """Load secrets from JSON file"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        secrets_path = os.path.join(current_dir, 'secrets.json')
        with open(secrets_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Error loading secrets: {e}")
        return {}

def get_db_connection():
    """Get a connection to the SQLite database"""
    try:
        conn = connect_db()
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

def get_active_schemes():
    """Get all active schemes from the database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT scheme_id, scheme_name, scheme_type, scheme_period_start, 
               scheme_period_end, applicable_region, dealer_type_eligibility,
               approval_status
        FROM schemes
        WHERE deal_status = 'Active'
        ORDER BY upload_timestamp DESC
        """)
        schemes = cursor.fetchall()
        return schemes
    except Exception as e:
        st.error(f"Error fetching schemes: {e}")
        return []
    finally:
        conn.close()

def get_scheme_details(scheme_id):
    """Get detailed information about a specific scheme"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT * FROM schemes WHERE scheme_id = ?
        """, (scheme_id,))
        scheme = cursor.fetchone()
        
        # Get products in this scheme
        cursor.execute("""
        SELECT p.product_id, p.product_name, p.product_category, p.product_subcategory,
               p.ram, p.storage, p.dealer_price_dp, p.mrp,
               sp.support_type, sp.payout_type, sp.payout_amount, sp.payout_unit,
               sp.dealer_contribution, sp.total_payout, sp.is_dealer_incentive,
               sp.is_bundle_offer, sp.bundle_price, sp.is_upgrade_offer, sp.is_slab_based
        FROM scheme_products sp
        JOIN products p ON sp.product_id = p.product_id
        WHERE sp.scheme_id = ?
        """, (scheme_id,))
        products = cursor.fetchall()
        
        # Get rules for this scheme
        cursor.execute("""
        SELECT rule_type, rule_description, rule_value
        FROM scheme_rules
        WHERE scheme_id = ?
        """, (scheme_id,))
        rules = cursor.fetchall()
        
        # Get parameters for this scheme
        cursor.execute("""
        SELECT parameter_name, parameter_description, parameter_criteria
        FROM scheme_parameters
        WHERE scheme_id = ?
        """, (scheme_id,))
        parameters = cursor.fetchall()
        
        # Get bundle offers for this scheme
        cursor.execute("""
        SELECT bo.bundle_id, p1.product_name as primary_product, 
               p2.product_name as bundle_product, bo.bundle_price
        FROM bundle_offers bo
        JOIN products p1 ON bo.primary_product_id = p1.product_id
        JOIN products p2 ON bo.bundle_product_id = p2.product_id
        WHERE bo.scheme_id = ?
        """, (scheme_id,))
        bundle_offers = cursor.fetchall()
        
        # Get payout slabs for products in this scheme
        cursor.execute("""
        SELECT ps.slab_id, sp.id as scheme_product_id, p.product_name,
               ps.min_quantity, ps.max_quantity, ps.payout_amount,
               ps.dealer_contribution, ps.total_payout
        FROM payout_slabs ps
        JOIN scheme_products sp ON ps.scheme_product_id = sp.id
        JOIN products p ON sp.product_id = p.product_id
        WHERE sp.scheme_id = ?
        """, (scheme_id,))
        payout_slabs = cursor.fetchall()
        
        return {
            'scheme': scheme,
            'products': products,
            'rules': rules,
            'parameters': parameters,
            'bundle_offers': bundle_offers,
            'payout_slabs': payout_slabs
        }
    except Exception as e:
        st.error(f"Error fetching scheme details: {e}")
        return None
    finally:
        conn.close()

def get_all_products():
    """Get all products from the database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT product_id, product_name, product_code, product_category, 
               product_subcategory, ram, storage, dealer_price_dp, mrp
        FROM products
        WHERE is_active = 1
        ORDER BY product_category, product_subcategory, product_name
        """)
        products = cursor.fetchall()
        return products
    except Exception as e:
        st.error(f"Error fetching products: {e}")
        return []
    finally:
        conn.close()

def get_all_dealers():
    """Get all dealers from the database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT dealer_id, dealer_name, dealer_code, dealer_type, 
               region, state, city
        FROM dealers
        WHERE is_active = 1
        ORDER BY region, state, city, dealer_name
        """)
        dealers = cursor.fetchall()
        return dealers
    except Exception as e:
        st.error(f"Error fetching dealers: {e}")
        return []
    finally:
        conn.close()

def get_sales_data(days=30):
    """Get sales data for the specified number of days"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT st.sale_id, d.dealer_name, d.region, p.product_name, 
               p.product_category, p.product_subcategory,
               st.quantity_sold, st.dealer_price_dp, 
               st.earned_dealer_incentive_amount, st.sale_timestamp,
               s.scheme_name, st.verification_status
        FROM sales_transactions st
        JOIN dealers d ON st.dealer_id = d.dealer_id
        JOIN products p ON st.product_id = p.product_id
        LEFT JOIN schemes s ON st.scheme_id = s.scheme_id
        WHERE st.sale_timestamp >= date('now', ?)
        ORDER BY st.sale_timestamp DESC
        """, (f'-{days} days',))
        sales = cursor.fetchall()
        return sales
    except Exception as e:
        st.error(f"Error fetching sales data: {e}")
        return []
    finally:
        conn.close()

def get_product_by_id(product_id):
    """Get product details by ID"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT * FROM products WHERE product_id = ?
        """, (product_id,))
        product = cursor.fetchone()
        return product
    except Exception as e:
        st.error(f"Error fetching product: {e}")
        return None
    finally:
        conn.close()

def get_dealer_by_id(dealer_id):
    """Get dealer details by ID"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT * FROM dealers WHERE dealer_id = ?
        """, (dealer_id,))
        dealer = cursor.fetchone()
        return dealer
    except Exception as e:
        st.error(f"Error fetching dealer: {e}")
        return None
    finally:
        conn.close()

def get_scheme_products(scheme_id):
    """Get products associated with a scheme"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT p.product_id, p.product_name, p.product_category, 
               sp.payout_amount, sp.dealer_contribution, sp.total_payout,
               p.dealer_price_dp, p.mrp
        FROM scheme_products sp
        JOIN products p ON sp.product_id = p.product_id
        WHERE sp.scheme_id = ?
        """, (scheme_id,))
        products = cursor.fetchall()
        return products
    except Exception as e:
        st.error(f"Error fetching scheme products: {e}")
        return []
    finally:
        conn.close()

def update_scheme_approval(scheme_id, approval_status, approved_by=None, notes=None):
    """Update the approval status of a scheme"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        UPDATE schemes
        SET approval_status = ?, 
            approved_by = ?,
            approval_timestamp = CURRENT_TIMESTAMP,
            notes = ?
        WHERE scheme_id = ?
        """, (approval_status, approved_by, notes, scheme_id))
        
        # Also add to approvals table
        cursor.execute("""
        INSERT INTO scheme_approvals (
            scheme_id, requested_by, approval_status, 
            approved_by, approved_at, approval_notes
        ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """, (scheme_id, "System", approval_status, approved_by, notes))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Error updating scheme approval: {e}")
        return False
    finally:
        conn.close()

def add_simulated_sale(dealer_id, product_id, scheme_id, quantity, dealer_price, incentive_amount):
    """Add a simulated sale to the database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        # Generate a random IMEI
        imei = ''.join([str(random.randint(0, 9)) for _ in range(15)])
        
        cursor.execute("""
        INSERT INTO sales_transactions (
            dealer_id, scheme_id, product_id, quantity_sold,
            dealer_price_dp, earned_dealer_incentive_amount, 
            imei_serial, verification_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Simulated')
        """, (dealer_id, scheme_id, product_id, quantity, dealer_price, incentive_amount, imei))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Error adding simulated sale: {e}")
        return False
    finally:
        conn.close()

def get_pending_approvals():
    """Get schemes pending approval"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT scheme_id, scheme_name, scheme_type, scheme_period_start, 
               scheme_period_end, applicable_region, dealer_type_eligibility,
               upload_timestamp
        FROM schemes
        WHERE approval_status = 'Pending'
        ORDER BY upload_timestamp DESC
        """)
        schemes = cursor.fetchall()
        return schemes
    except Exception as e:
        st.error(f"Error fetching pending approvals: {e}")
        return []
    finally:
        conn.close()

def save_uploaded_pdf(uploaded_file):
    """Save an uploaded PDF file to the uploads directory"""
    try:
        # Create uploads directory if it doesn't exist
        current_dir = os.path.dirname(os.path.abspath(__file__))
        uploads_dir = os.path.join(current_dir, "uploads")
        os.makedirs(uploads_dir, exist_ok=True)
        
        # Generate a unique filename
        file_extension = os.path.splitext(uploaded_file.name)[1]
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        file_path = os.path.join(uploads_dir, unique_filename)
        
        # Save the file
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        return file_path
    except Exception as e:
        st.error(f"Error saving uploaded file: {e}")
        return None

def add_new_scheme_from_data(structured_data, pdf_path):
    """Add a new scheme to the database from structured data"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Add scheme
        cursor.execute("""
        INSERT INTO schemes (
            scheme_name, scheme_type, scheme_period_start, scheme_period_end,
            applicable_region, dealer_type_eligibility, scheme_document_name,
            raw_extracted_text_path, deal_status, approval_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Active', 'Pending')
        """, (
            structured_data.get('scheme_name'),
            structured_data.get('scheme_type'),
            structured_data.get('scheme_period_start'),
            structured_data.get('scheme_period_end'),
            structured_data.get('applicable_region'),
            structured_data.get('dealer_type_eligibility'),
            os.path.basename(pdf_path),
            None  # No raw text path for now
        ))
        
        scheme_id = cursor.lastrowid
        
        # Add products
        for product in structured_data.get('products', []):
            # First add or get product
            cursor.execute("""
            SELECT product_id FROM products 
            WHERE product_name = ? AND product_code = ?
            """, (product.get('product_name'), product.get('product_code')))
            
            existing = cursor.fetchone()
            if existing:
                product_id = existing[0]
            else:
                cursor.execute("""
                INSERT INTO products (
                    product_name, product_code, product_category, product_subcategory,
                    ram, storage, connectivity, dealer_price_dp, mrp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    product.get('product_name'),
                    product.get('product_code'),
                    product.get('product_category'),
                    product.get('product_subcategory'),
                    product.get('ram'),
                    product.get('storage'),
                    product.get('connectivity'),
                    product.get('dealer_price_dp', random.randint(10000, 100000)),
                    product.get('mrp', random.randint(15000, 120000))
                ))
                product_id = cursor.lastrowid
            
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
                product.get('support_type'),
                product.get('payout_type'),
                product.get('payout_amount'),
                product.get('payout_unit'),
                product.get('dealer_contribution', 0),
                product.get('total_payout'),
                product.get('is_dealer_incentive', True),
                product.get('is_bundle_offer', False),
                product.get('bundle_price'),
                product.get('is_upgrade_offer', False),
                product.get('is_slab_based', False)
            ))
        
        # Add rules
        for rule in structured_data.get('scheme_rules', []):
            cursor.execute("""
            INSERT INTO scheme_rules (
                scheme_id, rule_type, rule_description, rule_value
            ) VALUES (?, ?, ?, ?)
            """, (
                scheme_id,
                rule.get('rule_type'),
                rule.get('rule_description'),
                rule.get('rule_value')
            ))
        
        conn.commit()
        return scheme_id
    except Exception as e:
        conn.rollback()
        st.error(f"Error adding new scheme: {e}")
        return None
    finally:
        conn.close()

def update_scheme_product(scheme_id, product_id, payout_amount, dealer_contribution, total_payout):
    """Update a scheme product's payout details"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        UPDATE scheme_products
        SET payout_amount = ?,
            dealer_contribution = ?,
            total_payout = ?,
            last_modified = CURRENT_TIMESTAMP
        WHERE scheme_id = ? AND product_id = ?
        """, (payout_amount, dealer_contribution, total_payout, scheme_id, product_id))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Error updating scheme product: {e}")
        return False
    finally:
        conn.close()

# Navigation functions
def navigate_to(page):
    """Navigate to a specific page"""
    st.session_state.page = page
    # Reset any page-specific state
    if page != 'scheme_details' and 'current_scheme' in st.session_state:
        st.session_state.current_scheme = None
    if page != 'simulate_sales':
        st.session_state.simulation_results = None
        st.session_state.show_simulation_results = False
    if page != 'upload_scheme':
        st.session_state.uploaded_pdf = None
        st.session_state.extracted_text = None
        st.session_state.structured_data = None

# Dashboard components
def render_dashboard():
    """Render the main dashboard"""
    st.markdown('<h1 class="main-header">Dealer Nudging System Dashboard</h1>', unsafe_allow_html=True)
    
    # Get data for dashboard
    schemes = get_active_schemes()
    products = get_all_products()
    dealers = get_all_dealers()
    sales_data = get_sales_data(30)  # Last 30 days
    
    # Convert to pandas DataFrames for easier manipulation
    schemes_df = pd.DataFrame([dict(s) for s in schemes]) if schemes else pd.DataFrame()
    products_df = pd.DataFrame([dict(p) for p in products]) if products else pd.DataFrame()
    dealers_df = pd.DataFrame([dict(d) for d in dealers]) if dealers else pd.DataFrame()
    sales_df = pd.DataFrame([dict(s) for s in sales_data]) if sales_data else pd.DataFrame()
    
    # Top metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{len(schemes) if schemes else 0}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Active Schemes</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{len(products) if products else 0}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Products</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{len(dealers) if dealers else 0}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Dealers</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        total_sales = sales_df['quantity_sold'].sum() if not sales_df.empty else 0
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{total_sales}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Units Sold (30d)</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Sales trend chart
    st.markdown('<h2 class="sub-header">Sales Trend</h2>', unsafe_allow_html=True)
    
    if not sales_df.empty and 'sale_timestamp' in sales_df.columns:
        # Convert timestamp to datetime
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_timestamp']).dt.date
        
        # Group by date and sum quantities
        daily_sales = sales_df.groupby('sale_date')['quantity_sold'].sum().reset_index()
        
        # Create a date range for the last 30 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=29)
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        date_df = pd.DataFrame({'sale_date': date_range.date})
        
        # Merge with daily sales to fill in missing dates
        complete_daily_sales = pd.merge(date_df, daily_sales, on='sale_date', how='left').fillna(0)
        
        # Create the line chart
        fig = px.line(
            complete_daily_sales, 
            x='sale_date', 
            y='quantity_sold',
            title='Daily Sales (Last 30 Days)',
            labels={'sale_date': 'Date', 'quantity_sold': 'Units Sold'},
            markers=True
        )
        fig.update_layout(
            xaxis_title='Date',
            yaxis_title='Units Sold',
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True, key="sales_trend_chart")
    else:
        st.info("No sales data available for the last 30 days.")
    
    # Product performance heatmap
    st.markdown('<h2 class="sub-header">Product Performance Heatmap</h2>', unsafe_allow_html=True)
    
    if not sales_df.empty and 'product_category' in sales_df.columns and 'product_subcategory' in sales_df.columns:
        # Group by category and subcategory
        category_sales = sales_df.groupby(['product_category', 'product_subcategory'])['quantity_sold'].sum().reset_index()
        
        # Pivot for heatmap
        pivot_df = category_sales.pivot_table(
            values='quantity_sold', 
            index='product_category', 
            columns='product_subcategory',
            fill_value=0
        )
        
        # Create heatmap
        fig = px.imshow(
            pivot_df,
            labels=dict(x="Product Subcategory", y="Product Category", color="Units Sold"),
            x=pivot_df.columns,
            y=pivot_df.index,
            color_continuous_scale='Viridis',
            aspect="auto"
        )
        fig.update_layout(
            title='Product Category Performance',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True, key="product_heatmap")
    else:
        st.info("Insufficient sales data for product performance heatmap.")
    
    # Regional performance
    st.markdown('<h2 class="sub-header">Regional Performance</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not sales_df.empty and 'region' in sales_df.columns:
            # Group by region
            region_sales = sales_df.groupby('region')['quantity_sold'].sum().reset_index()
            
            # Create pie chart
            fig = px.pie(
                region_sales, 
                values='quantity_sold', 
                names='region',
                title='Sales by Region',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True, key="region_pie")
        else:
            st.info("No regional sales data available.")
    
    with col2:
        if not sales_df.empty and 'dealer_name' in sales_df.columns:
            # Group by dealer
            dealer_sales = sales_df.groupby('dealer_name')['quantity_sold'].sum().reset_index()
            dealer_sales = dealer_sales.sort_values('quantity_sold', ascending=False).head(10)
            
            # Create bar chart
            fig = px.bar(
                dealer_sales, 
                x='dealer_name', 
                y='quantity_sold',
                title='Top 10 Dealers by Sales Volume',
                labels={'dealer_name': 'Dealer', 'quantity_sold': 'Units Sold'},
                color='quantity_sold',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(
                xaxis_title='Dealer',
                yaxis_title='Units Sold',
                xaxis_tickangle=-45,
                height=400
            )
            st.plotly_chart(fig, use_container_width=True, key="dealer_bar")
        else:
            st.info("No dealer sales data available.")
    
    # Scheme effectiveness
    st.markdown('<h2 class="sub-header">Scheme Effectiveness</h2>', unsafe_allow_html=True)
    
    if not sales_df.empty and 'scheme_name' in sales_df.columns and not all(pd.isna(sales_df['scheme_name'])):
        # Group by scheme
        scheme_sales = sales_df.groupby('scheme_name')['quantity_sold'].sum().reset_index()
        scheme_sales = scheme_sales.sort_values('quantity_sold', ascending=False)
        
        # Create horizontal bar chart
        fig = px.bar(
            scheme_sales, 
            y='scheme_name', 
            x='quantity_sold',
            title='Scheme Performance by Sales Volume',
            labels={'scheme_name': 'Scheme', 'quantity_sold': 'Units Sold'},
            orientation='h',
            color='quantity_sold',
            color_continuous_scale='Viridis'
        )
        fig.update_layout(
            yaxis_title='Scheme',
            xaxis_title='Units Sold',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True, key="scheme_bar")
    else:
        st.info("No scheme effectiveness data available.")
    
    # Recent activity
    st.markdown('<h2 class="sub-header">Recent Activity</h2>', unsafe_allow_html=True)
    
    if not sales_df.empty:
        # Display recent sales
        recent_sales = sales_df.head(5)
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Recent Sales")
        
        for _, sale in recent_sales.iterrows():
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                st.write(f"**{sale['product_name']}**")
                st.write(f"Dealer: {sale['dealer_name']}")
            with col2:
                st.write(f"Quantity: {sale['quantity_sold']}")
                st.write(f"Scheme: {sale['scheme_name'] if not pd.isna(sale['scheme_name']) else 'No Scheme'}")
            with col3:
                st.write(f"Date: {pd.to_datetime(sale['sale_timestamp']).date()}")
                status_class = "status-approved" if sale['verification_status'] == 'Verified' else "status-pending"
                st.markdown(f"<span class='{status_class}'>{sale['verification_status']}</span>", unsafe_allow_html=True)
            st.markdown("---")
        
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("No recent sales activity.")
    
    # Footer
    st.markdown('<div class="footer">Dealer Nudging System (DNS) - Powered by Streamlit</div>', unsafe_allow_html=True)

# Scheme Explorer components
def render_scheme_explorer():
    """Render the scheme explorer page"""
    st.markdown('<h1 class="main-header">Scheme Explorer</h1>', unsafe_allow_html=True)
    
    # Get all schemes
    schemes = get_active_schemes()
    
    if not schemes:
        st.info("No active schemes found.")
        return
    
    # Convert to DataFrame for filtering
    schemes_df = pd.DataFrame([dict(s) for s in schemes])
    
    # Filters
    st.markdown('<h2 class="sub-header">Filters</h2>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'scheme_type' in schemes_df.columns:
            scheme_types = ['All'] + sorted(schemes_df['scheme_type'].unique().tolist())
            selected_type = st.selectbox("Scheme Type", scheme_types)
    
    with col2:
        if 'applicable_region' in schemes_df.columns:
            regions = ['All'] + sorted(schemes_df['applicable_region'].unique().tolist())
            selected_region = st.selectbox("Region", regions)
    
    with col3:
        if 'dealer_type_eligibility' in schemes_df.columns:
            dealer_types = ['All'] + sorted(schemes_df['dealer_type_eligibility'].unique().tolist())
            selected_dealer_type = st.selectbox("Dealer Type", dealer_types)
    
    # Apply filters
    filtered_df = schemes_df.copy()
    
    if selected_type != 'All':
        filtered_df = filtered_df[filtered_df['scheme_type'] == selected_type]
    
    if selected_region != 'All':
        filtered_df = filtered_df[filtered_df['applicable_region'] == selected_region]
    
    if selected_dealer_type != 'All':
        filtered_df = filtered_df[filtered_df['dealer_type_eligibility'] == selected_dealer_type]
    
    # Display schemes
    st.markdown('<h2 class="sub-header">Schemes</h2>', unsafe_allow_html=True)
    
    if filtered_df.empty:
        st.info("No schemes match the selected filters.")
        return
    
    # Create cards for each scheme
    for _, scheme in filtered_df.iterrows():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.markdown(f"### {scheme['scheme_name']}")
            st.markdown(f"**Type:** {scheme['scheme_type']}")
            st.markdown(f"**Period:** {scheme['scheme_period_start']} to {scheme['scheme_period_end']}")
            st.markdown(f"**Region:** {scheme['applicable_region']}")
            st.markdown(f"**Dealer Eligibility:** {scheme['dealer_type_eligibility']}")
            
            # Display approval status with appropriate styling
            if scheme['approval_status'] == 'Approved':
                st.markdown("<span class='status-approved'>Approved</span>", unsafe_allow_html=True)
            elif scheme['approval_status'] == 'Pending':
                st.markdown("<span class='status-pending'>Pending Approval</span>", unsafe_allow_html=True)
            elif scheme['approval_status'] == 'Rejected':
                st.markdown("<span class='status-rejected'>Rejected</span>", unsafe_allow_html=True)
        
        with col2:
            st.button("View Details", key=f"view_{scheme['scheme_id']}", on_click=lambda sid=scheme['scheme_id']: view_scheme_details(sid))
        
        st.markdown("---")

def view_scheme_details(scheme_id):
    """View details of a specific scheme"""
    st.session_state.current_scheme = scheme_id
    navigate_to('scheme_details')

def render_scheme_details():
    """Render the details of a specific scheme"""
    if not st.session_state.current_scheme:
        st.error("No scheme selected.")
        st.button("Back to Scheme Explorer", on_click=lambda: navigate_to('scheme_explorer'))
        return
    
    scheme_id = st.session_state.current_scheme
    scheme_details = get_scheme_details(scheme_id)
    
    if not scheme_details:
        st.error("Failed to load scheme details.")
        st.button("Back to Scheme Explorer", on_click=lambda: navigate_to('scheme_explorer'))
        return
    
    scheme = scheme_details['scheme']
    products = scheme_details['products']
    rules = scheme_details['rules']
    parameters = scheme_details['parameters']
    bundle_offers = scheme_details['bundle_offers']
    payout_slabs = scheme_details['payout_slabs']
    
    # Header with scheme info
    st.markdown(f'<h1 class="main-header">{scheme["scheme_name"]}</h1>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"**Type:** {scheme['scheme_type']}")
        st.markdown(f"**Period:** {scheme['scheme_period_start']} to {scheme['scheme_period_end']}")
    
    with col2:
        st.markdown(f"**Region:** {scheme['applicable_region']}")
        st.markdown(f"**Dealer Eligibility:** {scheme['dealer_type_eligibility']}")
    
    with col3:
        # Display approval status with appropriate styling
        if scheme['approval_status'] == 'Approved':
            st.markdown("<span class='status-approved'>Approved</span>", unsafe_allow_html=True)
            st.markdown(f"Approved by: {scheme['approved_by']}")
        elif scheme['approval_status'] == 'Pending':
            st.markdown("<span class='status-pending'>Pending Approval</span>", unsafe_allow_html=True)
        elif scheme['approval_status'] == 'Rejected':
            st.markdown("<span class='status-rejected'>Rejected</span>", unsafe_allow_html=True)
            st.markdown(f"Rejected by: {scheme['approved_by']}")
    
    # Products in this scheme
    st.markdown('<h2 class="sub-header">Products</h2>', unsafe_allow_html=True)
    
    if products:
        # Convert to DataFrame for display
        products_df = pd.DataFrame([dict(p) for p in products])
        
        # Edit mode toggle
        edit_mode = st.checkbox("Edit Mode", value=st.session_state.edit_mode)
        st.session_state.edit_mode = edit_mode
        
        if edit_mode and scheme['approval_status'] == 'Pending':
            # Editable table
            for i, product in enumerate(products):
                col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                
                with col1:
                    st.markdown(f"**{product['product_name']}**")
                    st.markdown(f"{product['product_category']} - {product['product_subcategory'] if product['product_subcategory'] else 'N/A'}")
                
                with col2:
                    payout_amount = st.number_input(
                        "Payout Amount",
                        min_value=0.0,
                        value=float(product['payout_amount'] if product['payout_amount'] is not None else 0),
                        key=f"payout_{i}"
                    )
                
                with col3:
                    dealer_contribution = st.number_input(
                        "Dealer Contribution",
                        min_value=0.0,
                        value=float(product['dealer_contribution'] if product['dealer_contribution'] is not None else 0),
                        key=f"contribution_{i}"
                    )
                
                with col4:
                    total_payout = payout_amount + dealer_contribution
                    st.markdown(f"**Total: ₹{total_payout:.2f}**")
                
                with col5:
                    if st.button("Update", key=f"update_{i}"):
                        success = update_scheme_product(
                            scheme_id,
                            product['product_id'],
                            payout_amount,
                            dealer_contribution,
                            total_payout
                        )
                        if success:
                            st.success("Updated successfully!")
                            st.rerun()
                
                st.markdown("---")
        else:
            # Display as table
            display_cols = [
                'product_name', 'product_category', 'product_subcategory',
                'ram', 'storage', 'support_type', 'payout_type',
                'payout_amount', 'dealer_contribution', 'total_payout'
            ]
            display_cols = [col for col in display_cols if col in products_df.columns]
            
            st.dataframe(products_df[display_cols], use_container_width=True)
    else:
        st.info("No products associated with this scheme.")
    
    # Payout slabs if any
    if payout_slabs:
        st.markdown('<h2 class="sub-header">Payout Slabs</h2>', unsafe_allow_html=True)
        
        # Convert to DataFrame for display
        slabs_df = pd.DataFrame([dict(s) for s in payout_slabs])
        
        # Group by product
        for product_name in slabs_df['product_name'].unique():
            st.markdown(f"**{product_name}**")
            
            product_slabs = slabs_df[slabs_df['product_name'] == product_name]
            
            # Create a table for this product's slabs
            slab_data = []
            for _, slab in product_slabs.iterrows():
                max_qty = slab['max_quantity'] if slab['max_quantity'] is not None else "∞"
                slab_data.append({
                    "Quantity Range": f"{slab['min_quantity']} - {max_qty}",
                    "Payout Amount": f"₹{slab['payout_amount']:.2f}",
                    "Dealer Contribution": f"₹{slab['dealer_contribution']:.2f}",
                    "Total Payout": f"₹{slab['total_payout']:.2f}"
                })
            
            slab_table = pd.DataFrame(slab_data)
            st.table(slab_table)
    
    # Bundle offers if any
    if bundle_offers:
        st.markdown('<h2 class="sub-header">Bundle Offers</h2>', unsafe_allow_html=True)
        
        # Convert to DataFrame for display
        bundles_df = pd.DataFrame([dict(b) for b in bundle_offers])
        
        # Create a table for bundle offers
        bundle_data = []
        for _, bundle in bundles_df.iterrows():
            bundle_data.append({
                "Primary Product": bundle['primary_product'],
                "Bundle Product": bundle['bundle_product'],
                "Bundle Price": f"₹{bundle['bundle_price']:.2f}"
            })
        
        bundle_table = pd.DataFrame(bundle_data)
        st.table(bundle_table)
    
    # Rules
    if rules:
        st.markdown('<h2 class="sub-header">Scheme Rules</h2>', unsafe_allow_html=True)
        
        for rule in rules:
            st.markdown(f"**{rule['rule_type']}:** {rule['rule_description']}")
    
    # Parameters if any
    if parameters:
        st.markdown('<h2 class="sub-header">Scheme Parameters</h2>', unsafe_allow_html=True)
        
        for param in parameters:
            st.markdown(f"**{param['parameter_name']}:** {param['parameter_description']}")
            if param['parameter_criteria']:
                st.markdown(f"*Criteria: {param['parameter_criteria']}*")
    
    # Approval actions
    if scheme['approval_status'] == 'Pending':
        st.markdown('<h2 class="sub-header">Approval Actions</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            notes = st.text_area("Notes", key="approval_notes")
        
        with col2:
            approver = st.text_input("Approver Name", key="approver_name")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Approve", key="approve_btn"):
                    if not approver:
                        st.error("Please enter approver name.")
                    else:
                        success = update_scheme_approval(scheme_id, "Approved", approver, notes)
                        if success:
                            st.success("Scheme approved successfully!")
                            st.rerun()
            
            with col2:
                if st.button("Reject", key="reject_btn"):
                    if not approver:
                        st.error("Please enter approver name.")
                    else:
                        success = update_scheme_approval(scheme_id, "Rejected", approver, notes)
                        if success:
                            st.success("Scheme rejected successfully!")
                            st.rerun()
    
    # Navigation
    st.button("Back to Scheme Explorer", on_click=lambda: navigate_to('scheme_explorer'))

# Product Catalog components
def render_product_catalog():
    """Render the product catalog page"""
    st.markdown('<h1 class="main-header">Product Catalog</h1>', unsafe_allow_html=True)
    
    # Get all products
    products = get_all_products()
    
    if not products:
        st.info("No products found.")
        return
    
    # Convert to DataFrame for filtering
    products_df = pd.DataFrame([dict(p) for p in products])
    
    # Filters
    st.markdown('<h2 class="sub-header">Filters</h2>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'product_category' in products_df.columns:
            categories = ['All'] + sorted(products_df['product_category'].dropna().unique().tolist())
            selected_category = st.selectbox("Product Category", categories)
    
    with col2:
        # Filter subcategories based on selected category
        if 'product_subcategory' in products_df.columns:
            if selected_category != 'All':
                subcategories = ['All'] + sorted(products_df[products_df['product_category'] == selected_category]['product_subcategory'].dropna().unique().tolist())
            else:
                subcategories = ['All'] + sorted(products_df['product_subcategory'].dropna().unique().tolist())
            
            selected_subcategory = st.selectbox("Product Subcategory", subcategories)
    
    with col3:
        search_term = st.text_input("Search by Name or Code")
    
    # Apply filters
    filtered_df = products_df.copy()
    
    if selected_category != 'All':
        filtered_df = filtered_df[filtered_df['product_category'] == selected_category]
    
    if selected_subcategory != 'All':
        filtered_df = filtered_df[filtered_df['product_subcategory'] == selected_subcategory]
    
    if search_term:
        # Search in name and code
        name_mask = filtered_df['product_name'].str.contains(search_term, case=False, na=False)
        code_mask = filtered_df['product_code'].str.contains(search_term, case=False, na=False)
        filtered_df = filtered_df[name_mask | code_mask]
    
    # Display products
    st.markdown('<h2 class="sub-header">Products</h2>', unsafe_allow_html=True)
    
    if filtered_df.empty:
        st.info("No products match the selected filters.")
        return
    
    # Display as table with expandable rows
    st.dataframe(
        filtered_df[[
            'product_name', 'product_code', 'product_category', 
            'product_subcategory', 'ram', 'storage', 'dealer_price_dp', 'mrp'
        ]],
        use_container_width=True
    )
    
    # Product statistics
    st.markdown('<h2 class="sub-header">Product Statistics</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Category distribution
        if 'product_category' in products_df.columns:
            category_counts = products_df['product_category'].value_counts().reset_index()
            category_counts.columns = ['Category', 'Count']
            
            fig = px.pie(
                category_counts, 
                values='Count', 
                names='Category',
                title='Products by Category',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True, key="category_pie")
    
    with col2:
        # Price range distribution
        if 'dealer_price_dp' in products_df.columns:
            # Create price ranges
            products_df['price_range'] = pd.cut(
                products_df['dealer_price_dp'],
                bins=[0, 10000, 20000, 30000, 50000, 100000, float('inf')],
                labels=['0-10K', '10K-20K', '20K-30K', '30K-50K', '50K-100K', '100K+']
            )
            
            price_counts = products_df['price_range'].value_counts().reset_index()
            price_counts.columns = ['Price Range', 'Count']
            price_counts = price_counts.sort_values('Price Range')
            
            fig = px.bar(
                price_counts, 
                x='Price Range', 
                y='Count',
                title='Products by Price Range',
                color='Count',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True, key="price_bar")

# Dealer Management components
def render_dealer_management():
    """Render the dealer management page"""
    st.markdown('<h1 class="main-header">Dealer Management</h1>', unsafe_allow_html=True)
    
    # Get all dealers
    dealers = get_all_dealers()
    
    if not dealers:
        st.info("No dealers found.")
        return
    
    # Convert to DataFrame for filtering
    dealers_df = pd.DataFrame([dict(d) for d in dealers])
    
    # Filters
    st.markdown('<h2 class="sub-header">Filters</h2>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'region' in dealers_df.columns:
            regions = ['All'] + sorted(dealers_df['region'].dropna().unique().tolist())
            selected_region = st.selectbox("Region", regions)
    
    with col2:
        if 'dealer_type' in dealers_df.columns:
            dealer_types = ['All'] + sorted(dealers_df['dealer_type'].dropna().unique().tolist())
            selected_dealer_type = st.selectbox("Dealer Type", dealer_types)
    
    with col3:
        search_term = st.text_input("Search by Name or Code")
    
    # Apply filters
    filtered_df = dealers_df.copy()
    
    if selected_region != 'All':
        filtered_df = filtered_df[filtered_df['region'] == selected_region]
    
    if selected_dealer_type != 'All':
        filtered_df = filtered_df[filtered_df['dealer_type'] == selected_dealer_type]
    
    if search_term:
        # Search in name and code
        name_mask = filtered_df['dealer_name'].str.contains(search_term, case=False, na=False)
        code_mask = filtered_df['dealer_code'].str.contains(search_term, case=False, na=False)
        filtered_df = filtered_df[name_mask | code_mask]
    
    # Display dealers
    st.markdown('<h2 class="sub-header">Dealers</h2>', unsafe_allow_html=True)
    
    if filtered_df.empty:
        st.info("No dealers match the selected filters.")
        return
    
    # Display as table
    st.dataframe(
        filtered_df[[
            'dealer_name', 'dealer_code', 'dealer_type', 
            'region', 'state', 'city'
        ]],
        use_container_width=True
    )
    
    # Dealer statistics
    st.markdown('<h2 class="sub-header">Dealer Statistics</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Region distribution
        if 'region' in dealers_df.columns:
            region_counts = dealers_df['region'].value_counts().reset_index()
            region_counts.columns = ['Region', 'Count']
            
            fig = px.pie(
                region_counts, 
                values='Count', 
                names='Region',
                title='Dealers by Region',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True, key="region_pie_dealers")
    
    with col2:
        # Dealer type distribution
        if 'dealer_type' in dealers_df.columns:
            type_counts = dealers_df['dealer_type'].value_counts().reset_index()
            type_counts.columns = ['Dealer Type', 'Count']
            
            fig = px.bar(
                type_counts, 
                x='Dealer Type', 
                y='Count',
                title='Dealers by Type',
                color='Count',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True, key="type_bar_dealers")
    
    # Dealer performance (if sales data is available)
    sales_data = get_sales_data(90)  # Last 90 days
    
    if sales_data:
        st.markdown('<h2 class="sub-header">Dealer Performance (Last 90 Days)</h2>', unsafe_allow_html=True)
        
        # Convert to DataFrame
        sales_df = pd.DataFrame([dict(s) for s in sales_data])
        
        # Group by dealer and calculate metrics
        dealer_performance = sales_df.groupby('dealer_name').agg({
            'quantity_sold': 'sum',
            'earned_dealer_incentive_amount': 'sum',
            'sale_id': 'count'
        }).reset_index()
        
        dealer_performance.columns = ['Dealer', 'Units Sold', 'Incentive Earned', 'Transaction Count']
        dealer_performance = dealer_performance.sort_values('Units Sold', ascending=False)
        
        # Display as table
        st.dataframe(dealer_performance, use_container_width=True)
        
        # Performance visualization
        col1, col2 = st.columns(2)
        
        with col1:
            # Units sold by top dealers
            top_dealers = dealer_performance.head(10)
            
            fig = px.bar(
                top_dealers, 
                x='Dealer', 
                y='Units Sold',
                title='Top 10 Dealers by Units Sold',
                color='Units Sold',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                height=400
            )
            st.plotly_chart(fig, use_container_width=True, key="top_dealers_bar")
        
        with col2:
            # Incentive earned by top dealers
            top_incentive = dealer_performance.sort_values('Incentive Earned', ascending=False).head(10)
            
            fig = px.bar(
                top_incentive, 
                x='Dealer', 
                y='Incentive Earned',
                title='Top 10 Dealers by Incentive Earned',
                color='Incentive Earned',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                height=400
            )
            st.plotly_chart(fig, use_container_width=True, key="top_incentive_bar")

# Sales Tracker components
def render_sales_tracker():
    """Render the sales tracker page"""
    st.markdown('<h1 class="main-header">Sales Tracker</h1>', unsafe_allow_html=True)
    
    # Get sales data
    col1, col2 = st.columns(2)
    
    with col1:
        days = st.slider("Time Period (Days)", min_value=7, max_value=365, value=30, step=1)
    
    with col2:
        st.write("")  # Placeholder for alignment
    
    sales_data = get_sales_data(days)
    
    if not sales_data:
        st.info(f"No sales data found for the last {days} days.")
        return
    
    # Convert to DataFrame for analysis
    sales_df = pd.DataFrame([dict(s) for s in sales_data])
    
    # Summary metrics
    st.markdown('<h2 class="sub-header">Summary</h2>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_units = sales_df['quantity_sold'].sum()
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{total_units}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Total Units Sold</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        total_dealers = sales_df['dealer_name'].nunique()
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{total_dealers}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Active Dealers</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        total_products = sales_df['product_name'].nunique()
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{total_products}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Products Sold</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        total_incentive = sales_df['earned_dealer_incentive_amount'].sum()
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">₹{total_incentive:,.2f}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Total Incentives</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Sales trend
    st.markdown('<h2 class="sub-header">Sales Trend</h2>', unsafe_allow_html=True)
    
    # Convert timestamp to datetime
    sales_df['sale_date'] = pd.to_datetime(sales_df['sale_timestamp']).dt.date
    
    # Group by date and sum quantities
    daily_sales = sales_df.groupby('sale_date')['quantity_sold'].sum().reset_index()
    
    # Create a date range for the selected period
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days-1)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_df = pd.DataFrame({'sale_date': date_range.date})
    
    # Merge with daily sales to fill in missing dates
    complete_daily_sales = pd.merge(date_df, daily_sales, on='sale_date', how='left').fillna(0)
    
    # Create the line chart
    fig = px.line(
        complete_daily_sales, 
        x='sale_date', 
        y='quantity_sold',
        title=f'Daily Sales (Last {days} Days)',
        labels={'sale_date': 'Date', 'quantity_sold': 'Units Sold'},
        markers=True
    )
    fig.update_layout(
        xaxis_title='Date',
        yaxis_title='Units Sold',
        hovermode='x unified',
        height=400
    )
    st.plotly_chart(fig, use_container_width=True, key="sales_trend_tracker")
    
    # Sales by category
    st.markdown('<h2 class="sub-header">Sales by Category</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Product category distribution
        category_sales = sales_df.groupby('product_category')['quantity_sold'].sum().reset_index()
        category_sales = category_sales.sort_values('quantity_sold', ascending=False)
        
        fig = px.pie(
            category_sales, 
            values='quantity_sold', 
            names='product_category',
            title='Sales by Product Category',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, key="category_pie_sales")
    
    with col2:
        # Region distribution
        region_sales = sales_df.groupby('region')['quantity_sold'].sum().reset_index()
        region_sales = region_sales.sort_values('quantity_sold', ascending=False)
        
        fig = px.pie(
            region_sales, 
            values='quantity_sold', 
            names='region',
            title='Sales by Region',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, key="region_pie_sales")
    
    # Sales transactions
    st.markdown('<h2 class="sub-header">Sales Transactions</h2>', unsafe_allow_html=True)
    
    # Filters for transactions
    col1, col2, col3 = st.columns(3)
    
    with col1:
        verification_status = st.selectbox(
            "Verification Status",
            ['All', 'Verified', 'Pending', 'Simulated']
        )
    
    with col2:
        if 'product_category' in sales_df.columns:
            categories = ['All'] + sorted(sales_df['product_category'].dropna().unique().tolist())
            selected_category = st.selectbox("Product Category", categories, key="sales_category")
    
    with col3:
        if 'region' in sales_df.columns:
            regions = ['All'] + sorted(sales_df['region'].dropna().unique().tolist())
            selected_region = st.selectbox("Region", regions, key="sales_region")
    
    # Apply filters
    filtered_sales = sales_df.copy()
    
    if verification_status != 'All':
        filtered_sales = filtered_sales[filtered_sales['verification_status'] == verification_status]
    
    if selected_category != 'All':
        filtered_sales = filtered_sales[filtered_sales['product_category'] == selected_category]
    
    if selected_region != 'All':
        filtered_sales = filtered_sales[filtered_sales['region'] == selected_region]
    
    # Display transactions
    if filtered_sales.empty:
        st.info("No transactions match the selected filters.")
    else:
        # Sort by date (newest first)
        filtered_sales = filtered_sales.sort_values('sale_timestamp', ascending=False)
        
        # Display as table
        display_cols = [
            'sale_timestamp', 'dealer_name', 'region', 'product_name',
            'quantity_sold', 'earned_dealer_incentive_amount', 'verification_status'
        ]
        
        # Format the DataFrame for display
        display_df = filtered_sales[display_cols].copy()
        display_df['sale_timestamp'] = pd.to_datetime(display_df['sale_timestamp']).dt.strftime('%Y-%m-%d')
        display_df.columns = [
            'Date', 'Dealer', 'Region', 'Product',
            'Quantity', 'Incentive Amount', 'Status'
        ]
        
        st.dataframe(display_df, use_container_width=True)

# Upload Scheme components
def render_upload_scheme():
    """Render the upload scheme page"""
    st.markdown('<h1 class="main-header">Upload New Scheme</h1>', unsafe_allow_html=True)
    
    # Step 1: Upload PDF
    st.markdown('<h2 class="sub-header">Step 1: Upload Scheme PDF</h2>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("Choose a PDF file", type="pdf")
    
    if uploaded_file is not None:
        # Save the uploaded file
        pdf_path = save_uploaded_pdf(uploaded_file)
        st.session_state.uploaded_pdf = pdf_path
        
        if pdf_path:
            st.success(f"PDF uploaded successfully: {os.path.basename(pdf_path)}")
            
            # Step 2: Extract text
            st.markdown('<h2 class="sub-header">Step 2: Extract Text</h2>', unsafe_allow_html=True)
            
            if st.button("Extract Text from PDF"):
                with st.spinner("Extracting text from PDF..."):
                    # Load secrets for AWS integration
                    secrets = load_secrets()
                    
                    # Initialize AWS clients
                    from pdf_processor_fixed import initialize_aws_clients
                    bedrock_client, textract_client = initialize_aws_clients(secrets)
                    
                    # Extract text
                    pages_text = extract_text_from_pdf(pdf_path, textract_client)
                    
                    if pages_text:
                        # Combine text from all pages
                        full_text = "\n\n".join([page[1] for page in pages_text])
                        st.session_state.extracted_text = full_text
                        
                        # Show a preview
                        st.markdown('<h3>Text Preview</h3>', unsafe_allow_html=True)
                        st.text_area("Extracted Text", full_text, height=200)
                        
                        # Step 3: Extract structured data
                        st.markdown('<h2 class="sub-header">Step 3: Extract Structured Data</h2>', unsafe_allow_html=True)
                        
                        if st.button("Extract Structured Data"):
                            with st.spinner("Extracting structured data..."):
                                # Get inference profile ARN
                                inference_profile_arn = secrets.get('INFERENCE_PROFILE_CLAUDE')
                                
                                # Extract structured data
                                structured_data = extract_structured_data_from_text(
                                    full_text,
                                    os.path.basename(pdf_path),
                                    bedrock_client,
                                    inference_profile_arn
                                )
                                
                                if structured_data:
                                    st.session_state.structured_data = structured_data
                                    
                                    # Show a preview
                                    st.markdown('<h3>Structured Data Preview</h3>', unsafe_allow_html=True)
                                    
                                    # Display scheme details
                                    st.markdown('<h4>Scheme Details</h4>', unsafe_allow_html=True)
                                    scheme_details = {
                                        'Scheme Name': structured_data.get('scheme_name'),
                                        'Scheme Type': structured_data.get('scheme_type'),
                                        'Period Start': structured_data.get('scheme_period_start'),
                                        'Period End': structured_data.get('scheme_period_end'),
                                        'Region': structured_data.get('applicable_region'),
                                        'Dealer Eligibility': structured_data.get('dealer_type_eligibility')
                                    }
                                    st.json(scheme_details)
                                    
                                    # Display products
                                    st.markdown('<h4>Products</h4>', unsafe_allow_html=True)
                                    if 'products' in structured_data and structured_data['products']:
                                        products_df = pd.DataFrame(structured_data['products'])
                                        st.dataframe(products_df, use_container_width=True)
                                    else:
                                        st.info("No products found in the structured data.")
                                    
                                    # Step 4: Save to database
                                    st.markdown('<h2 class="sub-header">Step 4: Save to Database</h2>', unsafe_allow_html=True)
                                    
                                    if st.button("Save Scheme to Database"):
                                        with st.spinner("Saving scheme to database..."):
                                            scheme_id = add_new_scheme_from_data(structured_data, pdf_path)
                                            
                                            if scheme_id:
                                                st.success(f"Scheme saved successfully with ID: {scheme_id}")
                                                st.markdown("The scheme has been added with 'Pending' approval status. It will be available in the system once approved.")
                                                
                                                # Option to view the scheme
                                                if st.button("View Scheme Details"):
                                                    st.session_state.current_scheme = scheme_id
                                                    navigate_to('scheme_details')
                                            else:
                                                st.error("Failed to save scheme to database.")
                                else:
                                    st.error("Failed to extract structured data from the PDF.")
                    else:
                        st.error("Failed to extract text from the PDF.")

# Approval Center components
def render_approval_center():
    """Render the approval center page"""
    st.markdown('<h1 class="main-header">Approval Center</h1>', unsafe_allow_html=True)
    
    # Get pending approvals
    pending_schemes = get_pending_approvals()
    
    if not pending_schemes:
        st.info("No schemes pending approval.")
        return
    
    # Convert to DataFrame for display
    pending_df = pd.DataFrame([dict(s) for s in pending_schemes])
    
    # Display pending schemes
    st.markdown('<h2 class="sub-header">Schemes Pending Approval</h2>', unsafe_allow_html=True)
    
    for _, scheme in pending_df.iterrows():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.markdown(f"### {scheme['scheme_name']}")
            st.markdown(f"**Type:** {scheme['scheme_type']}")
            st.markdown(f"**Period:** {scheme['scheme_period_start']} to {scheme['scheme_period_end']}")
            st.markdown(f"**Region:** {scheme['applicable_region']}")
            st.markdown(f"**Dealer Eligibility:** {scheme['dealer_type_eligibility']}")
            st.markdown(f"**Uploaded:** {pd.to_datetime(scheme['upload_timestamp']).strftime('%Y-%m-%d %H:%M')}")
        
        with col2:
            st.button("Review", key=f"review_{scheme['scheme_id']}", on_click=lambda sid=scheme['scheme_id']: view_scheme_details(sid))
        
        st.markdown("---")
    
    # Approval statistics
    st.markdown('<h2 class="sub-header">Approval Statistics</h2>', unsafe_allow_html=True)
    
    # Get all schemes for statistics
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT approval_status, COUNT(*) as count
            FROM schemes
            GROUP BY approval_status
            """)
            approval_stats = cursor.fetchall()
            
            if approval_stats:
                # Convert to DataFrame
                stats_df = pd.DataFrame([dict(s) for s in approval_stats])
                
                # Create pie chart
                fig = px.pie(
                    stats_df, 
                    values='count', 
                    names='approval_status',
                    title='Schemes by Approval Status',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True, key="approval_pie")
            
            # Recent approvals
            cursor.execute("""
            SELECT s.scheme_name, s.scheme_type, s.approval_status, 
                   s.approved_by, s.approval_timestamp
            FROM schemes s
            WHERE s.approval_status != 'Pending'
            ORDER BY s.approval_timestamp DESC
            LIMIT 5
            """)
            recent_approvals = cursor.fetchall()
            
            if recent_approvals:
                st.markdown('<h2 class="sub-header">Recent Approvals</h2>', unsafe_allow_html=True)
                
                # Convert to DataFrame
                recent_df = pd.DataFrame([dict(a) for a in recent_approvals])
                
                # Display as table
                for _, approval in recent_df.iterrows():
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown(f"**{approval['scheme_name']}** ({approval['scheme_type']})")
                    
                    with col2:
                        status_class = "status-approved" if approval['approval_status'] == 'Approved' else "status-rejected"
                        st.markdown(f"<span class='{status_class}'>{approval['approval_status']}</span> by {approval['approved_by']}", unsafe_allow_html=True)
                        st.markdown(f"on {pd.to_datetime(approval['approval_timestamp']).strftime('%Y-%m-%d %H:%M')}")
                    
                    st.markdown("---")
        finally:
            conn.close()

# Simulate Sales components
def render_simulate_sales():
    """Render the sales simulation page"""
    st.markdown('<h1 class="main-header">Sales Simulation</h1>', unsafe_allow_html=True)
    
    # Get active schemes, products, and dealers
    schemes = get_active_schemes()
    products = get_all_products()
    dealers = get_all_dealers()
    
    if not schemes or not products or not dealers:
        st.error("Cannot simulate sales: Missing schemes, products, or dealers.")
        return
    
    # Convert to DataFrames
    schemes_df = pd.DataFrame([dict(s) for s in schemes])
    products_df = pd.DataFrame([dict(p) for p in products])
    dealers_df = pd.DataFrame([dict(d) for d in dealers])
    
    # Simulation form
    st.markdown('<h2 class="sub-header">Simulation Parameters</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Select scheme
        scheme_options = [(s['scheme_id'], s['scheme_name']) for s in schemes]
        selected_scheme_id = st.selectbox(
            "Select Scheme",
            options=[s[0] for s in scheme_options],
            format_func=lambda x: next((s[1] for s in scheme_options if s[0] == x), ""),
            key="sim_scheme"
        )
        
        # Get products for this scheme
        scheme_products = get_scheme_products(selected_scheme_id)
        
        if not scheme_products:
            st.warning("No products associated with this scheme.")
            return
        
        # Select product
        product_options = [(p['product_id'], p['product_name']) for p in scheme_products]
        selected_product_id = st.selectbox(
            "Select Product",
            options=[p[0] for p in product_options],
            format_func=lambda x: next((p[1] for p in product_options if p[0] == x), ""),
            key="sim_product"
        )
        
        # Get selected product details
        selected_product = next((p for p in scheme_products if p['product_id'] == selected_product_id), None)
        
        # Select dealer
        dealer_options = [(d['dealer_id'], d['dealer_name']) for d in dealers]
        selected_dealer_id = st.selectbox(
            "Select Dealer",
            options=[d[0] for d in dealer_options],
            format_func=lambda x: next((d[1] for d in dealer_options if d[0] == x), ""),
            key="sim_dealer"
        )
    
    with col2:
        # Quantity
        quantity = st.number_input("Quantity", min_value=1, value=1, key="sim_quantity")
        
        # Display product details
        if selected_product:
            st.markdown("**Product Details:**")
            st.markdown(f"Category: {selected_product['product_category']}")
            
            # Ensure dealer_price_dp is not None before using it
            dealer_price = selected_product['dealer_price_dp'] if selected_product['dealer_price_dp'] is not None else 0
            st.markdown(f"Dealer Price: ₹{dealer_price:,.2f}")
            
            # Calculate incentive
            payout_amount = selected_product['payout_amount'] if selected_product['payout_amount'] is not None else 0
            incentive = payout_amount * quantity
            st.markdown(f"Incentive per Unit: ₹{payout_amount:,.2f}")
            st.markdown(f"Total Incentive: ₹{incentive:,.2f}")
        
        # Simulate button
        simulate_button = st.button("Simulate Sale", key="sim_button")
    
    # Run simulation
    if simulate_button and selected_product:
        # Calculate values
        dealer_price = selected_product['dealer_price_dp'] if selected_product['dealer_price_dp'] is not None else 0
        total_dealer_price = dealer_price * quantity
        
        payout_amount = selected_product['payout_amount'] if selected_product['payout_amount'] is not None else 0
        total_incentive = payout_amount * quantity
        
        # Store simulation results
        st.session_state.simulation_results = {
            'scheme_id': selected_scheme_id,
            'scheme_name': next((s['scheme_name'] for s in schemes if s['scheme_id'] == selected_scheme_id), ""),
            'product_id': selected_product_id,
            'product_name': selected_product['product_name'],
            'dealer_id': selected_dealer_id,
            'dealer_name': next((d['dealer_name'] for d in dealers if d['dealer_id'] == selected_dealer_id), ""),
            'quantity': quantity,
            'dealer_price': dealer_price,
            'total_dealer_price': total_dealer_price,
            'incentive_per_unit': payout_amount,
            'total_incentive': total_incentive
        }
        
        st.session_state.show_simulation_results = True
    
    # Display simulation results
    if st.session_state.show_simulation_results and st.session_state.simulation_results:
        st.markdown('<h2 class="sub-header">Simulation Results</h2>', unsafe_allow_html=True)
        
        results = st.session_state.simulation_results
        
        # Display as card
        st.markdown('<div class="card">', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Scheme:** {results['scheme_name']}")
            st.markdown(f"**Product:** {results['product_name']}")
            st.markdown(f"**Dealer:** {results['dealer_name']}")
            st.markdown(f"**Quantity:** {results['quantity']}")
        
        with col2:
            st.markdown(f"**Dealer Price:** ₹{results['dealer_price']:,.2f}")
            st.markdown(f"**Total Dealer Price:** ₹{results['total_dealer_price']:,.2f}")
            st.markdown(f"**Incentive per Unit:** ₹{results['incentive_per_unit']:,.2f}")
            st.markdown(f"**Total Incentive:** ₹{results['total_incentive']:,.2f}")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Option to record the simulated sale
        if st.button("Record Simulated Sale", key="record_sim"):
            success = add_simulated_sale(
                results['dealer_id'],
                results['product_id'],
                results['scheme_id'],
                results['quantity'],
                results['dealer_price'],
                results['total_incentive']
            )
            
            if success:
                st.success("Simulated sale recorded successfully!")
                st.session_state.show_simulation_results = False
                st.session_state.simulation_results = None
                st.rerun()
            else:
                st.error("Failed to record simulated sale.")
        
        # Option to discard
        if st.button("Discard Simulation", key="discard_sim"):
            st.session_state.show_simulation_results = False
            st.session_state.simulation_results = None
            st.rerun()
    
    # Simulation history
    st.markdown('<h2 class="sub-header">Simulation History</h2>', unsafe_allow_html=True)
    
    # Get simulated sales
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT st.sale_id, d.dealer_name, p.product_name, 
                   s.scheme_name, st.quantity_sold, 
                   st.dealer_price_dp, st.earned_dealer_incentive_amount,
                   st.sale_timestamp
            FROM sales_transactions st
            JOIN dealers d ON st.dealer_id = d.dealer_id
            JOIN products p ON st.product_id = p.product_id
            JOIN schemes s ON st.scheme_id = s.scheme_id
            WHERE st.verification_status = 'Simulated'
            ORDER BY st.sale_timestamp DESC
            LIMIT 10
            """)
            simulated_sales = cursor.fetchall()
            
            if simulated_sales:
                # Convert to DataFrame
                sim_df = pd.DataFrame([dict(s) for s in simulated_sales])
                
                # Display as table
                sim_df['sale_timestamp'] = pd.to_datetime(sim_df['sale_timestamp']).dt.strftime('%Y-%m-%d %H:%M')
                
                display_cols = [
                    'sale_timestamp', 'dealer_name', 'product_name', 'scheme_name',
                    'quantity_sold', 'dealer_price_dp', 'earned_dealer_incentive_amount'
                ]
                
                display_df = sim_df[display_cols].copy()
                display_df.columns = [
                    'Date', 'Dealer', 'Product', 'Scheme',
                    'Quantity', 'Dealer Price', 'Incentive Amount'
                ]
                
                st.dataframe(display_df, use_container_width=True)
            else:
                st.info("No simulation history found.")
        finally:
            conn.close()

# Sidebar navigation
def render_sidebar():
    """Render the sidebar navigation"""
    st.sidebar.markdown('<h1 style="text-align: center;">DNS</h1>', unsafe_allow_html=True)
    
    # Navigation buttons
    if st.sidebar.button("Dashboard", use_container_width=True):
        navigate_to('dashboard')
    
    if st.sidebar.button("Scheme Explorer", use_container_width=True):
        navigate_to('scheme_explorer')
    
    if st.sidebar.button("Product Catalog", use_container_width=True):
        navigate_to('product_catalog')
    
    if st.sidebar.button("Dealer Management", use_container_width=True):
        navigate_to('dealer_management')
    
    if st.sidebar.button("Sales Tracker", use_container_width=True):
        navigate_to('sales_tracker')
    
    if st.sidebar.button("Upload Scheme", use_container_width=True):
        navigate_to('upload_scheme')
    
    if st.sidebar.button("Approval Center", use_container_width=True):
        navigate_to('approval_center')
    
    if st.sidebar.button("Simulate Sales", use_container_width=True):
        navigate_to('simulate_sales')
    
    # Sidebar footer
    st.sidebar.markdown('---')
    st.sidebar.markdown('<div style="text-align: center;">Dealer Nudging System v2.0</div>', unsafe_allow_html=True)

# Main function
def main():
    """Main function to render the app"""
    # Render sidebar
    render_sidebar()
    
    # Render the selected page
    if st.session_state.page == 'dashboard':
        render_dashboard()
    elif st.session_state.page == 'scheme_explorer':
        render_scheme_explorer()
    elif st.session_state.page == 'scheme_details':
        render_scheme_details()
    elif st.session_state.page == 'product_catalog':
        render_product_catalog()
    elif st.session_state.page == 'dealer_management':
        render_dealer_management()
    elif st.session_state.page == 'sales_tracker':
        render_sales_tracker()
    elif st.session_state.page == 'upload_scheme':
        render_upload_scheme()
    elif st.session_state.page == 'approval_center':
        render_approval_center()
    elif st.session_state.page == 'simulate_sales':
        render_simulate_sales()

if __name__ == "__main__":
    main()

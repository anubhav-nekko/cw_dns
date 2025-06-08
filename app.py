import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json
import datetime
import uuid
import tempfile
import shutil
from pdf_processor_fixed import extract_text_from_pdf, extract_structured_data_from_text, connect_db, initialize_aws_clients

# Set page configuration
st.set_page_config(
    page_title="Dealer Nudging System (DNS)",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
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
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    .metric-card {
        background-color: #e3f2fd;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        text-align: center;
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
    .free-item-alert {
        background-color: #e8f5e9;
        border-left: 5px solid #4caf50;
        padding: 15px;
        margin: 10px 0;
        border-radius: 5px;
    }
    .scheme-card {
        background-color: #f5f5f5;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 15px;
        border-left: 4px solid #2196F3;
    }
    .approval-pending {
        border-left: 4px solid #FFC107;
    }
    .approval-approved {
        border-left: 4px solid #4CAF50;
    }
    .approval-rejected {
        border-left: 4px solid #F44336;
    }
    .edit-mode {
        background-color: #fff8e1;
    }
    .table-container {
        overflow-x: auto;
    }
    .stButton>button {
        background-color: #1976D2;
        color: white;
        font-weight: 500;
    }
    .stButton>button:hover {
        background-color: #1565C0;
    }
</style>
""", unsafe_allow_html=True)

# Load secrets
def load_secrets():
    """Load AWS and API secrets from secrets.json"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_path = os.path.join(current_dir, 'secrets.json')
    
    try:
        with open(secrets_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Error loading secrets: {e}")
        return {}

# Initialize session state
def init_session_state():
    """Initialize session state variables"""
    if 'page' not in st.session_state:
        st.session_state.page = 'dashboard'
    
    if 'edit_mode' not in st.session_state:
        st.session_state.edit_mode = False
    
    if 'edited_scheme' not in st.session_state:
        st.session_state.edited_scheme = None
    
    if 'edited_products' not in st.session_state:
        st.session_state.edited_products = None
    
    if 'approval_requests' not in st.session_state:
        st.session_state.approval_requests = []
    
    if 'notifications' not in st.session_state:
        st.session_state.notifications = []
    
    if 'selected_scheme_id' not in st.session_state:
        st.session_state.selected_scheme_id = None
    
    if 'selected_dealer_id' not in st.session_state:
        st.session_state.selected_dealer_id = None
    
    if 'selected_product_id' not in st.session_state:
        st.session_state.selected_product_id = None

# Navigation
def render_sidebar():
    """Render the sidebar navigation"""
    st.sidebar.title("DNS Navigation")
    
    # Main navigation
    if st.sidebar.button("📊 Dashboard", key="nav_dashboard"):
        st.session_state.page = 'dashboard'
    
    if st.sidebar.button("🔍 Scheme Explorer", key="nav_schemes"):
        st.session_state.page = 'schemes'
    
    if st.sidebar.button("📱 Products", key="nav_products"):
        st.session_state.page = 'products'
    
    if st.sidebar.button("🏪 Dealers", key="nav_dealers"):
        st.session_state.page = 'dealers'
    
    if st.sidebar.button("💰 Sales Simulation", key="nav_simulation"):
        st.session_state.page = 'simulation'
    
    if st.sidebar.button("📤 Upload Scheme", key="nav_upload"):
        st.session_state.page = 'upload'
    
    # Approval section
    st.sidebar.markdown("---")
    st.sidebar.subheader("Approvals")
    
    # Check for pending approvals
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM scheme_approvals WHERE approval_status = 'Pending'")
    pending_count = cursor.fetchone()[0]
    conn.close()
    
    if pending_count > 0:
        if st.sidebar.button(f"⏳ Pending Approvals ({pending_count})", key="nav_approvals"):
            st.session_state.page = 'approvals'
    else:
        st.sidebar.markdown("No pending approvals")
    
    # Settings and help
    st.sidebar.markdown("---")
    if st.sidebar.button("⚙️ Settings", key="nav_settings"):
        st.session_state.page = 'settings'
    
    if st.sidebar.button("❓ Help", key="nav_help"):
        st.session_state.page = 'help'
    
    # Display current date
    st.sidebar.markdown("---")
    current_date = datetime.datetime.now().strftime("%B %d, %Y")
    st.sidebar.markdown(f"**Current Date:** {current_date}")

# Dashboard
def render_dashboard():
    """Render the main dashboard"""
    st.markdown("<h1 class='main-header'>Dealer Nudging System Dashboard</h1>", unsafe_allow_html=True)
    
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    # Get summary metrics
    cursor.execute("SELECT COUNT(*) FROM schemes WHERE deal_status = 'Active'")
    active_schemes = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM products WHERE is_active = 1")
    active_products = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dealers WHERE is_active = 1")
    active_dealers = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM sales_transactions")
    total_sales = cursor.fetchone()[0]
    
    # Display metrics in a row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-value'>{active_schemes}</div>", unsafe_allow_html=True)
        st.markdown("<div class='metric-label'>Active Schemes</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-value'>{active_products}</div>", unsafe_allow_html=True)
        st.markdown("<div class='metric-label'>Active Products</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col3:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-value'>{active_dealers}</div>", unsafe_allow_html=True)
        st.markdown("<div class='metric-label'>Active Dealers</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col4:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-value'>{total_sales}</div>", unsafe_allow_html=True)
        st.markdown("<div class='metric-label'>Total Sales</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Scheme effectiveness analysis
    st.markdown("<h2 class='sub-header'>Scheme Effectiveness Analysis</h2>", unsafe_allow_html=True)
    
    try:
        # Get scheme performance data
        cursor.execute("""
        SELECT s.scheme_name, COUNT(st.sale_id) as sales_count, SUM(st.earned_dealer_incentive_amount) as total_incentive
        FROM schemes s
        LEFT JOIN sales_transactions st ON s.scheme_id = st.scheme_id
        WHERE s.deal_status = 'Active'
        GROUP BY s.scheme_id
        ORDER BY total_incentive DESC
        LIMIT 10
        """)
        
        scheme_performance = cursor.fetchall()
        
        if scheme_performance and len(scheme_performance) > 0:
            # Convert to DataFrame
            scheme_df = pd.DataFrame(scheme_performance, columns=['Scheme', 'Sales Count', 'Total Incentive'])
            scheme_df = scheme_df.fillna(0)  # Replace NaN with 0
            
            # Create bar chart
            fig = px.bar(
                scheme_df,
                x='Scheme',
                y='Total Incentive',
                color='Sales Count',
                labels={'Total Incentive': 'Total Incentive Amount (₹)', 'Scheme': 'Scheme Name'},
                title='Top Performing Schemes by Incentive Amount',
                color_continuous_scale=px.colors.sequential.Blues
            )
            
            fig.update_layout(
                xaxis_tickangle=-45,
                height=500,
                margin=dict(l=20, r=20, t=40, b=100)
            )
            
            st.plotly_chart(fig, use_container_width=True, key="scheme_performance_chart")
        else:
            st.info("No scheme performance data available yet. Start recording sales to see this chart.")
    except Exception as e:
        st.error(f"Error generating scheme effectiveness chart: {str(e)}")
    
    # Product performance heatmap
    st.markdown("<h2 class='sub-header'>Product Performance Heatmap</h2>", unsafe_allow_html=True)
    
    try:
        # Get product performance by scheme
        cursor.execute("""
        SELECT p.product_name, s.scheme_name, COUNT(st.sale_id) as sales_count
        FROM products p
        JOIN sales_transactions st ON p.product_id = st.product_id
        JOIN schemes s ON st.scheme_id = s.scheme_id
        WHERE p.is_active = 1 AND s.deal_status = 'Active'
        GROUP BY p.product_id, s.scheme_id
        ORDER BY sales_count DESC
        LIMIT 50
        """)
        
        product_scheme_performance = cursor.fetchall()
        
        if product_scheme_performance and len(product_scheme_performance) > 0:
            # Convert to DataFrame
            product_scheme_df = pd.DataFrame(product_scheme_performance, columns=['Product', 'Scheme', 'Sales Count'])
            
            # Create pivot table for heatmap
            pivot_df = product_scheme_df.pivot_table(
                values='Sales Count',
                index='Product',
                columns='Scheme',
                fill_value=0
            )
            
            # Create heatmap
            fig = px.imshow(
                pivot_df,
                labels=dict(x="Scheme", y="Product", color="Sales Count"),
                x=pivot_df.columns,
                y=pivot_df.index,
                color_continuous_scale='Blues',
                title='Product Performance by Scheme'
            )
            
            fig.update_layout(
                height=600,
                margin=dict(l=20, r=20, t=40, b=20)
            )
            
            st.plotly_chart(fig, use_container_width=True, key="product_heatmap_chart")
        else:
            st.info("No product performance data available yet. Start recording sales to see this heatmap.")
    except Exception as e:
        st.error(f"Error generating product performance heatmap: {str(e)}")
    
    # Regional performance
    st.markdown("<h2 class='sub-header'>Regional Performance</h2>", unsafe_allow_html=True)
    
    try:
        # Get regional performance data
        cursor.execute("""
        SELECT d.region, COUNT(st.sale_id) as sales_count, SUM(st.earned_dealer_incentive_amount) as total_incentive
        FROM dealers d
        JOIN sales_transactions st ON d.dealer_id = st.dealer_id
        GROUP BY d.region
        ORDER BY total_incentive DESC
        """)
        
        regional_performance = cursor.fetchall()
        
        if regional_performance and len(regional_performance) > 0:
            # Convert to DataFrame
            region_df = pd.DataFrame(regional_performance, columns=['Region', 'Sales Count', 'Total Incentive'])
            region_df = region_df.fillna(0)  # Replace NaN with 0
            
            # Create pie chart
            fig = px.pie(
                region_df,
                values='Total Incentive',
                names='Region',
                title='Incentive Distribution by Region',
                color_discrete_sequence=px.colors.sequential.Blues_r
            )
            
            fig.update_layout(
                height=500,
                margin=dict(l=20, r=20, t=40, b=20)
            )
            
            st.plotly_chart(fig, use_container_width=True, key="regional_performance_chart")
        else:
            st.info("No regional performance data available yet. Start recording sales to see this chart.")
    except Exception as e:
        st.error(f"Error generating regional performance chart: {str(e)}")
    
    # Recent activity
    st.markdown("<h2 class='sub-header'>Recent Activity</h2>", unsafe_allow_html=True)
    
    try:
        # Get recent sales transactions
        cursor.execute("""
        SELECT st.sale_id, d.dealer_name, p.product_name, s.scheme_name, 
               st.quantity_sold, st.earned_dealer_incentive_amount, st.sale_timestamp
        FROM sales_transactions st
        JOIN dealers d ON st.dealer_id = d.dealer_id
        JOIN products p ON st.product_id = p.product_id
        JOIN schemes s ON st.scheme_id = s.scheme_id
        ORDER BY st.sale_timestamp DESC
        LIMIT 10
        """)
        
        recent_sales = cursor.fetchall()
        
        if recent_sales and len(recent_sales) > 0:
            # Convert to DataFrame
            sales_df = pd.DataFrame(recent_sales, columns=[
                'Sale ID', 'Dealer', 'Product', 'Scheme', 
                'Quantity', 'Incentive Amount', 'Timestamp'
            ])
            
            # Format timestamp
            sales_df['Timestamp'] = pd.to_datetime(sales_df['Timestamp']).dt.strftime('%Y-%m-%d %H:%M')
            
            # Display as table
            st.markdown("<div class='table-container'>", unsafe_allow_html=True)
            st.table(sales_df)
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("No recent sales activity available yet.")
    except Exception as e:
        st.error(f"Error retrieving recent activity: {str(e)}")
    
    conn.close()

# Scheme Explorer
def render_schemes():
    """Render the scheme explorer page"""
    st.markdown("<h1 class='main-header'>Scheme Explorer</h1>", unsafe_allow_html=True)
    
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    # Filters
    st.markdown("<h2 class='sub-header'>Filters</h2>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Get scheme types
        cursor.execute("SELECT DISTINCT scheme_type FROM schemes WHERE scheme_type IS NOT NULL")
        scheme_types = [row[0] for row in cursor.fetchall()]
        scheme_types = ['All'] + scheme_types
        
        selected_type = st.selectbox("Scheme Type", scheme_types, key="scheme_type_filter")
    
    with col2:
        # Get regions
        cursor.execute("SELECT DISTINCT applicable_region FROM schemes WHERE applicable_region IS NOT NULL")
        regions = [row[0] for row in cursor.fetchall()]
        regions = ['All'] + regions
        
        selected_region = st.selectbox("Region", regions, key="scheme_region_filter")
    
    with col3:
        # Status filter
        status_options = ['All', 'Active', 'Inactive']
        selected_status = st.selectbox("Status", status_options, key="scheme_status_filter")
    
    # Build query based on filters
    query = "SELECT * FROM schemes WHERE 1=1"
    params = []
    
    if selected_type != 'All':
        query += " AND scheme_type = ?"
        params.append(selected_type)
    
    if selected_region != 'All':
        query += " AND applicable_region = ?"
        params.append(selected_region)
    
    if selected_status != 'All':
        query += " AND deal_status = ?"
        params.append(selected_status)
    
    query += " ORDER BY upload_timestamp DESC"
    
    # Execute query
    cursor.execute(query, params)
    schemes = cursor.fetchall()
    
    # Display schemes
    st.markdown("<h2 class='sub-header'>Schemes</h2>", unsafe_allow_html=True)
    
    if schemes and len(schemes) > 0:
        for scheme in schemes:
            # Determine card class based on approval status
            card_class = "scheme-card"
            if scheme['approval_status'] == 'Pending':
                card_class += " approval-pending"
            elif scheme['approval_status'] == 'Approved':
                card_class += " approval-approved"
            elif scheme['approval_status'] == 'Rejected':
                card_class += " approval-rejected"
            
            st.markdown(f"<div class='{card_class}'>", unsafe_allow_html=True)
            
            # Scheme header
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                st.markdown(f"### {scheme['scheme_name']}")
            
            with col2:
                st.markdown(f"**Type:** {scheme['scheme_type'] or 'N/A'}")
            
            with col3:
                st.markdown(f"**Status:** {scheme['deal_status']}")
            
            # Scheme details
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Period:** {scheme['scheme_period_start']} to {scheme['scheme_period_end']}")
                st.markdown(f"**Region:** {scheme['applicable_region'] or 'All Regions'}")
            
            with col2:
                st.markdown(f"**Dealer Eligibility:** {scheme['dealer_type_eligibility'] or 'All Dealers'}")
                st.markdown(f"**Approval Status:** {scheme['approval_status']}")
            
            # Get products for this scheme
            cursor.execute("""
            SELECT p.product_name, sp.support_type, sp.payout_type, sp.payout_amount, sp.payout_unit, sp.free_item_description
            FROM scheme_products sp
            JOIN products p ON sp.product_id = p.product_id
            WHERE sp.scheme_id = ?
            """, (scheme['scheme_id'],))
            
            products = cursor.fetchall()
            
            if products and len(products) > 0:
                st.markdown("**Products:**")
                
                for product in products:
                    product_text = f"- {product['product_name']}: {product['support_type'] or 'Support'} - "
                    
                    if product['payout_type'] == 'Fixed':
                        product_text += f"₹{product['payout_amount']} {product['payout_unit'] or ''}"
                    elif product['payout_type'] == 'Percentage':
                        product_text += f"{product['payout_amount']}% {product['payout_unit'] or ''}"
                    else:
                        product_text += f"{product['payout_amount']} {product['payout_unit'] or ''}"
                    
                    # Add free item if available
                    if product['free_item_description']:
                        product_text += f" + <span class='highlight'>Free: {product['free_item_description']}</span>"
                    
                    st.markdown(product_text, unsafe_allow_html=True)
            
            # Actions
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                if st.button("View Details", key=f"view_{scheme['scheme_id']}"):
                    st.session_state.selected_scheme_id = scheme['scheme_id']
                    st.session_state.page = 'scheme_details'
            
            with col2:
                if st.button("Edit", key=f"edit_{scheme['scheme_id']}"):
                    st.session_state.edit_mode = True
                    st.session_state.edited_scheme = dict(scheme)
                    
                    # Get products for editing
                    cursor.execute("""
                    SELECT sp.*, p.product_name
                    FROM scheme_products sp
                    JOIN products p ON sp.product_id = p.product_id
                    WHERE sp.scheme_id = ?
                    """, (scheme['scheme_id'],))
                    
                    st.session_state.edited_products = [dict(p) for p in cursor.fetchall()]
                    st.session_state.page = 'edit_scheme'
            
            st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("No schemes found matching the selected filters.")
    
    conn.close()

# Scheme Details
def render_scheme_details():
    """Render detailed view of a selected scheme"""
    if not st.session_state.selected_scheme_id:
        st.error("No scheme selected. Please select a scheme from the Scheme Explorer.")
        if st.button("Back to Scheme Explorer"):
            st.session_state.page = 'schemes'
        return
    
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    # Get scheme details
    cursor.execute("SELECT * FROM schemes WHERE scheme_id = ?", (st.session_state.selected_scheme_id,))
    scheme = cursor.fetchone()
    
    if not scheme:
        st.error("Scheme not found. It may have been deleted.")
        if st.button("Back to Scheme Explorer"):
            st.session_state.page = 'schemes'
        conn.close()
        return
    
    # Display scheme header
    st.markdown(f"<h1 class='main-header'>{scheme['scheme_name']}</h1>", unsafe_allow_html=True)
    
    # Back button
    if st.button("← Back to Scheme Explorer"):
        st.session_state.page = 'schemes'
    
    # Scheme details
    st.markdown("<h2 class='sub-header'>Scheme Details</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown(f"**Scheme Type:** {scheme['scheme_type'] or 'N/A'}")
        st.markdown(f"**Period:** {scheme['scheme_period_start']} to {scheme['scheme_period_end']}")
        st.markdown(f"**Region:** {scheme['applicable_region'] or 'All Regions'}")
        st.markdown(f"**Dealer Eligibility:** {scheme['dealer_type_eligibility'] or 'All Dealers'}")
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown(f"**Status:** {scheme['deal_status']}")
        st.markdown(f"**Approval Status:** {scheme['approval_status']}")
        st.markdown(f"**Approved By:** {scheme['approved_by'] or 'N/A'}")
        st.markdown(f"**Upload Date:** {scheme['upload_timestamp']}")
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Get products for this scheme
    cursor.execute("""
    SELECT sp.*, p.product_name, p.product_category, p.ram, p.storage, p.color
    FROM scheme_products sp
    JOIN products p ON sp.product_id = p.product_id
    WHERE sp.scheme_id = ?
    """, (scheme['scheme_id'],))
    
    products = cursor.fetchall()
    
    # Display products
    st.markdown("<h2 class='sub-header'>Products</h2>", unsafe_allow_html=True)
    
    if products and len(products) > 0:
        for product in products:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Product:** {product['product_name']}")
                st.markdown(f"**Category:** {product['product_category'] or 'N/A'}")
                st.markdown(f"**Specifications:** {product['ram'] or 'N/A'} RAM, {product['storage'] or 'N/A'} Storage, {product['color'] or 'N/A'}")
            
            with col2:
                st.markdown(f"**Support Type:** {product['support_type'] or 'N/A'}")
                
                if product['payout_type'] == 'Fixed':
                    st.markdown(f"**Payout:** ₹{product['payout_amount']} {product['payout_unit'] or ''}")
                elif product['payout_type'] == 'Percentage':
                    st.markdown(f"**Payout:** {product['payout_amount']}% {product['payout_unit'] or ''}")
                else:
                    st.markdown(f"**Payout:** {product['payout_amount']} {product['payout_unit'] or ''}")
                
                st.markdown(f"**Dealer Contribution:** ₹{product['dealer_contribution']}")
                st.markdown(f"**Total Payout:** ₹{product['total_payout']}")
            
            # Display free item if available
            if product['free_item_description']:
                st.markdown("<div class='free-item-alert'>", unsafe_allow_html=True)
                st.markdown(f"**🎁 Free Item:** {product['free_item_description']}")
                st.markdown("</div>", unsafe_allow_html=True)
            
            # Display bundle offer details
            if product['is_bundle_offer'] == 1:
                st.markdown("<div class='highlight'>", unsafe_allow_html=True)
                st.markdown(f"**Bundle Offer:** Yes (Bundle Price: ₹{product['bundle_price']})")
                st.markdown("</div>", unsafe_allow_html=True)
            
            # Display upgrade offer details
            if product['is_upgrade_offer'] == 1:
                st.markdown("<div class='highlight'>", unsafe_allow_html=True)
                st.markdown("**Upgrade Offer:** Yes")
                st.markdown("</div>", unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("No products associated with this scheme.")
    
    # Get scheme rules
    cursor.execute("SELECT * FROM scheme_rules WHERE scheme_id = ?", (scheme['scheme_id'],))
    rules = cursor.fetchall()
    
    # Display rules
    if rules and len(rules) > 0:
        st.markdown("<h2 class='sub-header'>Scheme Rules</h2>", unsafe_allow_html=True)
        
        for rule in rules:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown(f"**Rule Type:** {rule['rule_type']}")
            st.markdown(f"**Description:** {rule['rule_description']}")
            st.markdown(f"**Value:** {rule['rule_value']}")
            st.markdown("</div>", unsafe_allow_html=True)
    
    # Get scheme parameters
    cursor.execute("SELECT * FROM scheme_parameters WHERE scheme_id = ?", (scheme['scheme_id'],))
    parameters = cursor.fetchall()
    
    # Display parameters
    if parameters and len(parameters) > 0:
        st.markdown("<h2 class='sub-header'>Scheme Parameters</h2>", unsafe_allow_html=True)
        
        for param in parameters:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown(f"**Parameter:** {param['parameter_name']}")
            st.markdown(f"**Description:** {param['parameter_description']}")
            st.markdown(f"**Criteria:** {param['parameter_criteria']}")
            st.markdown("</div>", unsafe_allow_html=True)
    
    # Get sales performance for this scheme
    cursor.execute("""
    SELECT p.product_name, COUNT(st.sale_id) as sales_count, SUM(st.earned_dealer_incentive_amount) as total_incentive
    FROM sales_transactions st
    JOIN products p ON st.product_id = p.product_id
    WHERE st.scheme_id = ?
    GROUP BY p.product_id
    ORDER BY total_incentive DESC
    """, (scheme['scheme_id'],))
    
    performance = cursor.fetchall()
    
    # Display performance
    if performance and len(performance) > 0:
        st.markdown("<h2 class='sub-header'>Performance</h2>", unsafe_allow_html=True)
        
        # Convert to DataFrame
        perf_df = pd.DataFrame(performance, columns=['Product', 'Sales Count', 'Total Incentive'])
        
        # Create bar chart
        fig = px.bar(
            perf_df,
            x='Product',
            y='Total Incentive',
            color='Sales Count',
            labels={'Total Incentive': 'Total Incentive Amount (₹)', 'Product': 'Product Name'},
            title='Product Performance in this Scheme',
            color_continuous_scale=px.colors.sequential.Blues
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            height=500,
            margin=dict(l=20, r=20, t=40, b=100)
        )
        
        st.plotly_chart(fig, use_container_width=True, key="scheme_product_performance")
    
    conn.close()

# Edit Scheme
def render_edit_scheme():
    """Render the scheme editing page"""
    if not st.session_state.edit_mode or not st.session_state.edited_scheme:
        st.error("No scheme selected for editing. Please select a scheme from the Scheme Explorer.")
        if st.button("Back to Scheme Explorer"):
            st.session_state.page = 'schemes'
        return
    
    scheme = st.session_state.edited_scheme
    
    # Display scheme header
    st.markdown(f"<h1 class='main-header'>Edit Scheme: {scheme['scheme_name']}</h1>", unsafe_allow_html=True)
    
    # Back button
    if st.button("← Cancel and Go Back"):
        st.session_state.edit_mode = False
        st.session_state.edited_scheme = None
        st.session_state.edited_products = None
        st.session_state.page = 'schemes'
        return
    
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    # Edit form
    st.markdown("<div class='edit-mode'>", unsafe_allow_html=True)
    st.markdown("<h2 class='sub-header'>Scheme Details</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        scheme['scheme_name'] = st.text_input("Scheme Name", scheme['scheme_name'])
        scheme['scheme_type'] = st.text_input("Scheme Type", scheme['scheme_type'] or "")
        scheme['scheme_period_start'] = st.date_input("Start Date", datetime.datetime.strptime(scheme['scheme_period_start'], "%Y-%m-%d") if scheme['scheme_period_start'] else datetime.datetime.now()).strftime("%Y-%m-%d")
        scheme['scheme_period_end'] = st.date_input("End Date", datetime.datetime.strptime(scheme['scheme_period_end'], "%Y-%m-%d") if scheme['scheme_period_end'] else (datetime.datetime.now() + datetime.timedelta(days=30))).strftime("%Y-%m-%d")
    
    with col2:
        scheme['applicable_region'] = st.text_input("Applicable Region", scheme['applicable_region'] or "")
        scheme['dealer_type_eligibility'] = st.text_input("Dealer Eligibility", scheme['dealer_type_eligibility'] or "")
        scheme['deal_status'] = st.selectbox("Status", ['Active', 'Inactive'], index=0 if scheme['deal_status'] == 'Active' else 1)
        scheme['notes'] = st.text_area("Notes", scheme['notes'] or "")
    
    # Edit products
    st.markdown("<h2 class='sub-header'>Products</h2>", unsafe_allow_html=True)
    
    if st.session_state.edited_products and len(st.session_state.edited_products) > 0:
        for i, product in enumerate(st.session_state.edited_products):
            st.markdown(f"<h3>Product: {product['product_name']}</h3>", unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                product['support_type'] = st.text_input("Support Type", product['support_type'] or "", key=f"support_type_{i}")
                product['payout_type'] = st.selectbox("Payout Type", ['Fixed', 'Percentage', 'Other'], index=0 if product['payout_type'] == 'Fixed' else (1 if product['payout_type'] == 'Percentage' else 2), key=f"payout_type_{i}")
                product['payout_amount'] = st.number_input("Payout Amount", min_value=0.0, value=float(product['payout_amount'] or 0), key=f"payout_amount_{i}")
                product['payout_unit'] = st.text_input("Payout Unit", product['payout_unit'] or "", key=f"payout_unit_{i}")
            
            with col2:
                product['dealer_contribution'] = st.number_input("Dealer Contribution", min_value=0.0, value=float(product['dealer_contribution'] or 0), key=f"dealer_contribution_{i}")
                product['total_payout'] = st.number_input("Total Payout", min_value=0.0, value=float(product['total_payout'] or 0), key=f"total_payout_{i}")
                product['is_bundle_offer'] = 1 if st.checkbox("Bundle Offer", product['is_bundle_offer'] == 1, key=f"is_bundle_{i}") else 0
                
                if product['is_bundle_offer'] == 1:
                    product['bundle_price'] = st.number_input("Bundle Price", min_value=0.0, value=float(product['bundle_price'] or 0), key=f"bundle_price_{i}")
            
            # Free item section
            product['free_item_description'] = st.text_input("Free Item Description (if any)", product['free_item_description'] or "", key=f"free_item_{i}")
            
            st.markdown("---")
    else:
        st.info("No products associated with this scheme.")
    
    # Submit button
    if st.button("Submit for Approval"):
        # Update scheme in database
        try:
            # First, create an approval request
            cursor.execute("""
            INSERT INTO scheme_approvals (scheme_id, requested_by, approval_status, approval_notes)
            VALUES (?, ?, ?, ?)
            """, (scheme['scheme_id'], "Current User", "Pending", "Edit request"))
            
            # Update scheme with edited values
            cursor.execute("""
            UPDATE schemes
            SET scheme_name = ?, scheme_type = ?, scheme_period_start = ?, scheme_period_end = ?,
                applicable_region = ?, dealer_type_eligibility = ?, deal_status = ?, notes = ?,
                approval_status = ?, last_modified = CURRENT_TIMESTAMP
            WHERE scheme_id = ?
            """, (
                scheme['scheme_name'], scheme['scheme_type'], scheme['scheme_period_start'], scheme['scheme_period_end'],
                scheme['applicable_region'], scheme['dealer_type_eligibility'], scheme['deal_status'], scheme['notes'],
                "Pending", scheme['scheme_id']
            ))
            
            # Update products
            if st.session_state.edited_products:
                for product in st.session_state.edited_products:
                    cursor.execute("""
                    UPDATE scheme_products
                    SET support_type = ?, payout_type = ?, payout_amount = ?, payout_unit = ?,
                        dealer_contribution = ?, total_payout = ?, is_bundle_offer = ?, bundle_price = ?,
                        free_item_description = ?, last_modified = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """, (
                        product['support_type'], product['payout_type'], product['payout_amount'], product['payout_unit'],
                        product['dealer_contribution'], product['total_payout'], product['is_bundle_offer'], product['bundle_price'],
                        product['free_item_description'], product['id']
                    ))
            
            conn.commit()
            st.success("Scheme updated successfully and submitted for approval.")
            
            # Reset edit mode
            st.session_state.edit_mode = False
            st.session_state.edited_scheme = None
            st.session_state.edited_products = None
            
            # Redirect to schemes page
            st.session_state.page = 'schemes'
            st.rerun()
        
        except Exception as e:
            conn.rollback()
            st.error(f"Error updating scheme: {str(e)}")
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    conn.close()

# Products
def render_products():
    """Render the products page"""
    st.markdown("<h1 class='main-header'>Products</h1>", unsafe_allow_html=True)
    
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    # Filters
    st.markdown("<h2 class='sub-header'>Filters</h2>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Get product categories
        cursor.execute("SELECT DISTINCT product_category FROM products WHERE product_category IS NOT NULL")
        categories = [row[0] for row in cursor.fetchall()]
        categories = ['All'] + categories
        
        selected_category = st.selectbox("Category", categories, key="product_category_filter")
    
    with col2:
        # Get RAM options
        cursor.execute("SELECT DISTINCT ram FROM products WHERE ram IS NOT NULL")
        ram_options = [row[0] for row in cursor.fetchall()]
        ram_options = ['All'] + ram_options
        
        selected_ram = st.selectbox("RAM", ram_options, key="product_ram_filter")
    
    with col3:
        # Get storage options
        cursor.execute("SELECT DISTINCT storage FROM products WHERE storage IS NOT NULL")
        storage_options = [row[0] for row in cursor.fetchall()]
        storage_options = ['All'] + storage_options
        
        selected_storage = st.selectbox("Storage", storage_options, key="product_storage_filter")
    
    # Build query based on filters
    query = "SELECT * FROM products WHERE is_active = 1"
    params = []
    
    if selected_category != 'All':
        query += " AND product_category = ?"
        params.append(selected_category)
    
    if selected_ram != 'All':
        query += " AND ram = ?"
        params.append(selected_ram)
    
    if selected_storage != 'All':
        query += " AND storage = ?"
        params.append(selected_storage)
    
    query += " ORDER BY product_name"
    
    # Execute query
    cursor.execute(query, params)
    products = cursor.fetchall()
    
    # Display products
    st.markdown("<h2 class='sub-header'>Products</h2>", unsafe_allow_html=True)
    
    if products and len(products) > 0:
        # Convert to DataFrame
        product_df = pd.DataFrame([dict(p) for p in products])
        
        # Select columns to display
        display_columns = [
            'product_name', 'product_code', 'product_category', 'product_subcategory',
            'ram', 'storage', 'connectivity', 'color', 'dealer_price_dp', 'mrp'
        ]
        
        # Filter columns that exist in the DataFrame
        display_columns = [col for col in display_columns if col in product_df.columns]
        
        # Rename columns for display
        rename_map = {
            'product_name': 'Product Name',
            'product_code': 'Product Code',
            'product_category': 'Category',
            'product_subcategory': 'Subcategory',
            'ram': 'RAM',
            'storage': 'Storage',
            'connectivity': 'Connectivity',
            'color': 'Color',
            'dealer_price_dp': 'Dealer Price',
            'mrp': 'MRP'
        }
        
        # Create display DataFrame
        display_df = product_df[display_columns].rename(columns=rename_map)
        
        # Display as table
        st.markdown("<div class='table-container'>", unsafe_allow_html=True)
        st.dataframe(display_df, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Product performance visualization
        st.markdown("<h2 class='sub-header'>Product Performance</h2>", unsafe_allow_html=True)
        
        try:
            # Get product sales data
            cursor.execute("""
            SELECT p.product_name, COUNT(st.sale_id) as sales_count, SUM(st.earned_dealer_incentive_amount) as total_incentive
            FROM products p
            LEFT JOIN sales_transactions st ON p.product_id = st.product_id
            WHERE p.is_active = 1
            GROUP BY p.product_id
            ORDER BY total_incentive DESC
            LIMIT 15
            """)
            
            product_performance = cursor.fetchall()
            
            if product_performance and len(product_performance) > 0:
                # Convert to DataFrame
                perf_df = pd.DataFrame(product_performance, columns=['Product', 'Sales Count', 'Total Incentive'])
                perf_df = perf_df.fillna(0)  # Replace NaN with 0
                
                # Create bar chart
                fig = px.bar(
                    perf_df,
                    x='Product',
                    y='Total Incentive',
                    color='Sales Count',
                    labels={'Total Incentive': 'Total Incentive Amount (₹)', 'Product': 'Product Name'},
                    title='Top Performing Products by Incentive Amount',
                    color_continuous_scale=px.colors.sequential.Blues
                )
                
                fig.update_layout(
                    xaxis_tickangle=-45,
                    height=500,
                    margin=dict(l=20, r=20, t=40, b=100)
                )
                
                st.plotly_chart(fig, use_container_width=True, key="product_performance_chart")
            else:
                st.info("No product performance data available yet.")
        except Exception as e:
            st.error(f"Error generating product performance chart: {str(e)}")
    else:
        st.info("No products found matching the selected filters.")
    
    conn.close()

# Dealers
def render_dealers():
    """Render the dealers page"""
    st.markdown("<h1 class='main-header'>Dealers</h1>", unsafe_allow_html=True)
    
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    # Filters
    st.markdown("<h2 class='sub-header'>Filters</h2>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Get dealer types
        cursor.execute("SELECT DISTINCT dealer_type FROM dealers WHERE dealer_type IS NOT NULL")
        dealer_types = [row[0] for row in cursor.fetchall()]
        dealer_types = ['All'] + dealer_types
        
        selected_type = st.selectbox("Dealer Type", dealer_types, key="dealer_type_filter")
    
    with col2:
        # Get regions
        cursor.execute("SELECT DISTINCT region FROM dealers WHERE region IS NOT NULL")
        regions = [row[0] for row in cursor.fetchall()]
        regions = ['All'] + regions
        
        selected_region = st.selectbox("Region", regions, key="dealer_region_filter")
    
    with col3:
        # Get states
        cursor.execute("SELECT DISTINCT state FROM dealers WHERE state IS NOT NULL")
        states = [row[0] for row in cursor.fetchall()]
        states = ['All'] + states
        
        selected_state = st.selectbox("State", states, key="dealer_state_filter")
    
    # Build query based on filters
    query = "SELECT * FROM dealers WHERE is_active = 1"
    params = []
    
    if selected_type != 'All':
        query += " AND dealer_type = ?"
        params.append(selected_type)
    
    if selected_region != 'All':
        query += " AND region = ?"
        params.append(selected_region)
    
    if selected_state != 'All':
        query += " AND state = ?"
        params.append(selected_state)
    
    query += " ORDER BY dealer_name"
    
    # Execute query
    cursor.execute(query, params)
    dealers = cursor.fetchall()
    
    # Display dealers
    st.markdown("<h2 class='sub-header'>Dealers</h2>", unsafe_allow_html=True)
    
    if dealers and len(dealers) > 0:
        # Convert to DataFrame
        dealer_df = pd.DataFrame([dict(d) for d in dealers])
        
        # Select columns to display
        display_columns = [
            'dealer_name', 'dealer_code', 'dealer_type', 'region', 'state', 'city',
            'contact_person', 'contact_email', 'contact_phone'
        ]
        
        # Filter columns that exist in the DataFrame
        display_columns = [col for col in display_columns if col in dealer_df.columns]
        
        # Rename columns for display
        rename_map = {
            'dealer_name': 'Dealer Name',
            'dealer_code': 'Dealer Code',
            'dealer_type': 'Type',
            'region': 'Region',
            'state': 'State',
            'city': 'City',
            'contact_person': 'Contact Person',
            'contact_email': 'Email',
            'contact_phone': 'Phone'
        }
        
        # Create display DataFrame
        display_df = dealer_df[display_columns].rename(columns=rename_map)
        
        # Display as table
        st.markdown("<div class='table-container'>", unsafe_allow_html=True)
        st.dataframe(display_df, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Dealer performance visualization
        st.markdown("<h2 class='sub-header'>Dealer Performance</h2>", unsafe_allow_html=True)
        
        try:
            # Get dealer sales data
            cursor.execute("""
            SELECT d.dealer_name, COUNT(st.sale_id) as sales_count, SUM(st.earned_dealer_incentive_amount) as total_incentive
            FROM dealers d
            LEFT JOIN sales_transactions st ON d.dealer_id = st.dealer_id
            WHERE d.is_active = 1
            GROUP BY d.dealer_id
            ORDER BY total_incentive DESC
            LIMIT 15
            """)
            
            dealer_performance = cursor.fetchall()
            
            if dealer_performance and len(dealer_performance) > 0:
                # Convert to DataFrame
                perf_df = pd.DataFrame(dealer_performance, columns=['Dealer', 'Sales Count', 'Total Incentive'])
                perf_df = perf_df.fillna(0)  # Replace NaN with 0
                
                # Create bar chart
                fig = px.bar(
                    perf_df,
                    x='Dealer',
                    y='Total Incentive',
                    color='Sales Count',
                    labels={'Total Incentive': 'Total Incentive Amount (₹)', 'Dealer': 'Dealer Name'},
                    title='Top Performing Dealers by Incentive Amount',
                    color_continuous_scale=px.colors.sequential.Blues
                )
                
                fig.update_layout(
                    xaxis_tickangle=-45,
                    height=500,
                    margin=dict(l=20, r=20, t=40, b=100)
                )
                
                st.plotly_chart(fig, use_container_width=True, key="dealer_performance_chart")
            else:
                st.info("No dealer performance data available yet.")
        except Exception as e:
            st.error(f"Error generating dealer performance chart: {str(e)}")
    else:
        st.info("No dealers found matching the selected filters.")
    
    conn.close()

# Sales Simulation
def render_simulate_sales():
    """Render the sales simulation page"""
    st.markdown("<h1 class='main-header'>Sales Simulation</h1>", unsafe_allow_html=True)
    
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    # Get active schemes
    cursor.execute("SELECT scheme_id, scheme_name FROM schemes WHERE deal_status = 'Active' LIMIT 10")
    schemes = cursor.fetchall()
    
    if not schemes or len(schemes) == 0:
        st.error("No active schemes found. Please add schemes first.")
        conn.close()
        return
    
    # Get active dealers
    cursor.execute("SELECT dealer_id, dealer_name FROM dealers WHERE is_active = 1")
    dealers = cursor.fetchall()
    
    if not dealers or len(dealers) == 0:
        st.error("No active dealers found. Please add dealers first.")
        conn.close()
        return
    
    # Simulation form
    st.markdown("<h2 class='sub-header'>Simulation Parameters</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Select scheme
        scheme_options = {scheme['scheme_id']: scheme['scheme_name'] for scheme in schemes}
        selected_scheme_id = st.selectbox("Select Scheme", list(scheme_options.keys()), format_func=lambda x: scheme_options[x], key="sim_scheme")
        
        # Get products for selected scheme
        cursor.execute("""
        SELECT p.product_id, p.product_name, p.dealer_price_dp, sp.payout_amount, sp.payout_type, sp.free_item_description
        FROM products p
        JOIN scheme_products sp ON p.product_id = sp.product_id
        WHERE sp.scheme_id = ? AND p.is_active = 1
        """, (selected_scheme_id,))
        
        products = cursor.fetchall()
        
        if not products or len(products) == 0:
            st.error("No products found for the selected scheme.")
            conn.close()
            return
        
        # Select product
        product_options = {product['product_id']: product['product_name'] for product in products}
        selected_product_id = st.selectbox("Select Product", list(product_options.keys()), format_func=lambda x: product_options[x], key="sim_product")
        
        # Get selected product details
        selected_product = None
        for product in products:
            if product['product_id'] == selected_product_id:
                selected_product = product
                break
        
        # Quantity
        quantity = st.number_input("Quantity", min_value=1, value=1, key="sim_quantity")
    
    with col2:
        # Select dealer
        dealer_options = {dealer['dealer_id']: dealer['dealer_name'] for dealer in dealers}
        selected_dealer_id = st.selectbox("Select Dealer", list(dealer_options.keys()), format_func=lambda x: dealer_options[x], key="sim_dealer")
        
        # Sale date
        sale_date = st.date_input("Sale Date", datetime.datetime.now(), key="sim_date")
        
        # IMEI/Serial
        imei_serial = st.text_input("IMEI/Serial Number (Optional)", key="sim_imei")
    
    # Simulate button
    if st.button("Simulate Sale"):
        if selected_product is None:
            st.error("Error: Selected product not found.")
            conn.close()
            return
        
        try:
            # Calculate dealer price and incentive
            # Use safe dictionary access with fallback values
            try:
                dealer_price = float(selected_product['dealer_price_dp']) if selected_product['dealer_price_dp'] is not None else 10000.0
            except (TypeError, KeyError):
                dealer_price = 10000.0  # Default value
            
            try:
                payout_amount = float(selected_product['payout_amount']) if selected_product['payout_amount'] is not None else 0.0
            except (TypeError, KeyError):
                payout_amount = 0.0  # Default value
            
            try:
                payout_type = selected_product['payout_type'] if selected_product['payout_type'] is not None else 'Fixed'
            except (TypeError, KeyError):
                payout_type = 'Fixed'  # Default value
            
            # Calculate incentive based on payout type
            if payout_type == 'Percentage':
                incentive_amount = (dealer_price * payout_amount / 100) * quantity
            else:  # Fixed or other
                incentive_amount = payout_amount * quantity
            
            # Calculate total dealer price
            total_dealer_price = dealer_price * quantity
            
            # Display results
            st.markdown("<h2 class='sub-header'>Simulation Results</h2>", unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                st.markdown(f"**Dealer:** {dealer_options[selected_dealer_id]}")
                st.markdown(f"**Scheme:** {scheme_options[selected_scheme_id]}")
                st.markdown(f"**Product:** {product_options[selected_product_id]}")
                st.markdown(f"**Quantity:** {quantity}")
                st.markdown(f"**Sale Date:** {sale_date}")
                st.markdown("</div>", unsafe_allow_html=True)
            
            with col2:
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                st.markdown(f"**Dealer Price:** ₹{dealer_price:,.2f}")
                st.markdown(f"**Total Dealer Price:** ₹{total_dealer_price:,.2f}")
                st.markdown(f"**Incentive Type:** {payout_type}")
                st.markdown(f"**Incentive Amount:** ₹{incentive_amount:,.2f}")
                st.markdown("</div>", unsafe_allow_html=True)
            
            # Check for free items and display customer prompt
            try:
                free_item = selected_product['free_item_description'] if selected_product['free_item_description'] is not None else None
            except (TypeError, KeyError):
                free_item = None
            
            if free_item:
                st.markdown("<div class='free-item-alert'>", unsafe_allow_html=True)
                st.markdown("### 🎁 Free Item Included!")
                st.markdown(f"**Free Item:** {free_item}")
                
                # Customer prompt section
                st.markdown("### 📣 Suggested Customer Prompt")
                st.markdown(f"""
                *"I'd like to inform you that with your purchase of {product_options[selected_product_id]}, 
                you'll receive a {free_item} absolutely free! This is part of our special 
                {scheme_options[selected_scheme_id]} promotion running right now."*
                """)
                st.markdown("</div>", unsafe_allow_html=True)
            
            # Record sale button
            if st.button("Record This Sale"):
                try:
                    # Insert into sales_transactions
                    cursor.execute("""
                    INSERT INTO sales_transactions (
                        dealer_id, scheme_id, product_id, quantity_sold, dealer_price_dp,
                        earned_dealer_incentive_amount, imei_serial, sale_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        selected_dealer_id, selected_scheme_id, selected_product_id, quantity,
                        dealer_price, incentive_amount, imei_serial, sale_date.strftime("%Y-%m-%d %H:%M:%S")
                    ))
                    
                    conn.commit()
                    st.success("Sale recorded successfully!")
                except Exception as e:
                    conn.rollback()
                    st.error(f"Error recording sale: {str(e)}")
        
        except Exception as e:
            st.error(f"Error simulating sale: {str(e)}")
    
    # Historical sales simulation
    st.markdown("<h2 class='sub-header'>Historical Sales Analysis</h2>", unsafe_allow_html=True)
    
    try:
        # Get historical sales data
        cursor.execute("""
        SELECT st.sale_id, d.dealer_name, p.product_name, s.scheme_name, 
               st.quantity_sold, st.earned_dealer_incentive_amount, st.sale_timestamp
        FROM sales_transactions st
        JOIN dealers d ON st.dealer_id = d.dealer_id
        JOIN products p ON st.product_id = p.product_id
        JOIN schemes s ON st.scheme_id = s.scheme_id
        ORDER BY st.sale_timestamp DESC
        LIMIT 20
        """)
        
        sales = cursor.fetchall()
        
        if sales and len(sales) > 0:
            # Convert to DataFrame
            sales_df = pd.DataFrame([dict(s) for s in sales])
            
            # Select columns to display
            display_columns = [
                'dealer_name', 'product_name', 'scheme_name', 'quantity_sold',
                'earned_dealer_incentive_amount', 'sale_timestamp'
            ]
            
            # Filter columns that exist in the DataFrame
            display_columns = [col for col in display_columns if col in sales_df.columns]
            
            # Rename columns for display
            rename_map = {
                'dealer_name': 'Dealer',
                'product_name': 'Product',
                'scheme_name': 'Scheme',
                'quantity_sold': 'Quantity',
                'earned_dealer_incentive_amount': 'Incentive Amount',
                'sale_timestamp': 'Sale Date'
            }
            
            # Create display DataFrame
            display_df = sales_df[display_columns].rename(columns=rename_map)
            
            # Display as table
            st.markdown("<div class='table-container'>", unsafe_allow_html=True)
            st.dataframe(display_df, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Sales trend visualization
            st.markdown("<h3>Sales Trend</h3>", unsafe_allow_html=True)
            
            # Convert timestamp to datetime
            sales_df['sale_timestamp'] = pd.to_datetime(sales_df['sale_timestamp'])
            
            # Group by date
            sales_df['date'] = sales_df['sale_timestamp'].dt.date
            trend_df = sales_df.groupby('date').agg({
                'earned_dealer_incentive_amount': 'sum',
                'sale_id': 'count'
            }).reset_index()
            
            trend_df.columns = ['Date', 'Incentive Amount', 'Sales Count']
            
            # Create line chart
            fig = px.line(
                trend_df,
                x='Date',
                y=['Incentive Amount', 'Sales Count'],
                labels={'value': 'Value', 'variable': 'Metric'},
                title='Sales Trend Over Time',
                color_discrete_sequence=['#1976D2', '#FFC107']
            )
            
            fig.update_layout(
                height=400,
                margin=dict(l=20, r=20, t=40, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True, key="sales_trend_chart")
        else:
            st.info("No sales data available yet.")
    except Exception as e:
        st.error(f"Error retrieving sales data: {str(e)}")
    
    conn.close()

# Upload Scheme
def render_upload_scheme():
    """Render the scheme upload page"""
    st.markdown("<h1 class='main-header'>Upload Scheme</h1>", unsafe_allow_html=True)
    
    # Load secrets
    secrets = load_secrets()
    
    # Initialize AWS clients
    bedrock_client, textract_client = initialize_aws_clients(secrets)
    
    # Upload form
    st.markdown("<h2 class='sub-header'>Upload Scheme Document</h2>", unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("Choose a PDF file", type="pdf")
    
    if uploaded_file is not None:
        # Save uploaded file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        uploads_dir = os.path.join(current_dir, 'uploads')
        os.makedirs(uploads_dir, exist_ok=True)
        
        file_path = os.path.join(uploads_dir, uploaded_file.name)
        
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.success(f"File uploaded successfully: {uploaded_file.name}")
        
        # Process PDF
        with st.spinner("Extracting text from PDF..."):
            pages_text = extract_text_from_pdf(file_path, textract_client)
            
            if not pages_text or len(pages_text) == 0:
                st.error("Failed to extract text from the PDF. Please try another file.")
                return
            
            # Combine all pages text
            all_text = "\n".join([page[1] for page in pages_text])
            
            # Save extracted text
            raw_texts_dir = os.path.join(current_dir, 'raw_texts')
            os.makedirs(raw_texts_dir, exist_ok=True)
            
            text_file_name = os.path.splitext(uploaded_file.name)[0] + "_raw.txt"
            text_file_path = os.path.join(raw_texts_dir, text_file_name)
            
            with open(text_file_path, "w", encoding="utf-8") as f:
                f.write(all_text)
        
        # Extract structured data
        with st.spinner("Extracting structured data..."):
            inference_profile_arn = secrets.get('INFERENCE_PROFILE_CLAUDE')
            
            structured_data = extract_structured_data_from_text(
                all_text, uploaded_file.name, bedrock_client, inference_profile_arn
            )
            
            if not structured_data:
                st.error("Failed to extract structured data from the PDF. Please try another file.")
                return
        
        # Display extracted data
        st.markdown("<h2 class='sub-header'>Extracted Scheme Data</h2>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown(f"**Scheme Name:** {structured_data.get('scheme_name', 'N/A')}")
            st.markdown(f"**Scheme Type:** {structured_data.get('scheme_type', 'N/A')}")
            st.markdown(f"**Period:** {structured_data.get('scheme_period_start', 'N/A')} to {structured_data.get('scheme_period_end', 'N/A')}")
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown(f"**Region:** {structured_data.get('applicable_region', 'All Regions')}")
            st.markdown(f"**Dealer Eligibility:** {structured_data.get('dealer_type_eligibility', 'All Dealers')}")
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Display products
        if 'products' in structured_data and structured_data['products']:
            st.markdown("<h3>Products</h3>", unsafe_allow_html=True)
            
            for product in structured_data['products']:
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**Product:** {product.get('product_name', 'N/A')}")
                    st.markdown(f"**Category:** {product.get('product_category', 'N/A')}")
                    st.markdown(f"**Specifications:** {product.get('ram', 'N/A')} RAM, {product.get('storage', 'N/A')} Storage")
                
                with col2:
                    st.markdown(f"**Support Type:** {product.get('support_type', 'N/A')}")
                    
                    if product.get('payout_type') == 'Fixed':
                        st.markdown(f"**Payout:** ₹{product.get('payout_amount', 0)} {product.get('payout_unit', '')}")
                    elif product.get('payout_type') == 'Percentage':
                        st.markdown(f"**Payout:** {product.get('payout_amount', 0)}% {product.get('payout_unit', '')}")
                    else:
                        st.markdown(f"**Payout:** {product.get('payout_amount', 0)} {product.get('payout_unit', '')}")
                
                # Display free item if available
                if product.get('free_item_description'):
                    st.markdown("<div class='free-item-alert'>", unsafe_allow_html=True)
                    st.markdown(f"**🎁 Free Item:** {product.get('free_item_description')}")
                    st.markdown("</div>", unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)
        
        # Display rules
        if 'scheme_rules' in structured_data and structured_data['scheme_rules']:
            st.markdown("<h3>Rules</h3>", unsafe_allow_html=True)
            
            for rule in structured_data['scheme_rules']:
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                st.markdown(f"**Rule Type:** {rule.get('rule_type', 'N/A')}")
                st.markdown(f"**Description:** {rule.get('rule_description', 'N/A')}")
                st.markdown(f"**Value:** {rule.get('rule_value', 'N/A')}")
                st.markdown("</div>", unsafe_allow_html=True)
        
        # Save to database button
        if st.button("Save to Database"):
            try:
                # Connect to database
                conn = connect_db()
                cursor = conn.cursor()
                
                # Insert scheme
                cursor.execute("""
                INSERT INTO schemes (
                    scheme_name, scheme_type, scheme_period_start, scheme_period_end,
                    applicable_region, dealer_type_eligibility, scheme_document_name,
                    raw_extracted_text_path, deal_status, approval_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    structured_data.get('scheme_name', 'Unnamed Scheme'),
                    structured_data.get('scheme_type', None),
                    structured_data.get('scheme_period_start', '2023-01-01'),
                    structured_data.get('scheme_period_end', '2023-12-31'),
                    structured_data.get('applicable_region', None),
                    structured_data.get('dealer_type_eligibility', None),
                    uploaded_file.name,
                    text_file_path,
                    'Active',
                    'Pending'
                ))
                
                scheme_id = cursor.lastrowid
                
                # Insert products
                if 'products' in structured_data and structured_data['products']:
                    for product_data in structured_data['products']:
                        # Check if product exists
                        cursor.execute(
                            "SELECT product_id FROM products WHERE product_name = ?",
                            (product_data.get('product_name', 'Unnamed Product'),)
                        )
                        
                        product_result = cursor.fetchone()
                        
                        if product_result:
                            product_id = product_result[0]
                        else:
                            # Insert new product
                            cursor.execute("""
                            INSERT INTO products (
                                product_name, product_code, product_category, product_subcategory,
                                ram, storage, connectivity, color, dealer_price_dp, mrp
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                product_data.get('product_name', 'Unnamed Product'),
                                product_data.get('product_code', None),
                                product_data.get('product_category', None),
                                product_data.get('product_subcategory', None),
                                product_data.get('ram', None),
                                product_data.get('storage', None),
                                product_data.get('connectivity', None),
                                product_data.get('color', None),
                                10000,  # Default dealer price
                                12000   # Default MRP
                            ))
                            
                            product_id = cursor.lastrowid
                        
                        # Insert scheme_product relationship
                        cursor.execute("""
                        INSERT INTO scheme_products (
                            scheme_id, product_id, support_type, payout_type, payout_amount,
                            payout_unit, dealer_contribution, total_payout, is_bundle_offer,
                            bundle_price, is_upgrade_offer, free_item_description
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            scheme_id,
                            product_id,
                            product_data.get('support_type', None),
                            product_data.get('payout_type', 'Fixed'),
                            product_data.get('payout_amount', 0),
                            product_data.get('payout_unit', None),
                            product_data.get('dealer_contribution', 0),
                            product_data.get('total_payout', product_data.get('payout_amount', 0)),
                            1 if product_data.get('is_bundle_offer', False) else 0,
                            product_data.get('bundle_price', 0),
                            1 if product_data.get('is_upgrade_offer', False) else 0,
                            product_data.get('free_item_description', None)
                        ))
                
                # Insert rules
                if 'scheme_rules' in structured_data and structured_data['scheme_rules']:
                    for rule_data in structured_data['scheme_rules']:
                        cursor.execute("""
                        INSERT INTO scheme_rules (
                            scheme_id, rule_type, rule_description, rule_value
                        ) VALUES (?, ?, ?, ?)
                        """, (
                            scheme_id,
                            rule_data.get('rule_type', None),
                            rule_data.get('rule_description', None),
                            rule_data.get('rule_value', None)
                        ))
                
                conn.commit()
                st.success("Scheme saved to database successfully!")
                
                # Redirect to scheme explorer
                st.session_state.page = 'schemes'
                st.rerun()
            
            except Exception as e:
                if 'conn' in locals():
                    conn.rollback()
                st.error(f"Error saving scheme to database: {str(e)}")
            
            finally:
                if 'conn' in locals():
                    conn.close()

# Approvals
def render_approvals():
    """Render the approvals page"""
    st.markdown("<h1 class='main-header'>Pending Approvals</h1>", unsafe_allow_html=True)
    
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    # Get pending approvals
    cursor.execute("""
    SELECT sa.approval_id, s.scheme_id, s.scheme_name, sa.requested_by, sa.approval_notes, sa.approval_status
    FROM scheme_approvals sa
    JOIN schemes s ON sa.scheme_id = s.scheme_id
    WHERE sa.approval_status = 'Pending'
    ORDER BY sa.approval_id DESC
    """)
    
    approvals = cursor.fetchall()
    
    if approvals and len(approvals) > 0:
        for approval in approvals:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"### Scheme: {approval['scheme_name']}")
                st.markdown(f"**Requested By:** {approval['requested_by']}")
                st.markdown(f"**Notes:** {approval['approval_notes']}")
            
            with col2:
                st.markdown(f"**Status:** {approval['approval_status']}")
                
                # Get scheme details
                cursor.execute("SELECT * FROM schemes WHERE scheme_id = ?", (approval['scheme_id'],))
                scheme = cursor.fetchone()
                
                if scheme:
                    if st.button("View Details", key=f"view_approval_{approval['approval_id']}"):
                        st.session_state.selected_scheme_id = approval['scheme_id']
                        st.session_state.page = 'scheme_details'
            
            # Approval actions
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("Approve", key=f"approve_{approval['approval_id']}"):
                    try:
                        # Update approval status
                        cursor.execute("""
                        UPDATE scheme_approvals
                        SET approval_status = 'Approved', approved_by = ?, approved_at = CURRENT_TIMESTAMP
                        WHERE approval_id = ?
                        """, ("Current User", approval['approval_id']))
                        
                        # Update scheme approval status
                        cursor.execute("""
                        UPDATE schemes
                        SET approval_status = 'Approved', approved_by = ?, approval_timestamp = CURRENT_TIMESTAMP
                        WHERE scheme_id = ?
                        """, ("Current User", approval['scheme_id']))
                        
                        conn.commit()
                        st.success("Scheme approved successfully!")
                        st.rerun()
                    
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error approving scheme: {str(e)}")
            
            with col2:
                if st.button("Reject", key=f"reject_{approval['approval_id']}"):
                    try:
                        # Update approval status
                        cursor.execute("""
                        UPDATE scheme_approvals
                        SET approval_status = 'Rejected', approved_by = ?, approved_at = CURRENT_TIMESTAMP
                        WHERE approval_id = ?
                        """, ("Current User", approval['approval_id']))
                        
                        # Update scheme approval status
                        cursor.execute("""
                        UPDATE schemes
                        SET approval_status = 'Rejected'
                        WHERE scheme_id = ?
                        """, (approval['scheme_id'],))
                        
                        conn.commit()
                        st.success("Scheme rejected successfully!")
                        st.rerun()
                    
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error rejecting scheme: {str(e)}")
            
            st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("No pending approvals found.")
    
    conn.close()

# Settings
def render_settings():
    """Render the settings page"""
    st.markdown("<h1 class='main-header'>Settings</h1>", unsafe_allow_html=True)
    
    # Load current secrets
    secrets = load_secrets()
    
    # AWS settings
    st.markdown("<h2 class='sub-header'>AWS Settings</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        aws_access_key = st.text_input("AWS Access Key ID", secrets.get('aws_access_key_id', ''))
        aws_secret_key = st.text_input("AWS Secret Access Key", secrets.get('aws_secret_access_key', ''), type="password")
        region = st.text_input("AWS Region", secrets.get('REGION', 'ap-south-1'))
    
    with col2:
        inference_profile = st.text_input("Claude Inference Profile ARN", secrets.get('INFERENCE_PROFILE_CLAUDE', ''))
        s3_bucket = st.text_input("S3 Bucket Name", secrets.get('s3_bucket_name', ''))
        container_name = st.text_input("Container Name", secrets.get('container_name', ''))
    
    # API settings
    st.markdown("<h2 class='sub-header'>API Settings</h2>", unsafe_allow_html=True)
    
    tavily_api = st.text_input("Tavily API Key", secrets.get('TAVILY_API', ''), type="password")
    
    # Save settings
    if st.button("Save Settings"):
        try:
            # Update secrets
            updated_secrets = {
                "aws_access_key_id": aws_access_key,
                "aws_secret_access_key": aws_secret_key,
                "INFERENCE_PROFILE_CLAUDE": inference_profile,
                "REGION": region,
                "TAVILY_API": tavily_api,
                "FAISS_INDEX_PATH": secrets.get('FAISS_INDEX_PATH', 'faiss_index.bin'),
                "METADATA_STORE_PATH": secrets.get('METADATA_STORE_PATH', 'metadata_store.pkl'),
                "s3_bucket_name": s3_bucket,
                "container_name": container_name
            }
            
            # Save to file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            secrets_path = os.path.join(current_dir, 'secrets.json')
            
            with open(secrets_path, 'w') as f:
                json.dump(updated_secrets, f, indent=4)
            
            st.success("Settings saved successfully!")
        
        except Exception as e:
            st.error(f"Error saving settings: {str(e)}")
    
    # Database management
    st.markdown("<h2 class='sub-header'>Database Management</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Backup Database"):
            try:
                # Create backup
                current_dir = os.path.dirname(os.path.abspath(__file__))
                db_path = os.path.join(current_dir, 'dns_database.db')
                backup_path = os.path.join(current_dir, f'dns_database_backup_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.db')
                
                shutil.copy2(db_path, backup_path)
                st.success(f"Database backed up successfully to {backup_path}")
            
            except Exception as e:
                st.error(f"Error backing up database: {str(e)}")
    
    with col2:
        if st.button("Reset Sample Data"):
            try:
                # Connect to database
                conn = connect_db()
                cursor = conn.cursor()
                
                # Clear existing data
                cursor.execute("DELETE FROM sales_transactions")
                cursor.execute("DELETE FROM scheme_products")
                cursor.execute("DELETE FROM scheme_rules")
                cursor.execute("DELETE FROM scheme_parameters")
                cursor.execute("DELETE FROM scheme_approvals")
                cursor.execute("DELETE FROM bundle_offers")
                cursor.execute("DELETE FROM schemes")
                cursor.execute("DELETE FROM products")
                cursor.execute("DELETE FROM dealers")
                
                conn.commit()
                conn.close()
                
                # Add sample data
                from pdf_processor_fixed import add_sample_data
                add_sample_data()
                
                st.success("Sample data reset successfully!")
            
            except Exception as e:
                if 'conn' in locals():
                    conn.rollback()
                st.error(f"Error resetting sample data: {str(e)}")
                
                if 'conn' in locals():
                    conn.close()

# Help
def render_help():
    """Render the help page"""
    st.markdown("<h1 class='main-header'>Help & Documentation</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    ## About Dealer Nudging System (DNS)
    
    The Dealer Nudging System (DNS) is a comprehensive platform designed to help OEMs (Original Equipment Manufacturers) incentivize dealers to sell specific products through various schemes and offers.
    
    ### Key Features
    
    - **Scheme Management**: Upload, view, and manage dealer incentive schemes
    - **Product Catalog**: Maintain a catalog of products with detailed specifications
    - **Dealer Network**: Manage your dealer network and track performance
    - **Sales Simulation**: Simulate sales to calculate incentives and free items
    - **Performance Analytics**: Visualize scheme effectiveness and dealer performance
    - **Approval Workflow**: Implement a structured approval process for scheme changes
    
    ### Getting Started
    
    1. **Dashboard**: Get an overview of active schemes, products, and performance metrics
    2. **Scheme Explorer**: Browse and filter available schemes
    3. **Products**: View the product catalog and performance data
    4. **Dealers**: Manage your dealer network and view dealer performance
    5. **Sales Simulation**: Simulate sales transactions to calculate incentives
    6. **Upload Scheme**: Upload new scheme documents in PDF format
    
    ### Need Help?
    
    For additional assistance, please contact the system administrator.
    """)
    
    # FAQ section
    st.markdown("<h2 class='sub-header'>Frequently Asked Questions</h2>", unsafe_allow_html=True)
    
    faq_items = [
        {
            "question": "How do I upload a new scheme?",
            "answer": "Navigate to the 'Upload Scheme' page from the sidebar, then upload a PDF document containing the scheme details. The system will automatically extract the scheme information and allow you to review it before saving to the database."
        },
        {
            "question": "How does the approval workflow work?",
            "answer": "When you edit a scheme, the changes are submitted for approval. Approvers can review the changes from the 'Approvals' page and either approve or reject them. Approved changes are immediately reflected in the system."
        },
        {
            "question": "What happens when a scheme includes free items?",
            "answer": "When a scheme includes free items, they are highlighted throughout the system. In the sales simulation, a special prompt is displayed to remind dealers to inform customers about the free items included with their purchase."
        },
        {
            "question": "How can I see which schemes are performing best?",
            "answer": "The Dashboard provides visualizations of scheme performance, including a bar chart of top-performing schemes by incentive amount and a product performance heatmap showing which products perform best under which schemes."
        },
        {
            "question": "Can I export data from the system?",
            "answer": "Currently, data export functionality is not implemented. However, you can take screenshots of the visualizations and tables for reporting purposes."
        }
    ]
    
    for i, faq in enumerate(faq_items):
        with st.expander(faq["question"]):
            st.markdown(faq["answer"])

# Main function
def main():
    """Main application function"""
    # Initialize session state
    init_session_state()
    
    # Render sidebar
    render_sidebar()
    
    # Render selected page
    if st.session_state.page == 'dashboard':
        render_dashboard()
    elif st.session_state.page == 'schemes':
        render_schemes()
    elif st.session_state.page == 'scheme_details':
        render_scheme_details()
    elif st.session_state.page == 'edit_scheme':
        render_edit_scheme()
    elif st.session_state.page == 'products':
        render_products()
    elif st.session_state.page == 'dealers':
        render_dealers()
    elif st.session_state.page == 'simulation':
        render_simulate_sales()
    elif st.session_state.page == 'upload':
        render_upload_scheme()
    elif st.session_state.page == 'approvals':
        render_approvals()
    elif st.session_state.page == 'settings':
        render_settings()
    elif st.session_state.page == 'help':
        render_help()

if __name__ == "__main__":
    main()

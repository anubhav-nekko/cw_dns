# Dealer Nudging System (DNS) - Documentation

## Overview

The Dealer Nudging System (DNS) is a comprehensive platform designed to help OEMs (Original Equipment Manufacturers) incentivize dealers to sell specific products through various schemes and offers. This improved version focuses on mobile phone dealers but is designed with a flexible schema that can accommodate other product categories.

## Key Features

1. **Granular Data Model**: Captures detailed product specifications, complex scheme structures, and various incentive types
2. **PDF Extraction**: Automatically extracts scheme details from PDF documents
3. **Interactive Dashboard**: Provides comprehensive insights with advanced visualizations
4. **Scheme Explorer**: Allows detailed exploration of all schemes and their parameters
5. **Editable Tables**: Supports editing of scheme details with approval workflow
6. **Sales Simulation**: Enhanced simulation tool to model different sales scenarios
7. **Approval Workflow**: Structured process for reviewing and approving scheme changes

## System Architecture

The system consists of the following components:

1. **Database**: SQLite database with a comprehensive schema for storing all scheme and sales data
2. **PDF Processor**: Python module for extracting structured data from scheme PDFs
3. **Streamlit App**: Web interface for interacting with the system
4. **AWS Integration**: Optional integration with AWS Bedrock (Claude) and Textract for enhanced PDF processing

## Setup Instructions

1. **Install Dependencies**:
   ```bash
   pip install streamlit plotly pandas boto3 PyPDF2 Pillow pymupdf
   ```

2. **Configure AWS Credentials** (Optional):
   - Place your AWS credentials in `secrets.json` file
   - Required for enhanced PDF extraction using AWS Textract and Claude

3. **Initialize the System**:
   ```bash
   python setup.py
   ```
   This will:
   - Create necessary directories
   - Initialize the database
   - Copy sample PDFs to the schemes directory
   - Set up the secrets file

4. **Process Scheme PDFs**:
   ```bash
   python pdf_processor.py
   ```
   This will:
   - Extract data from all PDFs in the schemes directory
   - Populate the database with scheme details
   - Create sample dealers and sales data

5. **Run the Application**:
   ```bash
   streamlit run app.py
   ```

## Database Schema

The system uses a comprehensive SQLite database with the following tables:

1. **schemes**: Stores basic scheme information
2. **products**: Stores product details with granular specifications
3. **dealers**: Stores dealer information
4. **scheme_products**: Links schemes to products with incentive details
5. **payout_slabs**: Stores quantity-based incentive slabs
6. **scheme_rules**: Stores scheme rules and conditions
7. **dealer_targets**: Stores targets assigned to dealers
8. **sales_transactions**: Records sales transactions
9. **exchange_transactions**: Records device exchange/upgrade transactions
10. **scheme_approvals**: Tracks approval workflow for schemes
11. **scheme_parameters**: Stores additional scheme parameters
12. **bundle_offers**: Stores bundle offer details

## Key Improvements

1. **Enhanced Data Model**:
   - Added support for RAM, storage, connectivity, and other detailed attributes
   - Implemented flexible payout structures (fixed, percentage, slab-based)
   - Added support for bundle offers and exchange programs

2. **Improved Dashboard**:
   - Added comprehensive visualizations for scheme effectiveness
   - Implemented regional performance tracking
   - Added product performance heatmap
   - Enhanced dealer performance comparison

3. **Advanced Features**:
   - Implemented approval workflow for scheme edits
   - Enhanced sales simulation with comparative analysis
   - Added support for bundle offers and upgrade programs
   - Implemented verification workflow for sales transactions

4. **Technical Improvements**:
   - Optimized PDF extraction process
   - Implemented AWS integration for enhanced text extraction
   - Designed responsive UI with modern styling
   - Added unique keys to all Plotly charts to prevent duplicate ID errors

## Usage Guide

### Dashboard

The dashboard provides a comprehensive overview of:
- Active schemes and products
- Sales performance over time
- Dealer performance comparison
- Product performance heatmap
- Scheme effectiveness analysis
- Regional performance visualization

### Scheme Explorer

Allows you to:
- Browse all schemes with detailed filtering
- View scheme details including products, incentives, and rules
- Approve or reject pending schemes

### Product Catalog

Provides:
- Comprehensive product listing with specifications
- Filtering by category, subcategory, and status
- Product management capabilities

### Dealer Management

Enables:
- Dealer listing and filtering
- Dealer performance tracking
- Dealer status management

### Sales Tracker

Offers:
- Transaction listing and filtering
- Verification workflow for transactions
- Sales metrics and analysis

### Upload Scheme

Allows:
- PDF upload for new schemes
- Automatic extraction of scheme details
- Manual editing of extracted data
- Submission for approval

### Approval Center

Provides:
- Listing of pending approvals
- Review and approval/rejection workflow
- Editing of scheme details before approval

### Sales Simulation

Enables:
- Simulation of sales scenarios
- Comparison of different schemes
- Analysis of dealer economics
- Recording of simulated sales

## Extensibility

The system is designed to be extensible in the following ways:

1. **Additional Product Categories**: The schema can accommodate various product types beyond mobile phones
2. **New Scheme Types**: The flexible scheme structure supports various incentive models
3. **Integration Points**: The system can be integrated with other systems through API endpoints
4. **Customizable Visualizations**: The dashboard can be extended with additional visualizations

## Troubleshooting

1. **PDF Extraction Issues**:
   - Ensure PDFs are text-based and not scanned images
   - For scanned PDFs, AWS Textract integration provides better results
   - Check AWS credentials if using Textract

2. **AWS Integration**:
   - Verify AWS credentials in secrets.json
   - Ensure the specified region matches your AWS setup
   - Check that the inference profile ARN is correct for Claude

3. **Database Issues**:
   - If database becomes corrupted, delete dealer_schemes.db and run setup.py again
   - Use the SQLite browser to inspect database contents if needed

4. **Streamlit Interface**:
   - If charts don't render, check for duplicate element IDs
   - Restart the application if the interface becomes unresponsive

## Future Enhancements

1. **Multi-user Authentication**: Add user roles and permissions
2. **Mobile App Integration**: Develop companion mobile app for dealers
3. **Advanced Analytics**: Implement predictive analytics for sales forecasting
4. **API Endpoints**: Create REST API for integration with other systems
5. **Notification System**: Add email/SMS notifications for scheme updates and approvals

# README.md - Dealer Nudging System (DNS)

## Overview

The Dealer Nudging System (DNS) is a comprehensive platform designed to help OEMs (Original Equipment Manufacturers) incentivize dealers to sell specific products through various schemes and offers. This improved version focuses on mobile phone dealers but is designed with a flexible schema that can accommodate other product categories.

## Directory Structure

```
dns_complete/
├── app.py                  # Main Streamlit application
├── pdf_processor_fixed.py  # PDF extraction and database population module
├── setup.py                # Environment setup script
├── documentation.md        # Comprehensive documentation
├── secrets.json            # AWS and API credentials
├── schemes/                # Directory for scheme PDF files
├── raw_texts/              # Directory for extracted text storage
└── uploads/                # Directory for user-uploaded PDFs
```

## Setup Instructions

1. **Install Dependencies**:
   ```bash
   pip install streamlit plotly pandas boto3 PyPDF2 Pillow pymupdf
   ```

2. **Initialize the System**:
   ```bash
   python setup.py
   ```
   This will:
   - Create necessary directories
   - Initialize the database
   - Copy sample PDFs to the schemes directory
   - Set up the secrets file

3. **Process Scheme PDFs**:
   ```bash
   python pdf_processor_fixed.py
   ```
   This will:
   - Extract data from all PDFs in the schemes directory
   - Populate the database with scheme details
   - Create sample dealers and sales data

4. **Run the Application**:
   ```bash
   streamlit run app.py
   ```

## Key Features

- Granular data model for mobile phone schemes
- PDF extraction with AWS integration
- Interactive dashboard with advanced visualizations
- Scheme explorer with detailed filtering
- Editable tables with approval workflow
- Enhanced sales simulation
- Comprehensive dealer and product management

For detailed documentation, please refer to `documentation.md`.

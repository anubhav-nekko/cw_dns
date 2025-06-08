# README.md - Dealer Nudging System (DNS)

## Overview

The Dealer Nudging System (DNS) is a comprehensive platform designed to help OEMs (Original Equipment Manufacturers) incentivize dealers to sell specific products through various schemes and offers. This improved version focuses on mobile phone dealers but is designed with a flexible schema that can accommodate other product categories.

## Directory Structure

```
dns_complete/
├── app.py                  # Main Streamlit application
├── pdf_processor_fixed.py  # PDF extraction and database population module
├── setup.py                # Environment setup script
├── sample_data.py          # Sample data generation module
├── documentation.md        # Comprehensive documentation
├── secrets.json            # AWS and API credentials
├── schemes/                # Directory for scheme PDF files
├── raw_texts/              # Directory for extracted text storage
└── uploads/                # Directory for user-uploaded PDFs
```

## Setup Instructions

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize the System**:
   ```bash
   python setup.py
   ```
   This will:
   - Create necessary directories
   - Initialize the database
   - Set up the secrets file
   - Create sample data with 1-2 schemes
   - Migrate database schema if needed

3. **Run the Application**:
   ```bash
   streamlit run app.py
   ```

## Key Features

- **Granular data model** for mobile phone schemes
- **PDF extraction** with AWS integration
- **Interactive dashboard** with advanced visualizations
- **Scheme explorer** with detailed filtering
- **Editable tables** with approval workflow
- **Enhanced sales simulation** with free item prompting
- **Comprehensive dealer and product management**

## Free Item Prompting

The system now includes a special feature that prompts dealers about free items included in schemes. When a scheme includes free items (like headphones with a mobile phone), the sales simulation will display a suggested script for dealers to use when informing customers about these free items.

## PDF Upload

You can upload new scheme PDFs during live demos. The system will:
1. Extract text from the PDF
2. Process the text to identify scheme details, products, and free items
3. Allow you to review the extracted data
4. Save the scheme to the database

For detailed documentation, please refer to `documentation.md`.

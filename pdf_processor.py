import fitz  # PyMuPDF
import os
import tempfile
from aws_clients import textract_client, call_claude_api # Assuming aws_clients.py is in the same directory or accessible via PYTHONPATH
import json

# Placeholder for Streamlit components if not running in Streamlit context
class StreamlitMock:
    def empty(self):
        class EmptyMock:
            def write(self, msg):
                print(msg)
        return EmptyMock()

    def progress(self, val):
        print(f"Progress: {val * 100:.2f}%")
        return None

    def error(self, msg):
        print(f"Error: {msg}")

st = StreamlitMock() # Use mock if streamlit is not available
try:
    import streamlit as st
except ImportError:
    print("Streamlit not found, using mock for st components.")

def extract_text_from_pdf_pages(file_path):
    """Extracts text from each page of a PDF using PyMuPDF and AWS Textract."""
    try:
        doc = fitz.open(file_path)
    except Exception as e:
        st.error(f"Error opening PDF {file_path}: {str(e)}")
        return []
        
    pages_text_data = []
    processing_message_placeholder = st.empty()
    progress_bar = st.progress(0)

    for page_num in range(len(doc)):
        processing_message_placeholder.write(f"Processing page {page_num + 1}/{len(doc)} of {os.path.basename(file_path)}...")
        temp_image_path = None

        try:
            page = doc.load_page(page_num)
            pix = page.get_pixmap()
            temp_image_dir = tempfile.gettempdir()
            if not os.path.exists(temp_image_dir):
                os.makedirs(temp_image_dir)
            temp_image_path = os.path.join(temp_image_dir, f"page_{page_num}.png")
            pix.save(temp_image_path)

            with open(temp_image_path, "rb") as image_file:
                image_bytes = image_file.read()

            response = textract_client.detect_document_text(
                Document={
                    'Bytes': image_bytes
                }
            )

            page_content_lines = []
            for block in response.get('Blocks', []):
                if block['BlockType'] == 'LINE' and 'Text' in block:
                    page_content_lines.append(block['Text'])
            
            extracted_text = "\n".join(page_content_lines)
            pages_text_data.append({
                "page_num": page_num + 1,
                "text": extracted_text
            })

        except Exception as e:
            st.error(f"Error processing page {page_num + 1} of {os.path.basename(file_path)}: {str(e)}")
            pages_text_data.append({
                "page_num": page_num + 1,
                "text": "",
                "error": str(e)
            })
            continue
        finally:
            if temp_image_path and os.path.exists(temp_image_path):
                os.remove(temp_image_path)

        if progress_bar:
            progress_bar.progress((page_num + 1) / len(doc))
    
    processing_message_placeholder.write(f"Finished processing {os.path.basename(file_path)}.")
    doc.close()
    return pages_text_data

def extract_structured_data_from_text(text_content, schema_description):
    """
    Uses Claude API to extract structured data from text based on a schema.
    """
    prompt = f"""Please extract information from the following text based on the provided schema description. 
    The text is from a sales scheme document. Identify all relevant details and structure them according to the schema. 
    If some information for a field is not present, use null or an empty string for that field. 
    Output the result as a single JSON object.

    Schema Description:
    {schema_description}

    Text Content:
    ---BEGIN TEXT---
    {text_content}
    ---END TEXT---

    Return only the JSON object.
    """
    
    structured_data_str = call_claude_api(prompt)
    if structured_data_str:
        try:
            # Ensure the response is a valid JSON
            # Sometimes Claude might return text around the JSON, try to extract JSON part
            if not structured_data_str.strip().startswith("{") or not structured_data_str.strip().endswith("}"):
                json_start = structured_data_str.find("{")
                json_end = structured_data_str.rfind("}")
                if json_start != -1 and json_end != -1 and json_start < json_end:
                    structured_data_str = structured_data_str[json_start:json_end+1]
            
            return json.loads(structured_data_str)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from Claude API response: {e}")
            print(f"Claude response was: {structured_data_str}")
            return None
    return None


if __name__ == "__main__":
    # This is an example of how to use the functions.
    # You'll need to provide a sample PDF path and a schema description.
    print("PDF Processor module loaded. You can test its functions here.")
    
    # Create a dummy PDF for testing if one doesn't exist
    sample_pdf_path = "/home/ubuntu/dealer_app/sample_document.pdf"
    if not os.path.exists(sample_pdf_path):
        try:
            from reportlab.pdfgen import canvas
            c = canvas.Canvas(sample_pdf_path)
            c.drawString(100, 750, "Hello World. This is a test PDF document.")
            c.drawString(100, 730, "Scheme Name: Test Scheme Alpha")
            c.drawString(100, 710, "Product: Test Product 1, Offer: 10% off")
            c.save()
            print(f"Created dummy PDF: {sample_pdf_path}")
        except ImportError:
            print("reportlab not found, skipping dummy PDF creation. Please provide a sample PDF for testing.")
        except Exception as e:
            print(f"Error creating dummy PDF: {e}") 

    if os.path.exists(sample_pdf_path):
        print(f"\n--- Testing PDF Text Extraction from {sample_pdf_path} ---")
        extracted_pages = extract_text_from_pdf_pages(sample_pdf_path)
        full_text = ""
        for page_data in extracted_pages:
            print(f"Page {page_data['page_num']}:")
            # print(page_data['text'])
            full_text += page_data['text'] + "\n\n"
        print("--- Finished PDF Text Extraction ---")

        if full_text:
            print("\n--- Testing Structured Data Extraction with Claude --- ")
            # Example schema (simplified for testing)
            schema_desc_example = '''
            {
                "deal_name": "Name of the deal/scheme",
                "scheme_period_start": "Start date of the scheme (YYYY-MM-DD)",
                "scheme_period_end": "End date of the scheme (YYYY-MM-DD)",
                "products_offers": [
                    {
                        "product_name": "Name of the product",
                        "offer_description": "Description of the offer (e.g., discount, payout)",
                        "payout_value": "Value of the payout/discount",
                        "payout_unit": "Unit of the payout (e.g., %, INR)"
                    }
                ]
            }
            '''
            structured_info = extract_structured_data_from_text(full_text.strip(), schema_desc_example)
            if structured_info:
                print("Structured Data Extracted:")
                print(json.dumps(structured_info, indent=2))
            else:
                print("Could not extract structured data.")
            print("--- Finished Structured Data Extraction Test ---")
    else:
        print(f"Sample PDF {sample_pdf_path} not found. Skipping tests.")


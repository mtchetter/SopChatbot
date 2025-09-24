"""Taking an HR docuemnt then extracting the text from it and saving it to a CSV file. The CSV file will have one row per page of the PDF, 
with columns for the PDF filename, page number, page content, word count, and character count."""
import os
import pandas as pd
import fitz  # PyMuPDF
import warnings
warnings.filterwarnings('ignore')

def extract_pdf_content(file_path):
    print("Step/1: Starting PDF extraction")
    print(f"Processing file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File does not exist at {file_path}")
        return None
    
    # Get the PDF filename without path
    pdf_filename = os.path.basename(file_path)
    print(f"PDF filename: {pdf_filename}")
    
    # Add a new list for storing each page's content as a separate row
    pages_df_data = []
    
    print("Step/2: Extracting document text page by page")
    try:
        doc = fitz.open(file_path)
        print(f"Document has {len(doc)} pages")
        
        # Process each page
        for page_num, page in enumerate(doc):
            print(f"Processing page {page_num+1}")
            
            # Get text - this will be sequential text by default
            text = page.get_text()
            
            # Create a row for this page in the pages DataFrame
            page_row = {
                'PDF_Filename': pdf_filename,
                'Page_Number': page_num + 1,
                'Page_Content': text,
                'Word_Count': len(text.split()),
                'Character_Count': len(text)
            }
            pages_df_data.append(page_row)
            
        print("Extraction complete")
    except Exception as e:
        print(f"Extraction error: {e}")
        return None
    
    # Create the pages DataFrame where each row is a page
    print("Step/3: Creating page-by-page DataFrame")
    pages_df = pd.DataFrame(pages_df_data)
    print(f"Created DataFrame with {len(pages_df)} rows (one per page)")
    
    return pages_df

def main():
    # File path
    file_path = r"C:\Users\mtschetter\OneDrive - heihotels.com\Projects\operation_frozen_heart\key_documents\source_files\HR.CB.002.16 - 2025 Property Sales + Revenue Management Leadership Incentive Plan.pdf"
    
    # Extract data
    pages_df = extract_pdf_content(file_path)
    
    if pages_df is not None:
        # Print sample from the pages DataFrame
        print("\nPage-by-Page DataFrame Sample (first page):")
        print(f"PDF Filename: {pages_df['PDF_Filename'].iloc[0]}")
        print(f"Page Number: {pages_df['Page_Number'].iloc[0]}")
        print(f"Word Count: {pages_df['Word_Count'].iloc[0]}")
        print(f"Character Count: {pages_df['Character_Count'].iloc[0]}")
        content_sample = pages_df['Page_Content'].iloc[0][:200] + "..." if len(pages_df['Page_Content'].iloc[0]) > 200 else pages_df['Page_Content'].iloc[0]
        print(f"Content Sample: {content_sample}")
        
        # Save only the pages DataFrame
        pages_output = os.path.join(os.path.dirname(file_path), "pdf_pages.csv")
        pages_df.to_csv(pages_output, index=False)
        print(f"Page-by-page data saved to: {pages_output}")
        
        return pages_df
    else:
        print("Failed to extract data from PDF")
        return None

if __name__ == "__main__":
    main()
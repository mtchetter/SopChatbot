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
    # Folder path - using the directory from the original file path
    folder_path = r"C:\Users\mtschetter\OneDrive - heihotels.com\Projects\operation_frozen_heart\key_documents\source_files"
    
    print("Step/4: Getting list of PDF files in folder")
    # Get all PDF files in the directory
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
    print(f"Found {len(pdf_files)} PDF files in folder")
    
    # Create an empty DataFrame to store all pages from all PDFs
    all_pages_df = pd.DataFrame()
    
    print("Step/5: Processing each PDF file")
    # Process each PDF file
    for pdf_file in pdf_files:
        print(f"\nProcessing PDF: {pdf_file}")
        file_path = os.path.join(folder_path, pdf_file)
        
        # Extract data from this PDF
        pages_df = extract_pdf_content(file_path)
        
        # Append to the main DataFrame if extraction was successful
        if pages_df is not None:
            print(f"Adding {len(pages_df)} pages to the main DataFrame")
            all_pages_df = pd.concat([all_pages_df, pages_df], ignore_index=True)
        else:
            print(f"Skipping {pdf_file} due to extraction failure")
    
    print("\nStep/6: Saving combined results")
    if not all_pages_df.empty:
        print(f"Total pages extracted from all PDFs: {len(all_pages_df)}")
        
        # Print sample from the first page of the first PDF
        print("\nSample from first page:")
        print(f"PDF Filename: {all_pages_df['PDF_Filename'].iloc[0]}")
        print(f"Page Number: {all_pages_df['Page_Number'].iloc[0]}")
        print(f"Word Count: {all_pages_df['Word_Count'].iloc[0]}")
        print(f"Character Count: {all_pages_df['Character_Count'].iloc[0]}")
        content_sample = all_pages_df['Page_Content'].iloc[0][:200] + "..." if len(all_pages_df['Page_Content'].iloc[0]) > 200 else all_pages_df['Page_Content'].iloc[0]
        print(f"Content Sample: {content_sample}")
        
        # Save all pages to a CSV file
        output_path = os.path.join(folder_path, "all_pdf_pages.csv")
        all_pages_df.to_csv(output_path, index=False)
        print(f"All page data saved to: {output_path}")
        
        return all_pages_df
    else:
        print("No PDF data was extracted")
        return None

if __name__ == "__main__":
    main()
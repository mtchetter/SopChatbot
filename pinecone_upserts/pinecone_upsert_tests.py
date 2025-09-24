# Import required packages
from pinecone import Pinecone
import numpy as np
from openai import OpenAI
import os
import time
import uuid
import csv
import pandas as pd

def upsert_csv_to_pinecone(csv_path):
    print("Step/1: Initializing API keys")
    pinecone_api_key = "pcsk_7AawNA_HtqvBxvERceVonytxvVWpQRf289AAGR3mYJJkYdw8abY7dKDemZ5qbrh5p4eLCY"
    
    # Fix the OpenAI API key - the previous one was rejected with a 401 error
    # The format should be "sk-..." not "sk-proj-..."
    openai_api_key = "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"
    print("API keys initialized")
    
    print("Step/2: Setting up OpenAI client")
    client = OpenAI(api_key=openai_api_key)
    print("OpenAI client initialized successfully")
    
    print("Step/3: Initializing Pinecone")
    pc = Pinecone(api_key=pinecone_api_key)
    print("Pinecone initialized successfully")
    
    print("Step/4: Listing existing indexes")
    active_indexes = pc.list_indexes()
    print(f"Active indexes: {active_indexes}")
    
    print("Step/5: Checking for 'hr-data' index")
    index_name = "hr-data"
    all_index_names = [index.name for index in active_indexes.indexes]
    print(f"All available indexes: {all_index_names}")
    
    if index_name in all_index_names:
        print(f"Found index: {index_name}")
        
        print("Step/6: Connecting to the index")
        index = pc.Index(index_name)
        print("Connected to index successfully")
        
        print("Step/7: Reading CSV file")
        print(f"CSV path: {csv_path}")
        
        if not os.path.exists(csv_path):
            print(f"Error: CSV file not found at path: {csv_path}")
            return False
        
        # Read the CSV file using pandas
        print("Opening CSV file")
        try:
            df = pd.read_csv(csv_path)
            print(f"CSV loaded successfully with {len(df)} rows")
            
            # Check for required columns
            required_columns = ["Page_Content", "Page_Number", "PDF_Filename"]
            for col in required_columns:
                if col not in df.columns:
                    print(f"Error: Required column '{col}' not found in CSV")
                    return False
                    
            print(f"All required columns found: {required_columns}")
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return False
        
        print("Step/8: Processing CSV data - each row will become one embedding")
        # Simple processing - each row becomes one embedding
        total_rows = len(df)
        print(f"Total pages to process: {total_rows}")
        
        print("Step/9: Generating embeddings and upserting to Pinecone")
        batch_size = 20  # Process this many pages at a time
        total_upserted = 0
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1} with {len(batch_df)} pages")
            
            vectors_to_upsert = []
            for j, (_, row) in enumerate(batch_df.iterrows()):
                print(f"Generating embedding for page {i+j+1}/{total_rows}")
                
                page_text = row["Page_Content"]
                page_number = int(row["Page_Number"])
                pdf_filename = row["PDF_Filename"]
                
                # Format page reference
                page_ref_str = f"p.{page_number}"
                
                # Generate embedding for a single page
                response = client.embeddings.create(
                    input=page_text,
                    model="text-embedding-3-large"
                )
                
                page_id = f"csv-page-{uuid.uuid4()}"
                vector = response.data[0].embedding
                
                # Enhanced metadata with page reference
                vectors_to_upsert.append((
                    page_id, 
                    vector, 
                    {
                        "text": page_text, 
                        "source": pdf_filename,
                        "page_number": str(page_number),
                        "page_reference": page_ref_str  # Formatted string for display
                    }
                ))
                
                print(f"Prepared vector for page {i+j+1}/{total_rows} from {pdf_filename}, {page_ref_str}")
                
                # Small pause between embedding requests to avoid rate limits
                time.sleep(0.5)
            
            # Upsert vectors to Pinecone
            print(f"Upserting batch of {len(vectors_to_upsert)} vectors")
            upsert_response = index.upsert(vectors=vectors_to_upsert)
            total_upserted += len(vectors_to_upsert)
            print(f"Upserted batch. Total vectors upserted: {total_upserted}/{total_rows}")
            
            # Rate limiting to avoid hitting API limits
            if i + batch_size < total_rows:
                print("Pausing for 3 seconds before processing next batch")
                time.sleep(3)
        
        print(f"Step/10: Successfully upserted {total_upserted} pages from CSV to Pinecone")
        return True
    else:
        print(f"Index '{index_name}' not found. Available indexes: {all_index_names}")
        return False

def format_page_references(page_numbers):
    """Format a list of page numbers into a readable reference string."""
    if not page_numbers:
        return "Unknown page"
    
    # Sort the page numbers
    page_numbers = sorted(page_numbers)
    
    # Group consecutive page numbers
    groups = []
    current_group = [page_numbers[0]]
    
    for i in range(1, len(page_numbers)):
        if page_numbers[i] == page_numbers[i-1] + 1:
            current_group.append(page_numbers[i])
        else:
            groups.append(current_group)
            current_group = [page_numbers[i]]
    
    groups.append(current_group)
    
    # Format the groups
    formatted_groups = []
    for group in groups:
        if len(group) == 1:
            formatted_groups.append(f"p.{group[0]}")
        else:
            formatted_groups.append(f"pp.{group[0]}-{group[-1]}")
    
    # Join the groups
    return ", ".join(formatted_groups)

if __name__ == "__main__":
    print("Starting CSV upsert process...")
    csv_path = r"C:\Users\mtschetter\OneDrive - heihotels.com\Projects\operation_frozen_heart\key_documents\source_files\all_pdf_pages.csv"
    result = upsert_csv_to_pinecone(csv_path)
    print(f"Process completed with result: {result}")
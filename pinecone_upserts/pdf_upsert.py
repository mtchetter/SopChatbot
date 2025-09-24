# Import required packages
from pinecone import Pinecone
import numpy as np
from openai import OpenAI
import PyPDF2
import os
import time
import uuid
import shutil
import glob

def process_all_pdfs_and_archive():
    print("Step/1: Setting up folder paths")
    sop_folder = r"C:\Users\mtschetter\Desktop\all_sop"
    archive_folder = os.path.join(sop_folder, "Archive")
    print(f"SOP folder: {sop_folder}")
    print(f"Archive folder: {archive_folder}")
    
    print("Step/2: Creating Archive folder if it doesn't exist")
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
        print("Archive folder created")
    else:
        print("Archive folder already exists")
    
    print("Step/3: Finding all PDF files in SOP folder")
    pdf_pattern = os.path.join(sop_folder, "*.pdf")
    pdf_files = glob.glob(pdf_pattern)
    print(f"Found {len(pdf_files)} PDF files")
    for pdf in pdf_files:
        print(f"  - {os.path.basename(pdf)}")
    
    print("Step/4: Initializing API keys")
    pinecone_api_key = "pcsk_xkaB7_F7MV5uV7pCHN9EFTA8JWCyDC8nFPeb6wmD7RveWWXXNJeSDRmhEKrQHovaZCePS"
    openai_api_key = "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"
    print("API keys initialized")
    
    print("Step/5: Setting up OpenAI client")
    client = OpenAI(api_key=openai_api_key)
    print("OpenAI client initialized successfully")
    
    print("Step/6: Initializing Pinecone")
    pc = Pinecone(api_key=pinecone_api_key)
    print("Pinecone initialized successfully")
    
    print("Step/7: Checking for 'hr-data' index")
    active_indexes = pc.list_indexes()
    index_name = "hr-data"
    all_index_names = [index.name for index in active_indexes.indexes]
    print(f"All available indexes: {all_index_names}")
    
    if index_name not in all_index_names:
        print(f"Index '{index_name}' not found. Available indexes: {all_index_names}")
        return False
    
    print(f"Found index: {index_name}")
    index = pc.Index(index_name)
    print("Connected to index successfully")
    
    print("Step/8: Processing each PDF file")
    successful_pdfs = []
    failed_pdfs = []
    
    for pdf_count, pdf_path in enumerate(pdf_files, 1):
        pdf_name = os.path.basename(pdf_path)
        print(f"\nStep/8.{pdf_count}: Processing PDF {pdf_count}/{len(pdf_files)}: {pdf_name}")
        
        print(f"Step/8.{pdf_count}.1: Reading PDF file")
        if not os.path.exists(pdf_path):
            print(f"Error: PDF file not found at path: {pdf_path}")
            failed_pdfs.append(pdf_name)
            continue
        
        pdf_file = open(pdf_path, 'rb')
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        print(f"PDF has {len(pdf_reader.pages)} pages")
        
        print(f"Step/8.{pdf_count}.2: Extracting text with page tracking")
        pages_content = []
        
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            page_text = page.extract_text()
            pages_content.append({
                'page_number': page_num + 1,
                'text': page_text
            })
            print(f"Extracted text from page {page_num+1}")
        
        print(f"Step/8.{pdf_count}.3: Chunking PDF content with page tracking")
        chunks = []
        chunk_size = 2000
        overlap = 75
        
        current_chunk = ""
        current_chunk_pages = set()
        
        for page_data in pages_content:
            page_number = page_data['page_number']
            page_text = page_data['text']
            
            text_segments = [page_text[i:i+500] for i in range(0, len(page_text), 400)]
            
            for segment in text_segments:
                if len(current_chunk) + len(segment) > chunk_size and len(current_chunk) >= 100:
                    chunks.append({
                        'text': current_chunk,
                        'pages': sorted(list(current_chunk_pages))
                    })
                    
                    print(f"Created chunk {len(chunks)} with {len(current_chunk)} chars from pages {current_chunk_pages}")
                    
                    overlap_text = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
                    current_chunk = overlap_text + segment
                    current_chunk_pages = {page_number}
                else:
                    current_chunk += segment
                    current_chunk_pages.add(page_number)
        
        if len(current_chunk) >= 100:
            chunks.append({
                'text': current_chunk,
                'pages': sorted(list(current_chunk_pages))
            })
            print(f"Created final chunk {len(chunks)} with {len(current_chunk)} chars from pages {current_chunk_pages}")
        
        print(f"Total chunks created for {pdf_name}: {len(chunks)}")
        pdf_file.close()
        
        print(f"Step/8.{pdf_count}.4: Generating embeddings and upserting to Pinecone")
        batch_size = 20
        total_upserted = 0
        
        for i in range(0, len(chunks), batch_size):
            batch_chunks = chunks[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1} with {len(batch_chunks)} chunks for {pdf_name}")
            
            vectors_to_upsert = []
            for j, chunk_data in enumerate(batch_chunks):
                print(f"Generating embedding for chunk {i+j+1}/{len(chunks)} from {pdf_name}")
                
                chunk_text = chunk_data['text']
                chunk_pages = chunk_data['pages']
                
                page_refs = chunk_pages
                page_ref_str = format_page_references(page_refs)
                
                response = client.embeddings.create(
                    input=chunk_text,
                    model="text-embedding-3-large"
                )
                
                chunk_id = f"pdf-chunk-{uuid.uuid4()}"
                vector = response.data[0].embedding
                
                page_numbers_as_strings = [str(page) for page in page_refs]
                
                vectors_to_upsert.append((
                    chunk_id, 
                    vector, 
                    {
                        "text": chunk_text, 
                        "source": os.path.basename(pdf_path),
                        "chunk_index": i+j,
                        "page_numbers": page_numbers_as_strings,
                        "page_reference": page_ref_str
                    }
                ))
                
                print(f"Prepared vector for chunk {i+j+1}/{len(chunks)} from pages {page_ref_str}")
                time.sleep(0.5)
            
            print(f"Upserting batch of {len(vectors_to_upsert)} vectors for {pdf_name}")
            upsert_response = index.upsert(vectors=vectors_to_upsert)
            total_upserted += len(vectors_to_upsert)
            print(f"Upserted batch. Total vectors upserted for {pdf_name}: {total_upserted}/{len(chunks)}")
            
            if i + batch_size < len(chunks):
                print("Pausing for 3 seconds before processing next batch")
                time.sleep(3)
        
        print(f"Step/8.{pdf_count}.5: Successfully processed {pdf_name} - upserted {total_upserted} chunks")
        
        print(f"Step/8.{pdf_count}.6: Moving {pdf_name} to Archive folder")
        source_path = pdf_path
        destination_path = os.path.join(archive_folder, pdf_name)
        
        print(f"Moving from: {source_path}")
        print(f"Moving to: {destination_path}")
        
        shutil.move(source_path, destination_path)
        print(f"Successfully moved {pdf_name} to Archive folder")
        
        successful_pdfs.append(pdf_name)
        print(f"PDF {pdf_count}/{len(pdf_files)} completed successfully")
    
    print("Step/9: Final processing summary")
    print(f"Total PDFs found: {len(pdf_files)}")
    print(f"Successfully processed: {len(successful_pdfs)}")
    print(f"Failed to process: {len(failed_pdfs)}")
    
    print("Successfully processed PDFs:")
    for pdf in successful_pdfs:
        print(f"  ✓ {pdf}")
    
    if failed_pdfs:
        print("Failed PDFs:")
        for pdf in failed_pdfs:
            print(f"  ✗ {pdf}")
    
    print("Step/10: Process completed!")
    return True

def format_page_references(page_numbers):
    """Format a list of page numbers into a readable reference string."""
    if not page_numbers:
        return "Unknown page"
    
    page_numbers = sorted(page_numbers)
    
    groups = []
    current_group = [page_numbers[0]]
    
    for i in range(1, len(page_numbers)):
        if page_numbers[i] == page_numbers[i-1] + 1:
            current_group.append(page_numbers[i])
        else:
            groups.append(current_group)
            current_group = [page_numbers[i]]
    
    groups.append(current_group)
    
    formatted_groups = []
    for group in groups:
        if len(group) == 1:
            formatted_groups.append(f"p.{group[0]}")
        else:
            formatted_groups.append(f"pp.{group[0]}-{group[-1]}")
    
    return ", ".join(formatted_groups)

if __name__ == "__main__":
    print("Starting PDF batch processing and archive process...")
    result = process_all_pdfs_and_archive()
    print(f"Batch process completed with result: {result}")
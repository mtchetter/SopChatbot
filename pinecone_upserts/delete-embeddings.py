'''deletes pinecone embeddings from the hr-data index'''

# Import required package
from pinecone import Pinecone
import time
import numpy as np
import os
from dotenv import load_dotenv

def delete_all_pinecone_embeddings():
    print("Step/1: Loading environment variables")
    
    # Get current directory
    current_dir = os.getcwd()
    print(f"Current working directory: {current_dir}")
    
    # Define .env path
    dotenv_path = os.path.join(current_dir, '.env')
    print(f"Looking for .env at: {dotenv_path}")
    
    # Check if file exists
    if os.path.exists(dotenv_path):
        print(f".env file exists at {dotenv_path}")
        
        # Try to read the file content
        print("Reading .env file content:")
        try:
            with open(dotenv_path, 'r') as f:
                env_content = f.read()
                print(f"File content length: {len(env_content)} characters")
                print(f"First 20 characters: {env_content[:20]}...")
        except Exception as e:
            print(f"Error reading .env file: {e}")
    else:
        print(f".env file NOT found at {dotenv_path}")
    
    # Try loading from file first
    print("Attempting to load with python-dotenv...")
    load_dotenv(dotenv_path)
    
    # Check if variables are loaded
    pinecone_api_key = os.getenv("PINECONE_API_KEY")
    print(f"PINECONE_API_KEY found from dotenv: {'Yes' if pinecone_api_key else 'No'}")
    
    # If not loaded from file, set directly
    if not pinecone_api_key:
        print("Setting API key directly as fallback")
        os.environ["PINECONE_API_KEY"] = "pcsk_7NdY6A_AaELQcsyWS531xhiWsJCqsSrdtT3GHvkWwGR49pB6SEzwThJWzAMTLNkYovRuM4"
        
        # Check again
        pinecone_api_key = os.getenv("PINECONE_API_KEY")
        print(f"PINECONE_API_KEY set directly: {'Yes' if pinecone_api_key else 'No'}")
    
    if not pinecone_api_key:
        print("Error: Failed to set Pinecone API key through any method")
        return False
    
    print("API key loaded successfully")
    
    print("Step/2: Initializing Pinecone")
    pc = Pinecone(api_key=pinecone_api_key)
    print("Pinecone initialized successfully")
    
    print("Step/3: Listing existing indexes")
    active_indexes = pc.list_indexes()
    print(f"Active indexes: {active_indexes}")
    
    print("Step/4: Checking for 'hr-data' index")
    index_name = "hr-data"
    all_index_names = [index.name for index in active_indexes.indexes]
    print(f"All available indexes: {all_index_names}")
    
    if index_name in all_index_names:
        print(f"Found index: {index_name}")
        
        print("Step/5: Connecting to the index")
        index = pc.Index(index_name)
        print("Connected to index successfully")
        
        print("Step/6: Getting index stats before deletion")
        stats = index.describe_index_stats()
        total_vectors = stats.total_vector_count
        print(f"Current vector count: {total_vectors}")
        
        if total_vectors == 0:
            print("No vectors to delete.")
            return True
        
        print("Step/7: Getting all vector IDs")
        # Create a dummy vector with the correct dimension (3072 based on the error message)
        dummy_vector = [0.0] * 3072
        
        # Set max batch size for fetching and deleting 
        batch_size = 100
        
        print(f"Querying index to fetch vector IDs in batches of {batch_size}...")
        total_deleted = 0
        
        while total_deleted < total_vectors:
            print(f"Progress: {total_deleted}/{total_vectors} vectors deleted")
            
            # Query for a batch of vectors
            results = index.query(
                vector=dummy_vector,
                top_k=batch_size,
                include_metadata=False,
                include_values=False
            )
            
            # Extract IDs
            ids_to_delete = [match.id for match in results.matches]
            
            if not ids_to_delete:
                print("Warning: No more IDs found but vectors may remain. Checking current count...")
                current_stats = index.describe_index_stats()
                remaining = current_stats.total_vector_count
                
                if remaining > 0:
                    print(f"Still have {remaining} vectors. Will try a different approach.")
                    # Try namespace approach - some Pinecone indexes have namespaces
                    namespaces = current_stats.namespaces
                    if namespaces:
                        for ns_name in namespaces:
                            print(f"Deleting all vectors in namespace: {ns_name}")
                            index.delete(namespace=ns_name)
                    else:
                        print("No namespaces found. Will try the default namespace.")
                        index.delete(namespace="")
                
                # If nothing else works, we'll try a brute force approach
                print("Attempting complete deletion with alternate approach...")
                index._vector_api.delete_vectors(delete_all=True)
                break
                
            print(f"Found {len(ids_to_delete)} vectors to delete")
            
            # Delete the batch of vectors
            print(f"Deleting batch of {len(ids_to_delete)} vectors...")
            index.delete(ids=ids_to_delete)
            
            total_deleted += len(ids_to_delete)
            print(f"Deleted {len(ids_to_delete)} vectors")
            
            # Small pause to avoid rate limiting
            time.sleep(1)
        
        print("Step/8: Verifying deletion")
        time.sleep(3)  # Wait for deletion to process
        final_stats = index.describe_index_stats()
        print(f"Final vector count: {final_stats.total_vector_count}")
        
        if final_stats.total_vector_count == 0:
            print("SUCCESS: All vectors have been deleted!")
            return True
        else:
            print(f"WARNING: {final_stats.total_vector_count} vectors still remain.")
            return False
    else:
        print(f"Index '{index_name}' not found. Available indexes: {all_index_names}")
        print("No deletion needed.")
        return True

if __name__ == "__main__":
    print("Starting deletion of all Pinecone embeddings...")
    result = delete_all_pinecone_embeddings()
    print(f"Process completed with result: {result}")
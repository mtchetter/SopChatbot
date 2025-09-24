from pinecone import Pinecone
import numpy as np
from openai import OpenAI
import os
from dotenv import load_dotenv
import pandas as pd  # Add pandas import

from main_line.all_tools import execute_sql_query  # Importing the SQL query function

execute_sql_query("""SELECT TOP (1000) [sender]
      ,[timestamp]
      ,[content]
      ,[message_id]
      ,[userid]
      ,[record_timestamp]
      ,[split_status]
      ,[ai_processed]
  FROM [FinancialModeling].[hr].[ticket_content]
""")




def query_pinecone_and_openai(query_text):
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
        with open(dotenv_path, 'r') as f:
            env_content = f.read()
            print(f"File content length: {len(env_content)} characters")
            print(f"First 20 characters: {env_content[:20]}...")
    else:
        print(f".env file NOT found at {dotenv_path}")
        print("Creating .env file with API keys")
        with open(dotenv_path, 'w') as f:
            f.write("PINECONE_API_KEY=pcsk_RAhuF_ARBFLnDGXEoTkjdmYdnZb4QAb6DTobGrRjrSnDmPmajLsDzXMbMEeHAVgPxXdDE\n")
            f.write("OPENAI_API_KEY=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\n")
        print("Created new .env file")
    
    # Try loading from file first
    print("Attempting to load with python-dotenv...")
    load_dotenv(dotenv_path)
    
    # Check if variables are loaded
    pinecone_api_key = os.getenv("PINECONE_API_KEY")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    
    print(f"PINECONE_API_KEY found from dotenv: {'Yes' if pinecone_api_key else 'No'}")
    print(f"OPENAI_API_KEY found from dotenv: {'Yes' if openai_api_key else 'No'}")
    
    # If not loaded from file, set directly
    if not pinecone_api_key or not openai_api_key:
        print("Setting API keys directly as fallback")
        os.environ["PINECONE_API_KEY"] = "pcsk_RAhuF_ARBFLnDGXEoTkjdmYdnZb4QAb6DTobGrRjrSnDmPmajLsDzXMbMEeHAVgPxXdDE"
        os.environ["OPENAI_API_KEY"] = "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"
        
        # Check again
        pinecone_api_key = os.getenv("PINECONE_API_KEY")
        openai_api_key = os.getenv("OPENAI_API_KEY")
        
        print(f"PINECONE_API_KEY set directly: {'Yes' if pinecone_api_key else 'No'}")
        print(f"OPENAI_API_KEY set directly: {'Yes' if openai_api_key else 'No'}")
    
    if not pinecone_api_key or not openai_api_key:
        print("Error: Failed to set API keys through any method")
        return "Error: Missing API keys"
    
    print("API keys loaded successfully")
    
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
        
        print("Step/7: Creating query")
        print(f"Query text: {query_text}")
        
        print("Step/8: Generating embedding for query")
        embedding_response = client.embeddings.create(
            input=query_text,
            model="text-embedding-3-large"
        )
        query_vector = embedding_response.data[0].embedding
        print(f"Generated embedding with dimension: {len(query_vector)}")
        
        print("Step/9: Querying Pinecone to retrieve 4 relevant documents")
        search_response = index.query(
            vector=query_vector,
            top_k=4,  # Get top 4 relevant documents
            include_metadata=True
        )
        print(f"Number of matching documents: {len(search_response['matches'])}")
        
        print("Step/10: Extracting text and metadata from matching documents")
        context_texts = []
        
        # Create lists to store data for dataframe
        doc_ids = []
        page_numbers = []
        sources = []
        similarity_scores = []
        doc_texts = []
        
        for i, match in enumerate(search_response['matches']):
            doc_id = i + 1
            doc_ids.append(doc_id)
            similarity_scores.append(match.score)
            
            # Extract metadata
            metadata = match.metadata
            
            # Extract text
            doc_text = metadata.get('text', 'No text available')
            doc_texts.append(doc_text)
            
            # Extract page_number (using default if not found)
            page_number = metadata.get('page_number', 'N/A')
            page_numbers.append(page_number)
            
            # Extract source (using default if not found)
            source = metadata.get('source', 'Unknown source')
            sources.append(source)
            
            print(f"Document {doc_id} score: {match.score}")
            print(f"Document {doc_id} page_number: {page_number}")
            print(f"Document {doc_id} source: {source}")
            print(f"Document {doc_id} first 50 chars: {doc_text[:50]}...")
            
            context_texts.append(doc_text)
        
        print("Step/11: Creating dataframe with document metadata")
        docs_df = pd.DataFrame({
            'doc_id': doc_ids,
            'page_number': page_numbers,
            'source': sources,
            'similarity_score': similarity_scores,
            'content': doc_texts
        })
        
        print("\n----- DOCUMENT METADATA DATAFRAME -----")
        print(docs_df[['doc_id', 'page_number', 'source', 'similarity_score']])
        print("----- END OF DATAFRAME -----\n")
        
        print("Step/12: Creating context for OpenAI")
        combined_context = "\n\n---\n\n".join(context_texts)
        print(f"Combined context length: {len(combined_context)} characters")
        
        print("Step/13: Sending query and context to OpenAI")
        chat_completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful HR assistant. Answer the question based on the provided context from company HR documents."},
                {"role": "user", "content": f"Context information from HR documents:\n\n{combined_context}\n\nBased only on the information provided above, please answer this question: {query_text}"}
            ],
            temperature=0
        )
        
        print("Step/14: Received response from OpenAI")
        openai_response = chat_completion.choices[0].message.content
        print("\n----- OPENAI RESPONSE -----")
        print(openai_response)
        print("----- END OF RESPONSE -----\n")
        
        # Return both the OpenAI response and the dataframe
        return openai_response, docs_df
    else:
        print(f"Index '{index_name}' not found. Available indexes: {all_index_names}")
        return "Error: HR data index not found.", None






# Modified function call to handle the returned dataframe
response, metadata_df = query_pinecone_and_openai(query_text="""You are a state-of-the-art AI HR assistant designed to fully replace traditional HR personnel. Your responses must always be grounded in official company HR documents, policies, and procedures. You do not speculate or improvise; if something is not covered by the documentation, you say so clearly and professionally.

Your tone should emulate a knowledgeable, warm, and professional HR representative. You should be empathetic, approachable, and articulate, while also staying firm and accurate where policy requires clarity or limitations.

Every answer should achieve two goals:

Accuracy & Compliance — All responses must strictly follow the company's HR documentation, including but not limited to employee handbooks, benefits guides, time-off policies, and code of conduct. Cite specific policies or section references when possible.

Human-Centric Delivery — Communicate like a real HR professional would. Be clear, thoughtful, and helpful. Mirror the style of a seasoned HR team member: friendly, supportive, and capable of handling sensitive topics with care.

If a question is outside the scope of the HR documentation provided, respond with something like:
“That’s a great question. Based on the current HR documentation available, there isn't a policy that directly addresses this. I recommend reaching out to [HR Manager Name or Team] for further clarification.”

Use plain language, avoid legalese unless quoting policy, and always aim to make the employee feel heard and respected—even when the answer is a “no.”

here is the question: Are arrests considered in the job?""")

# You can do more with the dataframe here if needed
if metadata_df is not None:
    print("\nFinal document metadata summary:")
    print(metadata_df[['doc_id', 'source', 'page_number']])
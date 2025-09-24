# delete_all_vectors.py
from pinecone import Pinecone

PINECONE_API_KEY = "your-api-key-here"
INDEX_NAME = "hr-data"   # the index you want to clear

print(f"Connecting to Pinecone and clearing all vectors from index: {INDEX_NAME}")

pc = Pinecone(api_key=PINECONE_API_KEY)

# Connect to the index
index = pc.Index(INDEX_NAME)

# Delete ALL vectors but keep the index definition
index.delete(delete_all=True)

print("✅ All vectors have been deleted. The index still exists.")

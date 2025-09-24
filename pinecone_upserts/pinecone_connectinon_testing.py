# test_pinecone_connection.py
from pinecone import Pinecone
import os

# --- Replace with your key or use env var if you prefer ---
PINECONE_API_KEY = "pcsk_6baxNS_TqwDk9caEhdTQDWXz4XB3Dxbuxr6N8m1P7KRRMvfmPxq1BA2DjfnAjpbQqRZWah"

print("Testing Pinecone connection…")

try:
    pc = Pinecone(api_key=PINECONE_API_KEY)

    # list_indexes() returns an iterator of index objects.
    # Convert each to its name for clarity.
    indexes = pc.list_indexes()
    print("\nRaw list_indexes() output:\n", indexes)

    # If indexes is a list of dicts/objects, pull the names for readability
    names = []
    for item in indexes:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict) and "name" in item:
            names.append(item["name"])
        else:
            names.append(repr(item))
    print("\nExtracted index names:\n", names)

except Exception as e:
    print("\nError while connecting to Pinecone:")
    print(type(e).__name__, e)

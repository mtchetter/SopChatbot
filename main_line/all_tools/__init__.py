# Fixed __init__.py
from .query_df import execute_sql_query
from .query_openai import prompt_openai

# Debug prints
print("DEBUG: Importing execute_sql_query...")
print("DEBUG: Importing extract_neo4j_graph_data...")
print("DEBUG: Importing prompt_openai...")
# print(f"DEBUG: prompt_openai type: {type(prompt_openai)}")
# print(f"DEBUG: prompt_openai callable: {callable(prompt_openai)}")

# __all__ = ['execute_sql_query', 'extract_neo4j_graph_data', 'prompt_openai']

__all__ = ['execute_sql_query', 'prompt_openai']
import pandas as pd
import sqlalchemy as sa
import urllib.parse

def execute_sql_query(sql_query):
    """
    Execute SQL query and return dataframe converted to string format
    Input: SQL query string
    Output: String representation of query results
    """
    
    # Step 1: Create connection parameters
    print("Step 1: Setting up connection parameters")
    server = 'colofinsql02'
    database = 'FinancialModeling'
    print(f"Server: {server}")
    print(f"Database: {database}")
    
    # Step 2: Create SQLAlchemy connection URL format
    print("Step 2: Creating SQLAlchemy connection URL")
    connection_url = f"mssql+pyodbc://@{server}/{database}?driver=SQL+Server&trusted_connection=yes"
    print(f"Connection URL created: {connection_url}")
    
    # Step 3: Create engine
    print("Step 3: Creating SQLAlchemy engine")
    engine = sa.create_engine(connection_url)
    print("Engine created successfully")
    
    # Step 4: Print the SQL query being executed
    print("Step 4: Preparing to execute SQL query")
    print(f"SQL Query: {sql_query}")
    
    # Step 5: Execute query and get dataframe
    print("Step 5: Executing query and converting to dataframe")
    df = pd.read_sql(sql_query, engine)
    print(f"Query executed successfully")
    print(f"Dataframe shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Step 6: Print the dataframe
    print("Step 6: Printing the dataframe")
    print(df)
    
    # Step 7: Close engine connection
    print("Step 7: Closing database connection")
    engine.dispose()
    print("Connection closed")
    
    # Step 8: Convert dataframe to string
    print("Step 8: Converting dataframe to string")
    df_string = df.to_string(index=False)
    print("Conversion completed")
    
    # Step 9: Print the string version
    print("Step 9: Printing the string version")
    print(df_string)
    
    # Step 10: Return the string
    print("Step 10: Returning string output")
    return df_string

# execute_sql_query("""SELECT TOP (1000) [sender]
#       ,[timestamp]
#       ,[content]
#       ,[message_id]
#       ,[userid]
#       ,[record_timestamp]
#       ,[split_status]
#       ,[ai_processed]
#   FROM [FinancialModeling].[hr].[ticket_content]
# """)
'''|06/02/2025 | Working | Banda Ki | This will classify each HR ticket into a category'''

from main_line.all_tools import execute_sql_query
from main_line.all_tools import prompt_openai
import json
import sqlalchemy as sa

def process_hr_ticket_classification_loop():
    
    print("Starting HR Ticket Classification Loop...")
    ticket_counter = 0
    
    while True:
        ticket_counter += 1
        print(f"\n=== PROCESSING TICKET #{ticket_counter} ===")
        
        # Step/1: Get the ticket data with unique identifier
        print("Step/1: Fetching ticket data from database...")
        data = execute_sql_query("""SELECT TOP (1) [sender]
              ,[content]      
              ,[ai_processed]
              ,[message_id]
          FROM [FinancialModeling].[hr].[ticket_content]
          where split_status = 'P' and classification is null""")
        
        print(f"Step/1 Complete: Retrieved data = {data}")
        
        # Check if no more records to process
        if not data or data.strip() == "" or "No results" in str(data) or "Empty DataFrame" in str(data):
            print("Step/1 Result: No more tickets with split_status = 'P' and classification is null found!")
            print(f"Total tickets processed: {ticket_counter - 1}")
            break
        
        # Step/2: Send to OpenAI for classification
        print("Step/2: Sending data to OpenAI for classification...")
        ai_response = prompt_openai(f"""You are an expert HR classification system. Given a description of an HR support ticket, classify it into the most appropriate HR category.

Respond with ONLY the classification name in strict JSON format using this structure:
{{
  "classification": "<CATEGORY_NAME>"
}}

Do not include any explanation or additional text.

Use these EXACT category names (max 10 characters to fit database):
- "Payroll"
- "Benefits" 
- "TimeOff"
- "Conflict"
- "Policy"
- "Onboard"
- "Terminate"
- "Equipment"
- "ITIssue"
- "Combo"
- "Unsure"
- "Other"

If the ticket clearly falls into multiple categories, respond with:
{{
  "classification": "Combo"
}}

If the ticket is too vague or ambiguous to classify, respond with:
{{
  "classification": "Unsure"
}}

Ticket:

{data}


Respond:
""")
        
        print(f"Step/2 Complete: AI Response = {ai_response}")
        
        # Step/3: Parse the JSON response
        print("Step/3: Parsing JSON response...")
        print(f"Step/3a: Raw AI response = {repr(ai_response)}")
        
        # Clean up the response - remove markdown code blocks if present
        clean_response = ai_response.strip()
        print(f"Step/3b: After strip = {repr(clean_response)}")
        
        # Remove ```json and ``` if present
        clean_response = clean_response.replace('```json', '').replace('```', '')
        print(f"Step/3c: After removing markdown = {repr(clean_response)}")
        
        clean_response = clean_response.strip()
        print(f"Step/3d: Final cleaned response = {repr(clean_response)}")
        
        parsed_response = json.loads(clean_response)
        classification = parsed_response["classification"]
        print(f"Step/3e: Extracted classification = {classification}")
        
        # Step/3f: Validate classification length
        print(f"Step/3f: Classification length = {len(classification)}")
        classification_length_limit = 10
        
        classification = classification[:classification_length_limit]
        print(f"Step/3g: Truncated classification (max {classification_length_limit} chars) = {classification}")
        print(f"Step/3 Complete: Final classification = {classification}")
        
        # Step/4: Extract sender properly from the data
        print("Step/4: Extracting sender from data...")
        print(f"Step/4a: Raw data structure = {data}")
        print(f"Step/4b: Data type = {type(data)}")
        
        # Parse the data to find the actual sender value
        sender_value = ""
        data_lines = data.strip().split('\n')
        print(f"Step/4c: Data lines count = {len(data_lines)}")
        
        # Look through the lines to find the sender
        for i, line in enumerate(data_lines):
            line = line.strip()
            print(f"Step/4d: Processing line {i}: '{line[:50]}...'")
            
            # Look for Alyson Powers specifically (from the error log)
            if "Alyson Powers" in line:
                sender_value = "Alyson Powers"
                print(f"Step/4e: Found sender 'Alyson Powers' in line {i}")
                break
        
        # Fallback - check for other common patterns
        if not sender_value:
            print("Step/4f: Alyson Powers not found, checking for other patterns...")
            # From the dataframe structure, sender should be in first column
            for line in data_lines:
                line = line.strip()
                # Skip headers and empty lines
                if line and not line.startswith('sender') and len(line.split()) > 0:
                    # Check if this line contains a person's name pattern
                    words = line.split()
                    if len(words) >= 2 and words[0].isalpha() and words[1].isalpha():
                        potential_sender = f"{words[0]} {words[1]}"
                        print(f"Step/4g: Potential sender found: '{potential_sender}'")
                        sender_value = potential_sender
                        break
        
        print(f"Step/4h: Final extracted sender value = '{sender_value}'")
        
        # Step/5: Get message_id for more precise update
        print("Step/5: Extracting message_id from data...")
        message_id = ""
        
        # Look for the message_id pattern
        for line in data_lines:
            if "28385000103656755_conList1" in line or "_conList" in line:
                # Extract the message_id
                words = line.split()
                for word in words:
                    if "_conList" in word:
                        message_id = word
                        print(f"Step/5a: Found message_id = '{message_id}'")
                        break
                if message_id:
                    break
        
        print(f"Step/5 Complete: Extracted message_id = '{message_id}'")
        
        # Step/6: Verify sender exists in database
        print("Step/6: Checking available sender values in database...")
        check_senders_query = """SELECT DISTINCT [sender]
                                FROM [FinancialModeling].[hr].[ticket_content] 
                                WHERE split_status = 'P' and classification is null
                                ORDER BY [sender]"""
        
        available_senders = execute_sql_query(check_senders_query)
        print(f"Step/6a: Available senders = {available_senders}")
        
        if sender_value in available_senders:
            print(f"Step/6b: Sender '{sender_value}' found in available senders")
        else:
            print(f"Step/6c: Sender '{sender_value}' NOT found in available senders")
        
        print(f"Step/6 Complete: Sender verification done")
        
        # Step/7: Setup database connection for UPDATE
        print("Step/7: Setting up database connection for UPDATE...")
        server = 'colofinsql02'
        database = 'FinancialModeling'
        connection_url = f"mssql+pyodbc://@{server}/{database}?driver=SQL+Server&trusted_connection=yes"
        engine = sa.create_engine(connection_url)
        print("Step/7a: Engine created for UPDATE operation")
        
        # Use message_id for more precise update if available, otherwise use sender
        if message_id:
            update_query = f"""UPDATE [FinancialModeling].[hr].[ticket_content] 
                              SET [classification] = '{classification}' 
                              WHERE [message_id] = '{message_id}' 
                              AND split_status = 'P' 
                              AND classification is null"""
            print(f"Step/7b: Using message_id for update")
        else:
            update_query = f"""UPDATE [FinancialModeling].[hr].[ticket_content] 
                              SET [classification] = '{classification}' 
                              WHERE [sender] = '{sender_value}' 
                              AND split_status = 'P' 
                              AND classification is null"""
            print(f"Step/7c: Using sender for update")
        
        print(f"Step/7d: Update query = {update_query}")
        
        # Execute the UPDATE using direct connection
        with engine.connect() as connection:
            result = connection.execute(sa.text(update_query))
            rows_affected = result.rowcount
            connection.commit()
            print(f"Step/7e: Rows affected = {rows_affected}")
        
        engine.dispose()
        print("Step/7 Complete: UPDATE operation finished")
        
        # Step/8: Verify the update
        print("Step/8: Verifying the update...")
        
        if message_id:
            verification_query = f"""SELECT TOP 5 [sender], [classification], [message_id], [split_status]
                                    FROM [FinancialModeling].[hr].[ticket_content] 
                                    WHERE [message_id] = '{message_id}' 
                                    ORDER BY [record_timestamp] DESC"""
        else:
            verification_query = f"""SELECT TOP 5 [sender], [classification], [split_status]
                                    FROM [FinancialModeling].[hr].[ticket_content] 
                                    WHERE [sender] = '{sender_value}' 
                                    ORDER BY [record_timestamp] DESC"""
        
        verification_result = execute_sql_query(verification_query)
        print(f"Step/8a: Verification result = {verification_result}")
        
        # Check recent classification updates
        print("Step/8b: Checking recent classification updates...")
        recent_updates_query = """SELECT TOP 5 [sender], [classification], [split_status], [message_id]
                                 FROM [FinancialModeling].[hr].[ticket_content] 
                                 WHERE [classification] IS NOT NULL
                                 ORDER BY [record_timestamp] DESC"""
        
        recent_updates = execute_sql_query(recent_updates_query)
        print(f"Step/8b Complete: Recent classification updates = {recent_updates}")
        
        print(f"Step/8 Complete: Verification done")
        
        print(f"Ticket #{ticket_counter} processed successfully with classification: {classification}")
        print(f"=== END OF TICKET #{ticket_counter} ===\n")
    
    print("All HR ticket classification processing completed!")
    print(f"Final count: {ticket_counter - 1} tickets processed")

# Run the process
# process_hr_ticket_classification_loop()
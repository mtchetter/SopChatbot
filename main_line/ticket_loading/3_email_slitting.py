"""|05/21/2025 | Banda Ki | """

import pandas as pd
import re
from datetime import datetime
import pyodbc

def parse_emails_to_dataframe(text):
    print("Step/1: Starting email parsing")
    
    # Find all instances of "From:" which indicate the start of an email
    from_positions = [match.start() for match in re.finditer(r"From:", text)]
    print(f"Step/2: Found {len(from_positions)} email starting positions")
    
    # If no From: lines found, try alternate patterns like "To:" as a fallback
    if len(from_positions) == 0:
        print("Step/2.1: No 'From:' lines found, checking for 'To:' lines as fallback")
        from_positions = [match.start() for match in re.finditer(r"To :", text)]
        # If still no matches, try without the space
        if len(from_positions) == 0:
            from_positions = [match.start() for match in re.finditer(r"To:", text)]
            
    # If still no email divisions found, treat the entire text as one email
    if len(from_positions) == 0:
        print("Step/2.2: No email divisions found, treating entire text as one email")
        from_positions = [0]  # Start at the beginning of the text
    
    print(f"Step/2.3: Processing {len(from_positions)} email sections")
    
    # Lists to store extracted information
    senders = []
    original_timestamps = []
    full_contents = []
    
    # Process each email chunk
    for i in range(len(from_positions)):
        start_pos = from_positions[i]
        # If this is the last email, the end position is the end of the text
        end_pos = from_positions[i+1] if i < len(from_positions)-1 else len(text)
        
        # Extract the full email content
        email_text = text[start_pos:end_pos].strip()
        print(f"Step/3.{i+1}: Processing email chunk {i+1}")
        print(f"  - Email starts with: {email_text[:50]}...")
        
        # Save the full content
        full_contents.append(email_text)
        
        # Extract sender - FIXED REGEX TO ONLY GET THE SENDER LINE
        sender_match = re.search(r"From:\s*([^\n\r]+)", email_text)
        if sender_match:
            sender = sender_match.group(1).strip()
            print(f"  - Found sender: {sender}")
            senders.append(sender)
        else:
            print("  - No sender found, checking for alternative patterns")
            # Try alternative patterns that might indicate sender
            alt_sender_match = re.search(r"From\s*:\s*([^\n\r]+)", email_text, re.IGNORECASE)
            if alt_sender_match:
                sender = alt_sender_match.group(1).strip()
                print(f"  - Found alternative sender: {sender}")
                senders.append(sender)
            else:
                print("  - No sender found")
                senders.append("")
            
        # Extract timestamp
        timestamp_match = re.search(r"Sent:\s*([^\n\r]+)", email_text)
        if timestamp_match:
            original_timestamp = timestamp_match.group(1).strip()
            print(f"  - Found timestamp: {original_timestamp}")
            original_timestamps.append(original_timestamp)
        else:
            # Try alternative timestamp patterns
            alt_timestamp_match = re.search(r"(?:Sent|Date):\s*([^\n\r]+)", email_text, re.IGNORECASE)
            if alt_timestamp_match:
                original_timestamp = alt_timestamp_match.group(1).strip()
                print(f"  - Found alternative timestamp: {original_timestamp}")
                original_timestamps.append(original_timestamp)
            else:
                print("  - No timestamp found")
                original_timestamps.append("")
        
        # Extract recipients (To:) - just for debugging
        recipient_match = re.search(r"To:\s*([^\n\r]+)", email_text)
        if recipient_match:
            recipient = recipient_match.group(1).strip()
            print(f"  - Found recipient: {recipient}")
        else:
            alt_recipient_match = re.search(r"To\s*:\s*([^\n\r]+)", email_text, re.IGNORECASE)
            if alt_recipient_match:
                recipient = alt_recipient_match.group(1).strip()
                print(f"  - Found alternative recipient: {recipient}")
            else:
                print("  - No recipient found")
            
        # Extract subject - just for debugging
        subject_match = re.search(r"Subject:\s*([^\n\r]+)", email_text)
        if subject_match:
            subject = subject_match.group(1).strip()
            print(f"  - Found subject: {subject}")
        else:
            alt_subject_match = re.search(r"Subject\s*:\s*([^\n\r]+)", email_text, re.IGNORECASE)
            if alt_subject_match:
                subject = alt_subject_match.group(1).strip()
                print(f"  - Found alternative subject: {subject}")
            else:
                re_subject_match = re.search(r"Re:\s*([^\n\r]+)", email_text)
                if re_subject_match:
                    subject = "Re: " + re_subject_match.group(1).strip()
                    print(f"  - Found Re: subject: {subject}")
                else:
                    print("  - No subject found")
        
        # Debug: Print what we're about to store as sender
        print(f"  - Final sender value (length {len(senders[-1])}): '{senders[-1]}'")
    
    print("\nStep/4: Creating DataFrame")
    # Create DataFrame with only the columns we need for the database
    df = pd.DataFrame({
        'Sender': senders,
        'Timestamp': original_timestamps,
        'Content': full_contents
    })
    
    print(f"Step/5: Successfully created DataFrame with {len(df)} rows")
    print("Step/5.1: DataFrame sender values:")
    for idx, sender in enumerate(df['Sender']):
        print(f"  - Row {idx}: '{sender}' (length: {len(sender)})")
    
    return df

def process_email_records():
    print("Banda Ki - Email Chain Splitter - SQL Only")
    print("Step/1: Connecting to SQL Server")
    
    # Use the 'SQL Server' driver which we know works
    conn_str = (
        'DRIVER={SQL Server};'
        'SERVER=colofinsql02;'
        'DATABASE=FinancialModeling;'
        'Trusted_Connection=yes;'
    )
    
    conn = pyodbc.connect(conn_str)
    print("Step/2: Successfully connected to SQL Server")
    
    total_records_processed = 0
    
    # Continue processing records until no more are found
    while True:
        # Get records that need to be processed
        print("\nStep/3: Fetching next record that needs to be split")
        cursor = conn.cursor()
        
        select_query = """
        SELECT TOP (1) 
            [content],
            [message_id],
            [userid],
            [record_timestamp]
        FROM [FinancialModeling].[hr].[ticket_content]
        WHERE split_status IS NULL
        """
        
        print("Step/3.1: Executing query: " + select_query)
        cursor.execute(select_query)
        row = cursor.fetchone()
        
        print(f"Step/3.2: Query executed, result type: {type(row)}")
        
        if not row:
            print("Step/4: No more records found to process")
            break
        
        print(f"Step/4.1: Row has {len(row) if row else 0} columns")
        for i in range(len(row)):
            print(f"  - Column {i} type: {type(row[i])}, value: {str(row[i])[:50]}...")
        
        # Extract all values
        content = row[0]
        message_id = row[1]
        userid = row[2]
        record_timestamp = row[3]
        
        # Ensure content is a string
        if content is None:
            print("Step/4.2: Content is None, using empty string instead")
            content = ""
        elif not isinstance(content, str):
            print(f"Step/4.3: Content is not a string (type: {type(content)}), converting")
            content = str(content)
        
        print(f"Step/5: Processing content with message_id: {message_id}")
        print(f"  - Content length: {len(content)} characters")
        print(f"  - Content start: {content[:100]}...")
        print(f"  - User ID: {userid}")
        print(f"  - Record timestamp: {record_timestamp}")
        
        # Parse emails and create DataFrame
        emails_df = parse_emails_to_dataframe(content)
        
        # Insert split records
        if len(emails_df) > 0:
            print(f"\nStep/6: Inserting {len(emails_df)} split email records with message_id: {message_id}")
            
            cursor = conn.cursor()
            records_inserted = 0
            
            for index, row_data in emails_df.iterrows():
                # Truncate sender if it's too long for the database column
                sender_value = row_data['Sender']
                if len(sender_value) > 50:  # Database sender column appears to be VARCHAR(50)
                    print(f"Step/6.{records_inserted+1}: Truncating sender from {len(sender_value)} to 50 characters")
                    print(f"  - Original: '{sender_value}'")
                    sender_value = sender_value[:50]
                    print(f"  - Truncated: '{sender_value}'")
                
                print(f"Step/6.{records_inserted+1}: About to insert sender: '{sender_value}' (length: {len(sender_value)})")
                
                insert_query = """
                INSERT INTO [FinancialModeling].[hr].[ticket_content]
                ([content], [sender], [timestamp], [message_id], [userid], [record_timestamp], [split_status])
                VALUES (?, ?, ?, ?, ?, ?, '2')
                """
                
                cursor.execute(
                    insert_query,
                    row_data['Content'],
                    sender_value,
                    row_data['Timestamp'],
                    message_id,
                    userid,
                    record_timestamp
                )
                records_inserted += 1
                
            conn.commit()
            print(f"Step/7: Successfully inserted {records_inserted} split email records")
            
            # Update the original record as processed
            print(f"\nStep/8: Updating original record with message_id: {message_id} as processed")
            
            cursor = conn.cursor()
            update_query = """
            UPDATE [FinancialModeling].[hr].[ticket_content]
            SET split_status = 'P'
            WHERE message_id = ? AND split_status IS NULL
            """
            
            cursor.execute(update_query, message_id)
            rows_affected = cursor.rowcount
            conn.commit()
            print(f"Step/9: Successfully marked {rows_affected} record(s) as processed")
            total_records_processed += 1
        else:
            print("Step/6: No emails found to insert, marking record with split_status='1'")
            # Mark with status '1' instead of 'P' for records with no split emails
            cursor = conn.cursor()
            update_query = """
            UPDATE [FinancialModeling].[hr].[ticket_content]
            SET split_status = '1'
            WHERE message_id = ? AND split_status IS NULL
            """
            cursor.execute(update_query, message_id)
            rows_affected = cursor.rowcount
            conn.commit()
            print(f"Step/7: Successfully marked {rows_affected} record(s) with split_status='1'")
            total_records_processed += 1
    
    conn.close()
    print("\nStep/10: All processing completed")
    print(f"Step/11: Successfully processed {total_records_processed} total records")
    print("Step/12: Connection closed")

if __name__ == "__main__":
    process_email_records()
    print("\nProcess completed!")
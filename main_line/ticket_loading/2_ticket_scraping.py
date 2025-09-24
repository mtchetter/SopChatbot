"""| 5/21/2025 | BandaKi | In-Progress | Taking a ticket URL and extracting all messages from the ticket page. Currently it 
will take like half and half of the messages, but it will fail on a lot of them. The current code has more debugging
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import pandas as pd
import pyodbc
import sqlalchemy as sa
from main_line.all_tools import execute_sql_query


def navigate_to_hei_page():
    # Step/1 - Setup Chrome driver once for the entire loop
    print("Step/1: Setting up Chrome driver...")
    driver = webdriver.Chrome()
    print("DEBUG: Chrome driver initialized")
    
    # Step/2 - Start the main processing loop
    print("Step/2: Starting ticket processing loop...")
    ticket_count = 0
    is_first_ticket = True
    
    while True:
        print(f"\nStep/2.{ticket_count + 1}: Starting processing of ticket #{ticket_count + 1}")
        
        # Step/3 - Get ticket URL from database
        print("Step/3: Executing SQL query to get next unprocessed ticket URL...")
        ticket_result = execute_sql_query("""SELECT TOP (1) [ticket_link]
        FROM [FinancialModeling].[hr].[hr_tickets]
        where processed_status is null""")
        
        print("DEBUG: Raw query result:")
        print(ticket_result)
        print(f"DEBUG: Result type: {type(ticket_result)}")
        
        # Step/4 - Check if there are any more tickets to process
        print("Step/4: Checking if any unprocessed tickets remain...")
        
        # Handle if result is a string (which it appears to be)
        ticket_url = None
        if isinstance(ticket_result, str):
            print("DEBUG: Result is a string, extracting URL from string...")
            # Split by newlines and find the line with the actual URL
            lines = ticket_result.strip().split('\n')
            print(f"DEBUG: Found {len(lines)} lines in result")
            
            for i, line in enumerate(lines):
                print(f"DEBUG: Line {i}: '{line}'")
                # Look for line that starts with https://
                if line.strip().startswith('https://'):
                    ticket_url = line.strip()
                    print(f"DEBUG: Found URL in line {i}: '{ticket_url}'")
                    break
            
            # Check if no URL was found (empty result)
            if not ticket_url and len(lines) <= 2:
                print("DEBUG: No more unprocessed tickets found - result appears empty")
                break
        else:
            print("DEBUG: Result is not a string, attempting DataFrame access...")
            # If it's actually a DataFrame, check if it's empty
            if len(ticket_result) == 0:
                print("DEBUG: No more unprocessed tickets found - DataFrame is empty")
                break
            ticket_url = ticket_result.iloc[0, 0]
        
        # Step/5 - Break loop if no more tickets
        if not ticket_url:
            print("Step/5: No more unprocessed tickets found. Ending processing loop.")
            break
        
        print(f"DEBUG: Extracted URL: '{ticket_url}'")
        print(f"DEBUG: URL type: {type(ticket_url)}")
        print(f"DEBUG: URL length: {len(str(ticket_url))}")
        
        # Step/6 - Clean and validate the URL
        print("Step/6: Cleaning and validating URL...")
        ticket_url = str(ticket_url).strip()
        print(f"DEBUG: Cleaned URL: '{ticket_url}'")
        
        # Validate URL format
        if not ticket_url.startswith(('http://', 'https://')):
            print(f"ERROR: Invalid URL format - URL must start with http:// or https://")
            print(f"Current URL: '{ticket_url}'")
            print("DEBUG: Skipping this ticket and continuing to next one...")
            # Mark this ticket as processed with error status
            update_ticket_processed_status_error(ticket_url)
            continue
        
        print(f"DEBUG: URL validation passed")

        # Step/7 - Navigate directly to the target URL
        print("Step/7: Navigating to support ticket page...")
        print(f"DEBUG: About to navigate to: {ticket_url}")
        
        driver.get(ticket_url)
        print("DEBUG: Navigation completed successfully")
        
        # Step/8 - Wait for page to load
        print("Step/8: Waiting for page to load...")
        time.sleep(5)
        print("DEBUG: Page load wait completed")
        
        # Step/9 - Handle login only for the first ticket
        if is_first_ticket:
            print("Step/9: First ticket - performing login...")
            
            # Step/9.1 - Input email address (assuming the text field is already active)
            print("Step/9.1: Entering email address...")
            active_element = driver.switch_to.active_element
            print(f"DEBUG: Active element tag: {active_element.tag_name}")
            print(f"DEBUG: Active element type: {active_element.get_attribute('type')}")
            
            # Try to clear the field, but handle if it fails
            print("DEBUG: Attempting to clear active element...")
            try:
                active_element.clear()
                print("DEBUG: Successfully cleared active element")
            except Exception as e:
                print(f"DEBUG: Could not clear active element: {e}")
                print("DEBUG: Proceeding without clearing...")
            
            # Send the email address
            print("DEBUG: Sending email keys...")
            active_element.send_keys("mtschetter@heihotels.com")
            print("DEBUG: Email entered into active element")
            
            # Step/9.2 - Click the first Next button using the provided XPath
            print("Step/9.2: Clicking the first Next button...")
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="nextbtn"]'))
            )
            next_button.click()
            print("DEBUG: First Next button clicked successfully")
            
            # Step/9.3 - Wait for password field to appear and enter password
            print("Step/9.3: Waiting for password field and entering password...")
            password_field = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.ID, "password"))
            )
            password_field.clear()
            password_field.send_keys("California.1")
            print("DEBUG: Password entered successfully")
            
            # Step/9.4 - Click the second Next button (same XPath)
            print("Step/9.4: Clicking the second Next button...")
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="nextbtn"]'))
            )
            next_button.click()
            print("DEBUG: Second Next button clicked successfully")
            
            # Step/9.5 - Wait for the page to load after login
            print("Step/9.5: Waiting for page to load after login...")
            time.sleep(8)  # Increased wait time to ensure page loads fully
            print("DEBUG: Post-login wait completed")
            
            # Mark that we've completed the first ticket login
            is_first_ticket = False
            print("DEBUG: Login completed - subsequent tickets will skip login process")
        else:
            print("Step/9: Subsequent ticket - skipping login (already authenticated)")
            # Just wait a bit for the authenticated page to load
            time.sleep(3)
            print("DEBUG: Authenticated page load wait completed")
        
        # Step/10 - Extract all chat messages from the ticket page and create dataframe
        print("Step/10: Extracting all messages from the ticket page...")
        messages_df = extract_all_messages(driver, ticket_url)
        print(f"DEBUG: Extracted {len(messages_df)} messages")
        
        # Step/11 - Save messages to CSV file only
        print("Step/11: Saving messages to file...")
        save_messages_to_file(messages_df, ticket_count + 1)
        print("DEBUG: Messages saved to file")
        
        # Step/12 - Save messages to SQL Server
        print("Step/12: Saving messages to SQL Server...")
        save_messages_to_sql(messages_df)
        print("DEBUG: Messages saved to SQL Server")
        
        # Step/13 - Print messages to console
        print("Step/13: Printing messages to console...")
        print_messages(messages_df)
        print("DEBUG: Messages printed to console")
        
        # Step/14 - Update ticket processed status in database using SQLAlchemy
        print("Step/14: Updating ticket processed status in database...")
        update_ticket_processed_status(ticket_url)
        print("DEBUG: Ticket processed status updated successfully")
        
        # Step/15 - Increment counter and prepare for next iteration
        ticket_count += 1
        print(f"Step/15: Completed processing ticket #{ticket_count}. Preparing for next ticket...")
        print("DEBUG: " + "="*60)
    
    # Step/16 - All tickets processed, keep browser open for manual review
    print(f"\nStep/16: All tickets processed! Total tickets handled: {ticket_count}")
    print("DEBUG: The script will now wait. The browser will stay open for manual review.")
    print("DEBUG: You can manually interact with the page.")
    
    # This will keep the script running and browser open
    input("Press Enter in this console when you want to close the browser...")
    
    # Step/17 - Close the browser when user is ready
    print("Step/17: Closing browser...")
    driver.quit()
    print("DEBUG: Browser closed successfully")

def update_ticket_processed_status(ticket_url):
    # Step/14.1 - Create connection parameters
    print("Step/14.1: Setting up connection parameters")
    server = 'colofinsql02'
    database = 'FinancialModeling'
    print(f"DEBUG: Server: {server}")
    print(f"DEBUG: Database: {database}")
    
    # Step/18.2 - Create SQLAlchemy connection URL format
    print("Step/18.2: Creating SQLAlchemy connection URL")
    connection_url = f"mssql+pyodbc://@{server}/{database}?driver=SQL+Server&trusted_connection=yes"
    print(f"DEBUG: Connection URL created: {connection_url}")
    
    # Step/18.3 - Create engine
    print("Step/18.3: Creating SQLAlchemy engine")
    engine = sa.create_engine(connection_url)
    print("DEBUG: Engine created successfully")
    
    # Step/18.4 - Execute update query
    print("Step/18.4: Executing update query to mark ticket as processed")
    update_query = f"""
    UPDATE [FinancialModeling].[hr].[hr_tickets] 
    SET [processed_status] = 1 
    WHERE [ticket_link] = '{ticket_url}' AND processed_status is null
    """
    print(f"DEBUG: Update query: {update_query}")
    
    # Step/18.5 - Execute the query
    print("Step/18.5: Executing the update query...")
    with engine.connect() as connection:
        result = connection.execute(sa.text(update_query))
        connection.commit()
        print(f"DEBUG: Query executed successfully")
        print(f"DEBUG: Rows affected: {result.rowcount}")
    
    print("Step/18.6: Update operation completed")

def update_ticket_processed_status_error(ticket_url):
    # Step/Error.1 - Create connection parameters for error status
    print("Step/Error.1: Setting up connection parameters for error status")
    server = 'colofinsql02'
    database = 'FinancialModeling'
    print(f"DEBUG: Server: {server}")
    print(f"DEBUG: Database: {database}")
    
    # Step/Error.2 - Create SQLAlchemy connection URL format
    print("Step/Error.2: Creating SQLAlchemy connection URL")
    connection_url = f"mssql+pyodbc://@{server}/{database}?driver=SQL+Server&trusted_connection=yes"
    print(f"DEBUG: Connection URL created: {connection_url}")
    
    # Step/Error.3 - Create engine
    print("Step/Error.3: Creating SQLAlchemy engine")
    engine = sa.create_engine(connection_url)
    print("DEBUG: Engine created successfully")
    
    # Step/Error.4 - Execute update query with error status
    print("Step/Error.4: Executing update query to mark ticket as error")
    update_query = f"""
    UPDATE [FinancialModeling].[hr].[hr_tickets] 
    SET [processed_status] = -1 
    WHERE [ticket_link] = '{ticket_url}' AND processed_status is null
    """
    print(f"DEBUG: Update query: {update_query}")
    
    # Step/Error.5 - Execute the query
    print("Step/Error.5: Executing the error update query...")
    with engine.connect() as connection:
        result = connection.execute(sa.text(update_query))
        connection.commit()
        print(f"DEBUG: Query executed successfully")
        print(f"DEBUG: Rows affected: {result.rowcount}")
    
    print("Step/Error.6: Error status update operation completed")

def extract_all_messages(driver, ticket_url):
    print("Step/1: Looking for the most recent message in the ticket...")
    
    # Create an empty dataframe to store the most recent message
    messages_df = pd.DataFrame(columns=["sender", "timestamp", "content", "message_id", "ticket_link"])
    print("DEBUG: Created empty messages DataFrame")
    
    # Get the ticket ID from the URL
    current_url = driver.current_url
    ticket_id = current_url.split("/")[-2]
    print(f"Step/2: Processing ticket ID: {ticket_id}")
    print(f"DEBUG: Current URL: {current_url}")
    
    # Wait for conversation elements to be present
    print("Step/3: Waiting for conversation thread elements...")
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CLASS_NAME, "conversation-thread-bg"))
    )
    print("DEBUG: Found conversation thread background elements")
    
    # Find all thread containers
    print("Step/4: Finding all thread containers...")
    thread_containers = []
    
    selectors = [
        ".wo-start-thread",
        ".conversion-open-thread", 
        "tr[id^='conListDesc']",
        "tr[id^='conList']",
        ".wo-thread-container"
    ]
    
    for selector in selectors:
        containers = driver.find_elements(By.CSS_SELECTOR, selector)
        print(f"DEBUG: Found {len(containers)} threads using selector '{selector}'")
        thread_containers.extend(containers)
    
    print(f"DEBUG: Total containers found before deduplication: {len(thread_containers)}")
    
    # Remove duplicates
    unique_ids = set()
    unique_containers = []
    
    for container in thread_containers:
        container_id = container.get_attribute("id") or container.get_attribute("data-id") or "unknown"
        print(f"DEBUG: Container ID found: {container_id}")
        if container_id not in unique_ids:
            unique_ids.add(container_id)
            unique_containers.append(container)
    
    thread_containers = unique_containers
    print(f"Step/5: Total unique conversation threads found: {len(thread_containers)}")
    
    # Look for expansion buttons
    print("Step/6: Looking for expansion buttons...")
    expand_buttons = driver.find_elements(By.CSS_SELECTOR, ".expand-all-conversation, .show-all-conversations")
    for button in expand_buttons:
        print(f"DEBUG: Found expansion button: {button.get_attribute('class')}")
        if button.is_displayed():
            button.click()
            print("DEBUG: Clicked button to expand all conversations")
            time.sleep(2)
            break
    
    # Find the most recent message WITH PROPER DATE PARSING
    print("Step/7: Finding the most recent message with proper date comparison...")
    
    from datetime import datetime
    
    most_recent_container = None
    most_recent_timestamp_text = None
    most_recent_datetime = None
    
    for i, container in enumerate(thread_containers):
        print(f"DEBUG: Processing container {i+1}/{len(thread_containers)}")
        print(f"DEBUG: Container tag: {container.tag_name}")
        print(f"DEBUG: Container classes: {container.get_attribute('class')}")
        
        # Try to get timestamp with more debugging
        time_selectors = [".wo-con-timestamp", ".timestamp", ".message-date", ".date-time"]
        timestamp_text = None
        
        for selector in time_selectors:
            time_elems = container.find_elements(By.CSS_SELECTOR, selector)
            print(f"DEBUG: Found {len(time_elems)} timestamp elements with selector '{selector}'")
            for time_elem in time_elems:
                elem_text = time_elem.text.strip(" -")
                print(f"DEBUG: Timestamp element text: '{elem_text}'")
                if elem_text:
                    timestamp_text = elem_text
                    break
            if timestamp_text:
                break
        
        print(f"DEBUG: Final timestamp for container {i+1}: {timestamp_text}")
        
        if timestamp_text:
            print(f"DEBUG: Found message with timestamp: {timestamp_text}")
            
            # PROPER DATE PARSING INSTEAD OF STRING COMPARISON
            print("DEBUG: Attempting to parse timestamp into datetime...")
            try:
                # Parse timestamps like "Apr 25, 2025 11:47 AM" 
                parsed_datetime = datetime.strptime(timestamp_text, "%b %d, %Y %I:%M %p")
                print(f"DEBUG: Successfully parsed datetime: {parsed_datetime}")
                
                # Compare with current most recent
                if most_recent_datetime is None or parsed_datetime > most_recent_datetime:
                    most_recent_datetime = parsed_datetime
                    most_recent_timestamp_text = timestamp_text
                    most_recent_container = container
                    print(f"DEBUG: NEW most recent message found: {timestamp_text}")
                else:
                    print(f"DEBUG: This message is older than current most recent: {most_recent_timestamp_text}")
                    
            except ValueError as e:
                print(f"DEBUG: Could not parse timestamp '{timestamp_text}': {e}")
                print("DEBUG: Falling back to string comparison for this timestamp")
                # Fallback to string comparison if parsing fails
                if not most_recent_timestamp_text or timestamp_text > most_recent_timestamp_text:
                    most_recent_timestamp_text = timestamp_text
                    most_recent_container = container
                    print(f"DEBUG: Setting as current most recent message (string comparison)")
    
    print(f"DEBUG: Final most recent message: {most_recent_timestamp_text}")
    
    # Process the most recent container
    if most_recent_container:
        print("Step/8: Processing the most recent message")
        container = most_recent_container
        
        # BANDA KI FIX: Get message ID and make it unique per ticket
        original_message_id = container.get_attribute("id") or "most_recent_message"
        # Make the message_id unique by combining ticket_id with original message_id
        message_id = f"{ticket_id}_{original_message_id}"
        print(f"DEBUG: Original Message ID: {original_message_id}")
        print(f"DEBUG: Unique Message ID: {message_id}")
        
        # Get sender with debugging
        print("Step/9: Extracting sender information...")
        sender_selectors = [".users-info", ".user-name", ".sender-name", ".from-name"]
        sender = "Unknown"
        
        for selector in sender_selectors:
            sender_elems = container.find_elements(By.CSS_SELECTOR, selector)
            print(f"DEBUG: Found {len(sender_elems)} sender elements with selector '{selector}'")
            for sender_elem in sender_elems:
                sender_text = sender_elem.text.strip()
                print(f"DEBUG: Sender element text: '{sender_text}'")
                if sender_text:
                    sender = sender_text
                    break
            if sender != "Unknown":
                break
        
        print(f"DEBUG: Final sender: {sender}")
        
        timestamp = most_recent_timestamp_text or "Unknown"
        print(f"DEBUG: Final timestamp: {timestamp}")
        
        # Step/10: ENHANCED MESSAGE EXPANSION WITH MULTIPLE ATTEMPTS
        print("Step/10: Attempting to expand message with multiple strategies...")
        clickable_selectors = [
            ".conversation-head", 
            ".message-header",
            ".expand-message", 
            ".wo-mail-msg",
            "[onclick*='openConversationThread']",
            ".conversation-head-section",
            ".message-expand-button"
        ]
        
        clicked = False
        for selector in clickable_selectors:
            elements = container.find_elements(By.CSS_SELECTOR, selector)
            print(f"DEBUG: Found {len(elements)} clickable elements with selector '{selector}'")
            for elem in elements:
                print(f"DEBUG: Clickable element displayed: {elem.is_displayed()}")
                print(f"DEBUG: Clickable element enabled: {elem.is_enabled()}")
                if elem.is_displayed():
                    driver.execute_script("arguments[0].click();", elem)
                    clicked = True
                    print("DEBUG: JavaScript clicked to expand message")
                    time.sleep(2)  # Increased wait time
                    break
            if clicked:
                break
        
        print(f"DEBUG: Message expansion clicked: {clicked}")
        
        # Step/11: ENHANCED CONTENT EXTRACTION WITH WAIT AND RETRY
        print("Step/11: Enhanced content extraction with wait and retry...")
        content_selectors = [
            ".conversation-container", 
            ".preline", 
            ".x_1199246650WordSection1",
            ".zm_2301649238697898930_parse_5217372361992668587",
            ".message-body",
            ".message-content",
            "[id^='notiDesc_']",
            ".message-text",
            ".content-body",
            ".email-content",
            ".thread-content"
        ]
        
        content = ""
        content_sources = []
        
        print("Step/12: First attempt - searching for content within container...")
        for selector in content_selectors:
            elements = container.find_elements(By.CSS_SELECTOR, selector)
            print(f"DEBUG: Selector '{selector}' found {len(elements)} elements in container")
            
            for j, elem in enumerate(elements):
                elem_text = elem.text.strip()
                elem_html = elem.get_attribute('innerHTML')
                print(f"DEBUG: Element {j+1} text length: {len(elem_text)}")
                print(f"DEBUG: Element {j+1} HTML length: {len(elem_html)}")
                print(f"DEBUG: Element {j+1} text preview: '{elem_text[:100]}...' " if len(elem_text) > 100 else f"DEBUG: Element {j+1} text: '{elem_text}'")
                
                if elem_text and len(elem_text.strip()) > 10:
                    content += elem_text + "\n"
                    content_sources.append(f"container-{selector}")
                    print(f"DEBUG: Added content chunk of length {len(elem_text)} from container-{selector}")
        
        # Step/13: If no content found, wait and try again with different approach
        if not content:
            print("Step/13: No content found in first attempt, trying expanded area and waiting...")
            
            # Wait for content to load
            time.sleep(3)
            print("DEBUG: Waited 3 seconds for content loading")
            
            # Try expanded area with message ID
            if original_message_id != "unknown":
                expanded_id = f"notify{original_message_id.replace('conList', '')}_CUR"
                print(f"DEBUG: Looking for expanded content with ID: {expanded_id}")
                
                expanded_elems = driver.find_elements(By.ID, expanded_id)
                print(f"DEBUG: Found {len(expanded_elems)} elements with expanded ID")
                
                for expanded_elem in expanded_elems:
                    for selector in content_selectors:
                        elements = expanded_elem.find_elements(By.CSS_SELECTOR, selector)
                        print(f"DEBUG: Selector '{selector}' found {len(elements)} elements in expanded area")
                        
                        for j, elem in enumerate(elements):
                            elem_text = elem.text.strip()
                            print(f"DEBUG: Expanded element {j+1} text length: {len(elem_text)}")
                            if elem_text and len(elem_text.strip()) > 10:
                                content += elem_text + "\n"
                                content_sources.append(f"expanded-{selector}")
                                print(f"DEBUG: Added content chunk of length {len(elem_text)} from expanded-{selector}")
        
        # Step/14: If still no content, try scrolling and waiting approach
        if not content:
            print("Step/14: Still no content, trying scroll and wait approach...")
            
            # Scroll to the container to ensure it's visible
            driver.execute_script("arguments[0].scrollIntoView(true);", container)
            time.sleep(2)
            print("DEBUG: Scrolled container into view")
            
            # Try clicking again with JavaScript
            driver.execute_script("arguments[0].click();", container)
            print("DEBUG: JavaScript clicked on container itself")
            time.sleep(3)
            
            # Try content extraction one more time
            for selector in content_selectors:
                elements = container.find_elements(By.CSS_SELECTOR, selector)
                print(f"DEBUG: RETRY - Selector '{selector}' found {len(elements)} elements in container")
                
                for j, elem in enumerate(elements):
                    elem_text = elem.text.strip()
                    print(f"DEBUG: RETRY - Element {j+1} text length: {len(elem_text)}")
                    if elem_text and len(elem_text.strip()) > 10:
                        content += elem_text + "\n"
                        content_sources.append(f"retry-{selector}")
                        print(f"DEBUG: RETRY - Added content chunk of length {len(elem_text)} from retry-{selector}")
        
        # Step/15: If still no content, try HTML innerHTML parsing approach
        if not content:
            print("Step/15: Still no content, trying HTML innerHTML parsing approach...")
            
            for selector in content_selectors:
                elements = container.find_elements(By.CSS_SELECTOR, selector)
                print(f"DEBUG: HTML_PARSE - Selector '{selector}' found {len(elements)} elements in container")
                
                for j, elem in enumerate(elements):
                    elem_html = elem.get_attribute('innerHTML')
                    print(f"DEBUG: HTML_PARSE - Element {j+1} HTML length: {len(elem_html)}")
                    
                    if elem_html and len(elem_html.strip()) > 50:
                        print(f"DEBUG: HTML_PARSE - Found HTML content, attempting to extract text...")
                        # Try to extract text from HTML using different methods
                        
                        # Method 1: Use driver.execute_script to get textContent
                        text_content = driver.execute_script("return arguments[0].textContent;", elem)
                        print(f"DEBUG: HTML_PARSE - textContent method yielded {len(text_content)} characters")
                        if text_content and len(text_content.strip()) > 10:
                            content += text_content.strip() + "\n"
                            content_sources.append(f"html_textContent-{selector}")
                            print(f"DEBUG: HTML_PARSE - Added {len(text_content)} chars from textContent method")
                            break
                
                if content:  # If we found content, break out of selector loop
                    break
        
        # Final content processing
        if content:
            content = content.strip()
            content_preview = content[:100] + "..." if len(content) > 100 else content
            print(f"DEBUG: Final content preview: {content_preview}")
            print(f"DEBUG: Content sources used: {content_sources}")
            print(f"DEBUG: Total content length: {len(content)}")
        else:
            content = "No content found - check selectors"
            print("DEBUG: NO CONTENT FOUND - All selectors and retries failed")
        
        # Add to dataframe with the UNIQUE message_id
        new_row = {
            "sender": sender,
            "timestamp": timestamp,
            "content": content,
            "message_id": message_id,  # This is now unique!
            "ticket_link": ticket_url
        }
        
        messages_df = pd.concat([messages_df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"DEBUG: Added message to dataframe with unique message_id: {message_id}")
        
    else:
        print("Step/8: Could not identify most recent message")
        # BANDA KI FIX: Make error message_id unique too
        unique_error_id = f"{ticket_id}_error_unknown"
        error_row = {
            "sender": "Unknown",
            "timestamp": "Unknown", 
            "content": "Could not identify most recent message",
            "message_id": unique_error_id,
            "ticket_link": ticket_url
        }
        messages_df = pd.concat([messages_df, pd.DataFrame([error_row])], ignore_index=True)
        print(f"DEBUG: Added error placeholder row with unique message_id: {unique_error_id}")
    
    print(f"Step/15: Total messages extracted: {len(messages_df)}")
    return messages_df
    

def save_messages_to_file(df, ticket_number):
    print("Step/15.1: Saving messages to CSV file")
    
    # Save as CSV with ticket number in filename
    filename = f"hei_messages_ticket_{ticket_number}.csv"
    df.to_csv(filename, index=False, encoding="utf-8")
    print(f"Step/15.2: Messages saved to {filename}")
    print(f"DEBUG: Saved {len(df)} rows to CSV file")

def save_messages_to_sql(df):
    print("Step/16.1: Establishing SQL Server connection")
    # Use the 'SQL Server' driver which we know works
    conn_str = (
        'DRIVER={SQL Server};'
        'SERVER=colofinsql02;'
        'DATABASE=FinancialModeling;'
        'Trusted_Connection=yes;'
    )
    print("DEBUG: Connection string prepared")
    
    print("Step/16.2: Insert data into SQL Server table")
    print("DEBUG: Connecting with SQL Server driver...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("DEBUG: Connection established successfully")
    
    print(f"Step/16.3: Upserting {len(df)} messages into ticket_content table")
    
    # Set userid and current timestamp
    userid = "mtschetter@heihotels.com"  # Using the email from the login process
    print(f"DEBUG: Using userid: {userid}")
    
    # First check for existing rows and update
    updated_count = 0
    inserted_count = 0
    
    for i, (_, row) in enumerate(df.iterrows()):
        print(f"DEBUG: Processing row {i+1}/{len(df)}")
        # Check if the message_id already exists
        cursor.execute(
            "SELECT COUNT(*) FROM financialmodeling.hr.ticket_content WHERE message_id = ?", 
            row['message_id']
        )
        count = cursor.fetchone()[0]
        print(f"DEBUG: Found {count} existing records for message_id: {row['message_id']}")
        
        if count > 0:
            # Update existing row
            print(f"Step/16.4: Updating message_id: {row['message_id']}")
            cursor.execute(
                """
                UPDATE financialmodeling.hr.ticket_content 
                SET sender = ?, timestamp = ?, content = ?, 
                    userid = ?, record_timestamp = GETDATE()
                WHERE message_id = ?
                """,
                row['sender'], row['timestamp'], row['content'], 
                userid, row['message_id']
            )
            updated_count += 1
            print(f"DEBUG: Updated record for message_id: {row['message_id']}")
        else:
            # Insert new row
            print(f"Step/16.5: Inserting new message_id: {row['message_id']}")
            cursor.execute(
                """
                INSERT INTO financialmodeling.hr.ticket_content 
                (sender, timestamp, content, message_id, userid, record_timestamp) 
                VALUES (?, ?, ?, ?, ?, GETDATE())
                """,
                row['sender'], row['timestamp'], row['content'], 
                row['message_id'], userid
            )
            inserted_count += 1
            print(f"DEBUG: Inserted new record for message_id: {row['message_id']}")
            
        # Commit after each 10 rows to avoid long transactions
        if (updated_count + inserted_count) % 10 == 0:
            conn.commit()
            print(f"Step/16.6: Committed {updated_count + inserted_count} rows")
    
    # Final commit for any remaining rows
    conn.commit()
    print(f"Step/16.7: SQL Server update complete: {updated_count} rows updated, {inserted_count} rows inserted")
    print(f"DEBUG: Total operations completed: {updated_count + inserted_count}")
    
    conn.close()
    print("Step/16.8: SQL Server connection closed")

def print_messages(df):
    print("Step/17.1: Printing extracted messages")
    print(f"DEBUG: Total messages found: {len(df)}")
    
    # Print dataframe column information
    print("Step/17.2: DataFrame Info:")
    print(df.info())
    
    # Print summary of first few messages
    print("Step/17.3: Message Summary:")
    for i, row in df.iterrows():
        if i >= 5:  # Limit to first 5 messages
            remaining = len(df) - 5
            if remaining > 0:
                print(f"DEBUG: ...and {remaining} more messages. See output files for all data.")
            break
            
        print(f"DEBUG: Message {i+1}:")
        print(f"DEBUG: From: {row['sender']}")
        print(f"DEBUG: Time: {row['timestamp']}")
        
        # Print content preview 
        content = row['content']
        if len(content) > 100:
            preview = content[:100] + "..."
            print(f"DEBUG: Content (preview): {preview}")
        else:
            print(f"DEBUG: Content: {content}")
            
        print(f"DEBUG: Ticket Link: {row['ticket_link']}")  # Print ticket link in console summary
        print("DEBUG: " + "-" * 50)
    
    print("Step/17.4: Message printing completed")

# Run the function
if __name__ == "__main__":
    print("Starting script...")
    navigate_to_hei_page()
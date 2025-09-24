# import win32com.client as win32

# def modify_cutoff_month():
#     print("Step 1: Connecting to Excel application")
#     xl = win32.gencache.EnsureDispatch('Excel.Application')
#     xl.Visible = False  # Keep Excel hidden
    
#     print("Step 2: Disabling Excel alerts and popups")
#     xl.DisplayAlerts = False
#     xl.AskToUpdateLinks = False
#     print("Alerts disabled - no popups will appear")
    
#     print("Step 3: Opening workbook")
#     wb = xl.Workbooks.Open(r"C:\Users\mtschetter\Desktop\month_end\ftf_lots_data.xlsx")
    
#     print("Step 4: Accessing Query1")
#     query = wb.Queries.Item("Query1")
    
#     print("Step 5: Getting current query formula")
#     current_formula = query.Formula
#     print(f"Current formula: {current_formula}")
    
#     print("Step 6: Checking if CutOffMonth = 6 exists in formula")
#     print(f"Looking for 'CutOffMonth = 6' in formula")
    
#     print("Step 7: Modifying CutOffMonth from 6 to 7")
#     new_formula = current_formula.replace("CutOffMonth = 6", "CutOffMonth = 7")
#     print(f"New formula: {new_formula}")
    
#     print("Step 8: Checking if replacement was made")
#     replacement_made = current_formula != new_formula
#     print(f"Replacement made: {replacement_made}")
    
#     print("Step 9: Applying the new formula to the query")
#     query.Formula = new_formula
#     print("Formula applied to query")
    
#     print("Step 10: Refreshing all queries to get new data")
#     wb.RefreshAll()
#     print("Refresh initiated - waiting for completion")
    
#     print("Step 11: Saving the workbook")
#     wb.Save()
#     print("Workbook saved successfully")
    
#     print("Step 12: Re-enabling Excel alerts")
#     xl.DisplayAlerts = True
#     xl.AskToUpdateLinks = True
#     print("Alerts re-enabled")
    
#     print("Step 13: Closing workbook and Excel")
#     wb.Close()
#     xl.Quit()
    
#     print("Step 14: Query modification completed successfully!")

# modify_cutoff_month()
import os

def get_all_file_names(folder_path):
    print("Step 1: Starting file name extraction")
    print(f"Target folder: {folder_path}")
    
    print("\nStep 2: Getting list of all items in directory")
    all_items = os.listdir(folder_path)
    print(f"Total items found: {len(all_items)}")
    print(f"All items: {all_items}")
    
    print("\nStep 3: Filtering only files (excluding directories)")
    file_names = []
    for item in all_items:
        full_path = os.path.join(folder_path, item)
        print(f"Checking: {item}")
        
        is_file = os.path.isfile(full_path)
        print(f"  Is file: {is_file}")
        
        file_names.append(item)
    
    print(f"\nStep 4: Final list of file names")
    print(f"Total files found: {len(file_names)}")
    
    print("\nStep 5: Displaying all file names")
    for i, file_name in enumerate(file_names, 1):
        print(f"{i}: {file_name}")
    
    print(f"\nStep 6: Returning file names list")
    return file_names

# Usage
folder_path = r"C:\Users\mtschetter\Desktop\all_sop\Archive"
extracted_files = get_all_file_names(folder_path)
print(f"\nFinal result: {len(extracted_files)} files extracted")
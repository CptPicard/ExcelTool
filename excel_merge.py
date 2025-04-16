import pandas as pd
from pandas import ExcelWriter  # Add this import if not already present
from datetime import datetime

# --- Configuration ---
MASTER_EXCEL_PATH = "Jäsenrekisteri.xlsx"
UPDATE_EXCEL_PATH = "Uudet_tiedot.xlsx"
OUTPUT_EXCEL_PATH = "merged_output.xlsx"

# Sheet names (optional, set to None to use first sheet)
MASTER_SHEET = "JÄSENET 1"
UPDATE_SHEET = "Sheet1"

# Field mappings (update_excel -> master_excel)
FIELD_MAPPINGS = {
    'kadunnimi ja osoite': 'katuosoite',
    'sähköpostiosoite': 'sähköposti',
    'puhelinnumero': 'puhelinnumero'
}

# Name and timestamp fields
UPDATE_NAME_FIELD = 'etu- ja sukunimi'
UPDATE_TIMESTAMP_FIELD = 'completion time'
MASTER_FIRSTNAME_FIELD = 'etunimi'
MASTER_LASTNAME_FIELD = 'sukunimi'
MASTER_UPDATED_FIELD = 'päivitys pvm'

# --- Load data ---
master_df = pd.read_excel(MASTER_EXCEL_PATH, sheet_name=MASTER_SHEET)
update_df = pd.read_excel(UPDATE_EXCEL_PATH, sheet_name=UPDATE_SHEET)

# Normalize column names in both dataframes to avoid issues with spaces or case sensitivity
master_df.columns = master_df.columns.str.strip().str.lower()
update_df.columns = update_df.columns.str.strip().str.lower()

# Combine first and last name in master for matching
master_df['full_name_key'] = (master_df[MASTER_FIRSTNAME_FIELD].str.strip() + " " +
                              master_df[MASTER_LASTNAME_FIELD].str.strip()).str.lower()

# Prepare update dataframe
update_df['full_name_key'] = update_df[UPDATE_NAME_FIELD].str.strip().str.lower()
update_df[UPDATE_TIMESTAMP_FIELD] = pd.to_datetime(update_df[UPDATE_TIMESTAMP_FIELD], errors='coerce')

# Handle duplicates in the update file by keeping only the most recent entry
duplicate_names = update_df[update_df.duplicated(subset=['full_name_key'], keep=False)]
if not duplicate_names.empty:
    print("[INFO] Found duplicate names in update file:")
    rows_to_drop = []
    
    for name in duplicate_names['full_name_key'].unique():
        dupes = update_df[update_df['full_name_key'] == name].copy()
        print(f"  '{name}' appears {len(dupes)} times")
        
        # Sort by timestamp (most recent first) and keep only the first row
        dupes.sort_values(by=UPDATE_TIMESTAMP_FIELD, ascending=False, inplace=True)
        kept_time = dupes.iloc[0][UPDATE_TIMESTAMP_FIELD]
        print(f"    Keeping row from {kept_time}")
        
        # Mark all but the first row for removal
        rows_to_drop.extend(dupes.iloc[1:].index.tolist())
        for idx in dupes.iloc[1:].index:
            drop_time = update_df.loc[idx, UPDATE_TIMESTAMP_FIELD]
            print(f"    Discarding row from {drop_time}")
    
    # Remove the older duplicate rows
    if rows_to_drop:
        update_df = update_df.drop(rows_to_drop)
        update_df.reset_index(drop=True, inplace=True)
        print(f"[INFO] Removed {len(rows_to_drop)} older duplicate entries.")

# Merge update info into master
updated_rows = 0
unmatched_entries = []  # Track entries that couldn't be matched to master file
master_df['Updated'] = False  # Initialize the 'Updated' column

for i, update_row in update_df.iterrows():
    key = update_row['full_name_key']
    timestamp = update_row[UPDATE_TIMESTAMP_FIELD]

    # Find matching master row
    mask = master_df['full_name_key'] == key
    match_count = mask.sum()
    if match_count == 0:
        print(f"[WARN] Row {i}: No match for '{update_row[UPDATE_NAME_FIELD]}' (key: '{key}') in master Excel.")
        unmatched_entries.append({
            'row': i,
            'name': update_row[UPDATE_NAME_FIELD],
            'key': key,
            'timestamp': timestamp
        })
        continue
    elif match_count > 1:
        raise ValueError(f"[ERROR] Row {i}: Multiple matches for '{update_row[UPDATE_NAME_FIELD]}' (key: '{key}') in master Excel.")

    master_index = master_df.index[mask][0]

    # Update master data with update file values
    updated = False
    updated_fields = []  # Store all updated fields for this person
    
    for update_field, master_field in FIELD_MAPPINGS.items():
        if update_field in update_df.columns:
            # Check if the value is different before updating
            if pd.notna(update_row[update_field]) and master_df.loc[master_index, master_field] != update_row[update_field]:
                master_df.loc[master_index, master_field] = update_row[update_field]
                updated = True
                updated_fields.append(f"{master_field}: {update_row[update_field]}")
    
    # Mark row as updated if any changes were made and log all updates at once
    if updated:
        master_df.loc[master_index, 'Updated'] = True
        if pd.notna(timestamp):
            master_df.loc[master_index, MASTER_UPDATED_FIELD] = timestamp
        updated_rows += 1  # Increment only if the row was actually updated
        
        # Log all field updates for this person in a single line
        print(f"[INFO] Row {i}: Updated '{update_row[UPDATE_NAME_FIELD]}' ({key}) - {', '.join(updated_fields)}")

# Handle non-updated rows timestamps:
# 1. Rows that were updated already have their timestamp from the update file
# 2. For non-updated rows, keep existing values from päivitys pvm or päivitys pvm2
# Create a temporary mask for rows that weren't updated
non_updated_mask = ~master_df['Updated']

# For non-updated rows that have päivitys pvm2, use that value if päivitys pvm is empty
if 'päivitys pvm2' in master_df.columns:
    # Convert päivitys pvm2 to datetime first to avoid type incompatibility
    master_df['päivitys pvm2'] = pd.to_datetime(master_df['päivitys pvm2'], errors='coerce')
    
    # Only fill empty values in non-updated rows
    fill_mask = non_updated_mask & pd.isna(master_df[MASTER_UPDATED_FIELD])
    master_df.loc[fill_mask, MASTER_UPDATED_FIELD] = master_df.loc[fill_mask, 'päivitys pvm2']

# Ensure "Päivitys pvm" is saved as a proper datetime type
master_df[MASTER_UPDATED_FIELD] = pd.to_datetime(master_df[MASTER_UPDATED_FIELD], errors='coerce')

# --- Clean up data before saving ---
# Function to properly capitalize names (first letter uppercase, rest lowercase)
def proper_case(text):
    if pd.isna(text) or not isinstance(text, str):
        return text
    return text.strip().title()

# Function to normalize Finnish phone numbers (convert +358 to 0 and remove dashes)
def normalize_phone(phone):
    if pd.isna(phone) or not isinstance(phone, str):
        return phone
    phone = phone.strip()
    # Remove dashes
    phone = phone.replace('-', '')
    # Convert international format to local
    if phone.startswith('+358'):
        return '0' + phone[4:]
    return phone

# Apply cleanup to name columns
master_df[MASTER_FIRSTNAME_FIELD] = master_df[MASTER_FIRSTNAME_FIELD].apply(proper_case)
master_df[MASTER_LASTNAME_FIELD] = master_df[MASTER_LASTNAME_FIELD].apply(proper_case)

# Apply cleanup to phone number column if it exists
if 'puhelinnumero' in master_df.columns:
    master_df['puhelinnumero'] = master_df['puhelinnumero'].apply(normalize_phone)

# Drop unnecessary columns
columns_to_drop = ['päivitys pvm2'] if 'päivitys pvm2' in master_df.columns else []
columns_to_drop.append('Updated')  # Remove the temporary 'Updated' column
columns_to_drop.append('full_name_key')
master_df.drop(columns=columns_to_drop, inplace=True)

# --- Save result ---
with ExcelWriter(OUTPUT_EXCEL_PATH, datetime_format='YYYY-MM-DD') as writer:
    master_df.to_excel(writer, index=False)

print(f"Merge complete. {updated_rows} rows were updated.")

# Print summary of unmatched entries
if unmatched_entries:
    print("\n--- UNMATCHED ENTRIES SUMMARY ---")
    print(f"Found {len(unmatched_entries)} entries in the update file that couldn't be matched to the master file:")
    for entry in unmatched_entries:
        print(f"  Row {entry['row']}: '{entry['name']}' (Timestamp: {entry['timestamp']})")
    print("These entries require manual review and potential addition to the master file.")
else:
    print("All entries in the update file were successfully matched to the master file.")

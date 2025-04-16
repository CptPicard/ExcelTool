import pandas as pd
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
    'Kadunnimi ja osoite': 'Katuosoite',
    'Sähköpostiosoite': 'Sähköposti',
    'Puhelinnumero': 'Puhelinnumero'
}

# Name and timestamp fields
UPDATE_NAME_FIELD = 'Etu- ja sukunimi'
UPDATE_TIMESTAMP_FIELD = 'Completion time'
MASTER_FIRSTNAME_FIELD = 'Etunimi'
MASTER_LASTNAME_FIELD = 'Sukunimi'
MASTER_UPDATED_FIELD = 'Päivitys pvm'

# --- Load data ---
master_df = pd.read_excel(MASTER_EXCEL_PATH, sheet_name=MASTER_SHEET)
update_df = pd.read_excel(UPDATE_EXCEL_PATH, sheet_name=UPDATE_SHEET)

# Combine first and last name in master for matching
master_df['full_name_key'] = (master_df[MASTER_FIRSTNAME_FIELD].str.strip() + " " +
                              master_df[MASTER_LASTNAME_FIELD].str.strip()).str.lower()

# Prepare update dataframe
update_df['full_name_key'] = update_df[UPDATE_NAME_FIELD].str.strip().str.lower()
update_df[UPDATE_TIMESTAMP_FIELD] = pd.to_datetime(update_df[UPDATE_TIMESTAMP_FIELD], errors='coerce')

# Merge update info into master
current_time = datetime.now().strftime("%Y-%m-%d")
updated_rows = 0

for i, update_row in update_df.iterrows():
    key = update_row['full_name_key']
    timestamp = update_row[UPDATE_TIMESTAMP_FIELD]

    # Find matching master row
    mask = master_df['full_name_key'] == key
    if not mask.any():
        print(f"[WARN] Row {i}: No match for '{update_row[UPDATE_NAME_FIELD]}' (key: '{key}') in master Excel.")
        continue

    master_index = master_df.index[mask][0]
    master_timestamp = master_df.loc[master_index, MASTER_UPDATED_FIELD]

    # Convert to datetime or NaT
    if pd.notnull(master_timestamp):
        master_timestamp = pd.to_datetime(master_timestamp, errors='coerce')
    else:
        master_timestamp = pd.NaT

    # Update if update timestamp is newer
    if pd.isna(master_timestamp) or (pd.notna(timestamp) and timestamp > master_timestamp):
        for update_field, master_field in FIELD_MAPPINGS.items():
            if update_field in update_df.columns:
                master_df.loc[master_index, master_field] = update_row[update_field]
        master_df.loc[master_index, MASTER_UPDATED_FIELD] = current_time
        updated_rows += 1
    else:
        print(f"[INFO] Row {i}: Not updating '{update_row[UPDATE_NAME_FIELD]}' (key: '{key}') because update timestamp ({timestamp}) is not newer than master ({master_timestamp}).")

# Drop helper column
master_df.drop(columns=['full_name_key'], inplace=True)

# Ensure only date (YYYY-MM-DD) is written in the output for the update field
if master_df[MASTER_UPDATED_FIELD].dtype == 'datetime64[ns]':
    master_df[MASTER_UPDATED_FIELD] = master_df[MASTER_UPDATED_FIELD].dt.strftime('%Y-%m-%d')
else:
    master_df[MASTER_UPDATED_FIELD] = master_df[MASTER_UPDATED_FIELD].astype(str).str[:10]

# --- Save result ---
master_df.to_excel(OUTPUT_EXCEL_PATH, index=False)

print(f"Merge complete. {updated_rows} rows were updated.")

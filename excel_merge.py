import pandas as pd
from pandas import ExcelWriter
import argparse
import sys


# Constants for field names and column mappings
class FieldNames:
    # Master file fields
    MASTER_FIRSTNAME = 'etunimi'
    MASTER_LASTNAME = 'sukunimi'
    MASTER_UPDATED = 'päivitys pvm'
    MASTER_UPDATED_2 = 'päivitys pvm2'
    MASTER_ADDRESS = 'katuosoite'
    MASTER_EMAIL = 'sähköposti'
    MASTER_PHONE = 'puhelinnumero'
    
    # Update file fields
    UPDATE_FULLNAME = 'etu- ja sukunimi'
    UPDATE_TIMESTAMP = 'completion time'
    UPDATE_ADDRESS = 'kadunnimi ja osoite'
    UPDATE_EMAIL = 'sähköpostiosoite'
    UPDATE_PHONE = 'puhelinnumero'
    
    # Common fields
    FULL_NAME_KEY = 'full_name_key'
    UPDATED_FLAG = 'Updated'
    
    # Field mappings dictionary (update_excel -> master_excel)
    @classmethod
    def get_field_mappings(cls):
        return {
            cls.UPDATE_ADDRESS: cls.MASTER_ADDRESS,
            cls.UPDATE_EMAIL: cls.MASTER_EMAIL,
            cls.UPDATE_PHONE: cls.MASTER_PHONE
        }


class ExcelMerger:
    """Class for merging data from an update Excel file into a master Excel file."""

    def __init__(self, master_path, update_path, output_path,
                 master_sheet=None, update_sheet=None, include_extra_columns=False):
        """Initialize the ExcelMerger with file paths and settings."""
        # File paths
        self.master_path = master_path
        self.update_path = update_path
        self.output_path = output_path
        
        # Sheet names (None means use the first sheet)
        self.master_sheet = master_sheet
        self.update_sheet = update_sheet
        
        # Option to include extra columns from update file
        self.include_extra_columns = include_extra_columns
        
        # Field mappings (update_excel -> master_excel)
        self.field_mappings = FieldNames.get_field_mappings()
        
        # Name and timestamp fields
        self.update_name_field = FieldNames.UPDATE_FULLNAME
        self.update_timestamp_field = FieldNames.UPDATE_TIMESTAMP
        self.master_firstname_field = FieldNames.MASTER_FIRSTNAME
        self.master_lastname_field = FieldNames.MASTER_LASTNAME
        self.master_updated_field = FieldNames.MASTER_UPDATED
        
        # Initialize counters and storage
        self.updated_rows = 0
        self.unmatched_entries = []
        
        # For tracking added columns
        self.added_columns = []
        
        # Initialize to empty sets to avoid attribute errors
        self.common_columns = set()
        self.extra_columns = set()

    def load_data(self):
        """Load data from Excel files and normalize column names."""
        # Load Excel files, using first sheet if none specified
        try:
            # When sheet_name is None, read_excel returns a dict of all sheets
            if self.master_sheet is None:
                # Load all sheets and get the first one
                all_sheets = pd.read_excel(self.master_path, sheet_name=None)
                first_sheet_name = list(all_sheets.keys())[0]
                self.master_df = all_sheets[first_sheet_name]
                print(f"Loaded master file from: {self.master_path}")
                print(f"Using first sheet: '{first_sheet_name}'")
            else:
                # Load the specified sheet
                self.master_df = pd.read_excel(self.master_path, sheet_name=self.master_sheet)
                print(f"Loaded master file from: {self.master_path}")
                print(f"Using specified sheet: '{self.master_sheet}'")
        except Exception as e:
            raise ValueError(f"Error loading master file: {e}")
            
        try:
            # When sheet_name is None, read_excel returns a dict of all sheets
            if self.update_sheet is None:
                # Load all sheets and get the first one
                all_sheets = pd.read_excel(self.update_path, sheet_name=None)
                first_sheet_name = list(all_sheets.keys())[0]
                self.update_df = all_sheets[first_sheet_name]
                print(f"Loaded update file from: {self.update_path}")
                print(f"Using first sheet: '{first_sheet_name}'")
            else:
                # Load the specified sheet
                self.update_df = pd.read_excel(self.update_path, sheet_name=self.update_sheet)
                print(f"Loaded update file from: {self.update_path}")
                print(f"Using specified sheet: '{self.update_sheet}'")
        except Exception as e:
            raise ValueError(f"Error loading update file: {e}")
        
        # Normalize column names to avoid issues with spaces or case sensitivity
        self.master_df.columns = self.master_df.columns.str.strip().str.lower()
        self.update_df.columns = self.update_df.columns.str.strip().str.lower()
        
        # Combine first and last name in master for matching
        self.master_df[FieldNames.FULL_NAME_KEY] = (
            self.master_df[self.master_firstname_field].str.strip() + " " +
            self.master_df[self.master_lastname_field].str.strip()
        ).str.lower()
        
        # Prepare update dataframe
        self.update_df[FieldNames.FULL_NAME_KEY] = self.update_df[self.update_name_field].str.strip().str.lower()
        self.update_df[self.update_timestamp_field] = pd.to_datetime(
            self.update_df[self.update_timestamp_field], errors='coerce'
        )
        
        # Find matching column names for automatic mapping
        # Always calculate common columns regardless of include_extra_columns setting
        self.common_columns = set(self.master_df.columns).intersection(set(self.update_df.columns))
        
        # Only set up extra columns if the flag is enabled
        if self.include_extra_columns:
            self.extra_columns = set(self.update_df.columns) - set(self.master_df.columns) - {
                self.update_name_field, self.update_timestamp_field, FieldNames.FULL_NAME_KEY
            }
            
            if self.extra_columns:
                print(f"[INFO] Found {len(self.extra_columns)} extra columns in update file that will be added:")
                for col in self.extra_columns:
                    print(f"  - {col}")
                    # Add these columns to the master dataframe
                    self.master_df[col] = None
                    self.added_columns.append(col)

    def handle_duplicates(self):
        """Handle duplicate names in the update file by keeping the most recent entry."""
        duplicate_names = self.update_df[self.update_df.duplicated(subset=[FieldNames.FULL_NAME_KEY], keep=False)]
        if not duplicate_names.empty:
            print("[INFO] Found duplicate names in update file:")
            rows_to_drop = []
            
            for name in duplicate_names[FieldNames.FULL_NAME_KEY].unique():
                dupes = self.update_df[self.update_df[FieldNames.FULL_NAME_KEY] == name].copy()
                print(f"  '{name}' appears {len(dupes)} times")
                
                # Sort by timestamp (most recent first) and keep only the first row
                dupes.sort_values(by=self.update_timestamp_field, ascending=False, inplace=True)
                kept_time = dupes.iloc[0][self.update_timestamp_field]
                print(f"    Keeping row from {kept_time}")
                
                # Mark all but the first row for removal
                rows_to_drop.extend(dupes.iloc[1:].index.tolist())
                for idx in dupes.iloc[1:].index:
                    drop_time = self.update_df.loc[idx, self.update_timestamp_field]
                    print(f"    Discarding row from {drop_time}")
            
            # Remove the older duplicate rows
            if rows_to_drop:
                self.update_df = self.update_df.drop(rows_to_drop)
                self.update_df.reset_index(drop=True, inplace=True)
                print(f"[INFO] Removed {len(rows_to_drop)} older duplicate entries.")

    def process_updates(self):
        """Process updates from the update file to the master file."""
        self.master_df[FieldNames.UPDATED_FLAG] = False  # Initialize the 'Updated' column
        
        for i, update_row in self.update_df.iterrows():
            key = update_row[FieldNames.FULL_NAME_KEY]
            timestamp = update_row[self.update_timestamp_field]
            
            # Find matching master row
            mask = self.master_df[FieldNames.FULL_NAME_KEY] == key
            match_count = mask.sum()
            
            if match_count == 0:
                print(f"[WARN] Row {i}: No match for '{update_row[self.update_name_field]}' (key: '{key}') in master Excel.")
                self.unmatched_entries.append({
                    'row': i,
                    'name': update_row[self.update_name_field],
                    'key': key,
                    'timestamp': timestamp
                })
                continue
            elif match_count > 1:
                raise ValueError(f"[ERROR] Row {i}: Multiple matches for '{update_row[self.update_name_field]}' (key: '{key}') in master Excel.")
            
            master_index = self.master_df.index[mask][0]
            
            # Update master data with update file values
            updated = False
            updated_fields = []  # Store all updated fields for this person
            
            # Process field mappings
            for update_field, master_field in self.field_mappings.items():
                if update_field in self.update_df.columns:
                    # Check if the value is different before updating
                    if pd.notna(update_row[update_field]) and self.master_df.loc[master_index, master_field] != update_row[update_field]:
                        self.master_df.loc[master_index, master_field] = update_row[update_field]
                        updated = True
                        updated_fields.append(f"{master_field}: {update_row[update_field]}")
            
            # Process columns with identical names in both files
            for col in self.common_columns:
                # Skip fields that are already handled by explicit mappings or special fields
                if col in [self.update_name_field, self.update_timestamp_field, FieldNames.FULL_NAME_KEY]:
                    continue
                    
                # Skip fields that have an explicit mapping
                if col in self.field_mappings or col in self.field_mappings.values():
                    continue
                    
                # Check if the value is different before updating
                if pd.notna(update_row[col]) and self.master_df.loc[master_index, col] != update_row[col]:
                    self.master_df.loc[master_index, col] = update_row[col]
                    updated = True
                    updated_fields.append(f"{col}: {update_row[col]}")
            
            # Process extra columns if option is enabled
            if self.include_extra_columns:
                for col in self.extra_columns:
                    if pd.notna(update_row[col]):
                        self.master_df.loc[master_index, col] = update_row[col]
                        # Only mark as updated if this is the first time we're filling in this value
                        if pd.isna(self.master_df.loc[master_index, col]) and not updated_fields:
                            updated = True
                            updated_fields.append(f"{col}: {update_row[col]}")
            
            # Mark row as updated if any changes were made and log all updates at once
            if updated:
                self.master_df.loc[master_index, FieldNames.UPDATED_FLAG] = True
                if pd.notna(timestamp):
                    self.master_df.loc[master_index, self.master_updated_field] = timestamp
                self.updated_rows += 1  # Increment only if the row was actually updated
                
                # Log all field updates for this person in a single line
                print(f"[INFO] Row {i}: Updated '{update_row[self.update_name_field]}' ({key}) - {', '.join(updated_fields)}")

    def handle_timestamps(self):
        """Handle timestamps for non-updated rows."""
        # Create a temporary mask for rows that weren't updated
        non_updated_mask = ~self.master_df[FieldNames.UPDATED_FLAG]
        
        # For non-updated rows that have päivitys pvm2, use that value if päivitys pvm is empty
        if FieldNames.MASTER_UPDATED_2 in self.master_df.columns:
            # Convert päivitys pvm2 to datetime first to avoid type incompatibility
            self.master_df[FieldNames.MASTER_UPDATED_2] = pd.to_datetime(self.master_df[FieldNames.MASTER_UPDATED_2], errors='coerce')
            
            # Only fill empty values in non-updated rows
            fill_mask = non_updated_mask & pd.isna(self.master_df[self.master_updated_field])
            self.master_df.loc[fill_mask, self.master_updated_field] = self.master_df.loc[fill_mask, FieldNames.MASTER_UPDATED_2]
        
        # Ensure "Päivitys pvm" is saved as a proper datetime type
        self.master_df[self.master_updated_field] = pd.to_datetime(self.master_df[self.master_updated_field], errors='coerce')

    def clean_data(self):
        """Clean and standardize data before saving."""
        # Apply cleanup to name columns
        self.master_df[self.master_firstname_field] = self.master_df[self.master_firstname_field].apply(self.proper_case)
        self.master_df[self.master_lastname_field] = self.master_df[self.master_lastname_field].apply(self.proper_case)
        
        # Apply cleanup to phone number column if it exists
        if FieldNames.MASTER_PHONE in self.master_df.columns:
            self.master_df[FieldNames.MASTER_PHONE] = self.master_df[FieldNames.MASTER_PHONE].apply(self.normalize_phone)
        
        # Drop unnecessary columns
        columns_to_drop = [FieldNames.MASTER_UPDATED_2] if FieldNames.MASTER_UPDATED_2 in self.master_df.columns else []
        columns_to_drop.append(FieldNames.UPDATED_FLAG)  # Remove the temporary 'Updated' column
        columns_to_drop.append(FieldNames.FULL_NAME_KEY)
        self.master_df.drop(columns=columns_to_drop, inplace=True)
        
        # Standardize column names for better compatibility when reusing as master file
        self.master_df.columns = [col.strip().title() for col in self.master_df.columns]

    def save_result(self):
        """Save the result to an Excel file."""
        with ExcelWriter(self.output_path, datetime_format='YYYY-MM-DD') as writer:
            self.master_df.to_excel(writer, index=False)
        
        print(f"Merge complete. {self.updated_rows} rows were updated.")
        
        # Print information about added columns
        if self.include_extra_columns and self.added_columns:
            print(f"\nAdded {len(self.added_columns)} new columns from update file:")
            for col in self.added_columns:
                print(f"  - {col}")
        
        # Print summary of unmatched entries
        if self.unmatched_entries:
            print("\n--- UNMATCHED ENTRIES SUMMARY ---")
            print(f"Found {len(self.unmatched_entries)} entries in the update file that couldn't be matched to the master file:")
            for entry in self.unmatched_entries:
                print(f"  Row {entry['row']}: '{entry['name']}' (Timestamp: {entry['timestamp']})")
            print("These entries require manual review and potential addition to the master file.")
        else:
            print("All entries in the update file were successfully matched to the master file.")

    def run(self):
        """Run the full merge process."""
        self.load_data()
        self.handle_duplicates()
        self.process_updates()
        self.handle_timestamps()
        self.clean_data()
        self.save_result()
        
    @staticmethod
    def proper_case(text):
        """Convert text to proper case (first letter uppercase, rest lowercase)."""
        if pd.isna(text) or not isinstance(text, str):
            return text
        return text.strip().title()
    
    @staticmethod
    def normalize_phone(phone):
        """Normalize Finnish phone numbers (convert +358 to 0 and remove dashes)."""
        if pd.isna(phone) or not isinstance(phone, str):
            return phone
        phone = phone.strip()
        # Remove dashes
        phone = phone.replace('-', '')
        # Convert international format to local
        if phone.startswith('+358'):
            return '0' + phone[4:]
        return phone


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Merge updates from a new Excel file into a master Excel file.')
    
    parser.add_argument('--master', '-m', required=True,
                        help='Path to the master Excel file')
    parser.add_argument('--update', '-u', required=True,
                        help='Path to the update Excel file with new data')
    parser.add_argument('--output', '-o', required=True,
                        help='Path to save the merged output Excel file')
    parser.add_argument('--master-sheet',
                        help='Sheet name in the master Excel file (default: use first sheet)')
    parser.add_argument('--update-sheet',
                        help='Sheet name in the update Excel file (default: use first sheet)')
    parser.add_argument('--include-extra-columns', '-e', action='store_true',
                        help='Include all columns from update file not present in master file')
    
    return parser.parse_args()


def main():
    """Main function to run the script from command line."""
    try:
        args = parse_args()
        
        merger = ExcelMerger(
            master_path=args.master,
            update_path=args.update,
            output_path=args.output,
            master_sheet=args.master_sheet,
            update_sheet=args.update_sheet,
            include_extra_columns=args.include_extra_columns
        )
        
        merger.run()
        return 0
    
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
else:
    # When imported as a module, this enables poetry's script to work
    # The entry point in pyproject.toml calls excel_merge:main
    pass

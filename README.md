# Excel Merger

A tool for merging data from an update Excel file into a master Excel file.

## Installation

```bash
# Install with Poetry
poetry install
```

## Usage

```bash
# Run with Poetry
poetry run excel-merger --master Jäsenrekisteri.xlsx --update Uudet_tiedot.xlsx --output merged_output.xlsx

# Or activate the virtual environment and run directly
poetry shell
excel-merger --master Input.xlsx --update New_Data.xlsx --output Merged_Output.xlsx
```

For more options:
```bash
poetry run excel-merger --help
```

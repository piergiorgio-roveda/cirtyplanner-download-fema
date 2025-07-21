# Script 06c: Create Clean Conversion Table

## Overview

This script creates clean tables from the conversion_06b_log table for easier analysis and merging operations:
1. Creates a `clean_conversion_table` with product_name, gpkg_path, and filename
2. Creates a `gpkg_filename_groups` table with filename and count
3. Generates statistics about filename groups

## Implementation Details

### Database Schema Extensions

The script creates two new tables in the SQLite database:

#### clean_conversion_table

```sql
CREATE TABLE IF NOT EXISTS clean_conversion_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    gpkg_path TEXT NOT NULL,
    filename TEXT NOT NULL
)
```

Indexes:
- `idx_clean_product` on `product_name`
- `idx_clean_filename` on `filename`

#### gpkg_filename_groups

```sql
CREATE TABLE IF NOT EXISTS gpkg_filename_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    count INTEGER NOT NULL
)
```

Indexes:
- `idx_gpkg_filename_groups` on `filename`

### Process Flow

1. **Setup and Initialization**
   - Load configuration from config.json
   - Connect to SQLite database
   - Create tables and indexes if they don't exist

2. **Data Extraction**
   - Query `conversion_06b_log` table for successful conversions
   - Extract filename from gpkg_path for each record
   - Insert data into `clean_conversion_table`

3. **Filename Grouping**
   - Group records by filename
   - Count occurrences of each filename
   - Insert data into `gpkg_filename_groups` table

4. **Reporting**
   - Generate statistics about product distribution
   - Generate statistics about filename groups
   - Output detailed report to console and log file

### Key Functions

- `extract_filename(gpkg_path)`: Extracts filename without path and extension
- `populate_clean_table(conn, logger)`: Populates both tables from conversion_06b_log
- `generate_report(conn, inserted_count, groups_count, logger)`: Generates statistics report

## Configuration Options

The script uses the following configuration options from config.json:

```json
{
  "database": {
    "path": "meta_results/flood_risk_shapefiles.db"
  }
}
```

### Command Line Options

- `--config PATH`: Use custom configuration file (default: config.json)
- `--force-rebuild`: Clear and recreate tables
- `--verbose`: Enable verbose logging

## Usage Examples

### Basic Usage

```bash
python notebooks/06c_create_clean_conversion_table.py
```

### Force Rebuild

```bash
python notebooks/06c_create_clean_conversion_table.py --force-rebuild
```

### Verbose Logging

```bash
python notebooks/06c_create_clean_conversion_table.py --verbose
```

## Performance Considerations

- The script is generally fast as it only performs database operations
- For large datasets (>100,000 records), the grouping operation may take a few seconds
- Memory usage is minimal as data is processed through database queries

## Error Handling

- Database connection errors are caught and reported
- JSON parsing errors for config file are handled with clear messages
- Missing configuration sections trigger validation errors
- All errors are logged to both console and log file

## Dependencies

- Standard Python libraries only (sqlite3, os, json, argparse, logging)
- No external dependencies required

## Output Example

```
=== Creating Clean Conversion Table (Script 06c) ===
Start time: 2025-07-21 10:30:00.000000

=== Populating Clean Conversion Table ===
Found 5432 successful conversions to process
Progress: 5432/5432 rows processed
Successfully inserted 5432 rows into clean_conversion_table
Populating gpkg_filename_groups table...
Successfully created 42 filename groups

================================================================================
CLEAN CONVERSION TABLE REPORT
================================================================================

[PRODUCT STATISTICS (Top 10)]:
  FRD_01001C_Shapefiles: 12 files
  FRD_01003C_Shapefiles: 10 files
  FRD_01005C_Shapefiles: 9 files
  FRD_01007C_Shapefiles: 8 files
  FRD_01009C_Shapefiles: 8 files
  FRD_01011C_Shapefiles: 7 files
  FRD_01013C_Shapefiles: 7 files
  FRD_01015C_Shapefiles: 7 files
  FRD_01017C_Shapefiles: 6 files
  FRD_01019C_Shapefiles: 6 files

[FILENAME GROUPS (Top 10)]:
  S_FRD_Proj_Ar: 1254 occurrences
  S_HUC_Ar: 1254 occurrences
  S_CSLF_Ar: 1123 occurrences
  S_FEMA_Jurisdiction_Ar: 987 occurrences
  S_Raster_Index_Ar: 812 occurrences
  S_Depth_Ar: 789 occurrences
  S_Velocity_Ar: 654 occurrences
  S_Percent_Annual_Chance_Ar: 543 occurrences
  S_Percent_30yr_Ar: 432 occurrences
  S_Hazus_Ar: 321 occurrences

================================================================================
CLEAN CONVERSION TABLE CREATED - 5432 rows total
GPKG FILENAME GROUPS CREATED - 42 unique filenames
================================================================================

Process completed at: 2025-07-21 10:30:05.000000
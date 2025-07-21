# FEMA Flood Risk Data Collector - Database Schema

This document provides a comprehensive overview of the database schema used in the FEMA Flood Risk Data Collector project. The database tracks the collection, processing, and management of FEMA flood risk data across various stages of the workflow.

## Core Data Tables

### States Table
```sql
CREATE TABLE states (
    state_code TEXT PRIMARY KEY,
    state_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
- Stores basic information about states
- Primary key is the FEMA state code (e.g., "01" for Alabama)

### Counties Table
```sql
CREATE TABLE counties (
    county_code TEXT PRIMARY KEY,
    county_name TEXT NOT NULL,
    state_code TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (state_code) REFERENCES states (state_code)
);
```
- Stores information about counties
- Primary key is the FEMA county code
- Foreign key relationship with states table

### Communities Table
```sql
CREATE TABLE communities (
    community_code TEXT PRIMARY KEY,
    community_name TEXT NOT NULL,
    county_code TEXT NOT NULL,
    state_code TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (county_code) REFERENCES counties (county_code),
    FOREIGN KEY (state_code) REFERENCES states (state_code)
);
```
- Stores information about communities
- Primary key is the FEMA community code
- Foreign key relationships with both counties and states tables

### Shapefiles Table
```sql
CREATE TABLE shapefiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    community_code TEXT NOT NULL,
    county_code TEXT NOT NULL,
    state_code TEXT NOT NULL,
    product_id INTEGER,
    product_type_id TEXT,
    product_subtype_id TEXT,
    product_name TEXT,
    product_description TEXT,
    product_effective_date INTEGER,
    product_issue_date INTEGER,
    product_effective_date_string TEXT,
    product_posting_date INTEGER,
    product_posting_date_string TEXT,
    product_issue_date_string TEXT,
    product_effective_flag TEXT,
    product_file_path TEXT,
    product_file_size TEXT,
    fetch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (community_code) REFERENCES communities (community_code),
    FOREIGN KEY (county_code) REFERENCES counties (county_code),
    FOREIGN KEY (state_code) REFERENCES states (state_code)
);
```
- Central table storing information about available flood risk shapefiles
- Contains detailed metadata about each shapefile product
- Foreign key relationships with communities, counties, and states tables
- Indices on state_code, county_code, community_code, and product_name for efficient querying

## Processing Workflow Tables

The following tables track the various stages of the data processing workflow:

### Request Log
```sql
CREATE TABLE request_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    community_code TEXT NOT NULL,
    county_code TEXT NOT NULL,
    state_code TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    shapefiles_found INTEGER DEFAULT 0,
    request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (community_code) REFERENCES communities (community_code)
);
```
- Tracks API requests for flood risk shapefiles
- Records success/failure and number of shapefiles found

### Download Log
```sql
CREATE TABLE download_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_code TEXT NOT NULL,
    county_code TEXT NOT NULL,
    community_code TEXT NOT NULL,
    product_name TEXT NOT NULL,
    product_file_path TEXT NOT NULL,
    download_success BOOLEAN NOT NULL,
    download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_path TEXT,
    file_size_bytes INTEGER,
    error_message TEXT
);
```
- Tracks the downloading of shapefile zip files
- Records file paths, sizes, and success/failure

### Extraction Log (06a)
```sql
CREATE TABLE extraction_06a_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    zip_file_path TEXT NOT NULL,
    extracted_path TEXT,
    shapefile_name TEXT,
    extraction_success BOOLEAN NOT NULL,
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);
```
- Tracks the extraction of shapefiles from zip files
- Records extracted paths and success/failure

### Conversion Log (06b)
```sql
CREATE TABLE conversion_06b_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    shapefile_path TEXT NOT NULL,
    gpkg_path TEXT NOT NULL,
    conversion_success BOOLEAN NOT NULL,
    conversion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);
```
- Tracks the conversion of shapefiles to GeoPackage format
- Records source and destination paths and success/failure

### Clean Conversion Table
```sql
CREATE TABLE clean_conversion_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    gpkg_path TEXT NOT NULL,
    filename TEXT NOT NULL
);
```
- Provides a clean view of successfully converted GeoPackage files
- Used for organizing files for merging

### GeoPackage Filename Groups
```sql
CREATE TABLE gpkg_filename_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    count INTEGER NOT NULL
);
```
- Tracks groups of similar GeoPackage files for merging
- Records the count of each filename type

### Merge Log (06d)
```sql
CREATE TABLE merge_06d_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename_group TEXT NOT NULL,
    source_files_count INTEGER NOT NULL,
    merged_gpkg_path TEXT NOT NULL,
    merge_success BOOLEAN NOT NULL,
    merge_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);
```
- Tracks the merging of similar GeoPackage files
- Records source file counts, destination paths, and success/failure

### Shapefile Processing Log
```sql
CREATE TABLE shapefile_processing_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_code TEXT NOT NULL,
    shapefile_type TEXT NOT NULL,
    geometry_type TEXT,
    source_files_count INTEGER DEFAULT 0,
    total_features_merged INTEGER DEFAULT 0,
    output_gpkg_path TEXT,
    processing_success BOOLEAN NOT NULL,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_size_bytes INTEGER,
    coordinate_system TEXT,
    error_message TEXT,
    FOREIGN KEY (state_code) REFERENCES states (state_code)
);
```
- Tracks the overall processing of shapefiles
- Records detailed information about the processed data, including geometry type, feature counts, and coordinate systems

### Shapefile Contributions
```sql
CREATE TABLE shapefile_contributions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_code TEXT NOT NULL,
    county_code TEXT NOT NULL,
    community_code TEXT NOT NULL,
    product_name TEXT NOT NULL,
    shapefile_type TEXT NOT NULL,
    source_shapefile_path TEXT NOT NULL,
    features_count INTEGER DEFAULT 0,
    merged_into_gpkg TEXT,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (state_code) REFERENCES states (state_code),
    FOREIGN KEY (county_code) REFERENCES counties (county_code),
    FOREIGN KEY (community_code) REFERENCES communities (community_code)
);
```
- Tracks individual shapefile contributions to merged GeoPackage files
- Records feature counts and source paths

## Data Processing Workflow

The database schema supports the following workflow:

1. **Data Collection**:
   - States, counties, and communities data is collected and stored in their respective tables
   - Flood risk shapefiles are identified and recorded in the shapefiles table
   - API requests are logged in the request_log table

2. **Download Process**:
   - Shapefiles are downloaded as zip files
   - Download operations are logged in the download_log table

3. **Extraction Process (06a)**:
   - Zip files are extracted to access the shapefiles
   - Extraction operations are logged in the extraction_06a_log table

4. **Conversion Process (06b)**:
   - Shapefiles are converted to GeoPackage format
   - Conversion operations are logged in the conversion_06b_log table
   - Successfully converted files are recorded in the clean_conversion_table

5. **Merging Process (06d)**:
   - Similar GeoPackage files are identified and grouped in the gpkg_filename_groups table
   - Files are merged into consolidated GeoPackage files
   - Merging operations are logged in the merge_06d_log table

6. **Processing and Analysis**:
   - Overall processing statistics are recorded in the shapefile_processing_log table
   - Individual shapefile contributions are tracked in the shapefile_contributions table

This workflow enables the systematic collection, processing, and management of FEMA flood risk data, with comprehensive logging at each stage to ensure traceability and facilitate troubleshooting.
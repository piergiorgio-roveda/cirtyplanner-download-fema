# FEMA Flood Risk Shapefile Data Collector

A comprehensive Python toolkit for collecting and analyzing FEMA flood risk shapefile data across all US states, counties, and communities.

## Overview

This project provides automated tools to:
- Extract state, county, and community data from FEMA's portal
- Collect flood risk shapefile information for all jurisdictions
- Download all available shapefile ZIP files
- Extract and merge shapefiles into consolidated GPKG files by state and type
- Store data in a structured SQLite database for analysis
- Generate comprehensive reports and statistics

## 🚧 Project Status

**Current Progress:**
- ✅ **Scripts 01-03**: Fully tested and operational
- 🔄 **Script 04**: Currently running data collection
  - Status: Processing county 444/3176 (Habersham County, Georgia)
  - Progress: ~14% complete
- ⚠️ **Script 05**: Created but not yet tested
  - Download functionality implemented
  - Awaiting completion of script 04 for testing
- 🚧 **Script 06**: Implementation complete
  - Extract and merge shapefiles functionality
  - Creates consolidated GPKG files by state and shapefile type
  - Comprehensive validation and error handling

**Data Collection Status:**
The project is actively collecting flood risk shapefile metadata from FEMA's portal. Script 04 is currently processing Georgia counties and has successfully collected data from hundreds of counties across multiple states.

## Project Structure

```
├── notebooks/                    # Python scripts for data collection
│   ├── 01_get_all_state.py      # Extract all US states/territories
│   ├── 02_get_all_counties.py   # Extract counties for each state
│   ├── 03_get_all_communities.py # Extract communities for each county
│   ├── 04_get_flood_risk_shapefiles.py # Collect shapefile data
│   ├── 05_download_shapefiles.py # Download all shapefile ZIP files
│   ├── 06_extract_and_merge_shapefiles.py # Legacy: Extract ZIPs and merge to GPKG
│   ├── 06a_extract_zip_files.py # Extract ZIP files only
│   ├── 06b_convert_shapefiles_to_gpkg.py # Convert shapefiles to GPKG
│   ├── 06c_create_clean_conversion_table.py # Create clean tables for analysis
│   └── 06d_merge_gpkg_files.py # Merge GPKG files by filename group
├── meta/                         # Reference HTML/JSON files
│   ├── state.html               # FEMA state dropdown HTML
│   ├── advanceSearch-getCounty.json
│   ├── advanceSearch-getCommunity.json
│   └── portal-advanceSearch.json
├── meta_results/                 # Generated data files
│   ├── states_data.json
│   ├── all_counties_data.json
│   ├── all_communities_data.json
│   └── flood_risk_shapefiles.db # SQLite database
├── meta_results_sample/          # Sample data for testing
├── E:\FEMA_MERGED\              # Consolidated GPKG files by filename (from script 06d)
│   ├── S_FRD_Proj_Ar.gpkg       # Merged project area files
│   ├── S_HUC_Ar.gpkg            # Merged HUC area files
│   └── ...                      # All merged files by filename
├── config.json                   # Configuration file for processing settings
├── config.sample.json            # Sample configuration template
├── LICENSE                       # MIT License with FEMA data notice
├── IMPLEMENTATION/               # Detailed implementation documentation
│   ├── README.md                # Implementation overview
│   ├── 2025-01-20_01_get_all_state.md
│   ├── 2025-01-20_02_get_all_counties.md
│   ├── 2025-01-20_03_get_all_communities.md
│   ├── 2025-01-20_04_get_flood_risk_shapefiles.md
│   ├── 2025-01-20_05_download_shapefiles.md
│   └── 2025-01-20_06_extract_and_merge_shapefiles.md
└── .roo/                         # Roo development rules and standards
    ├── rules/                    # General project standards
    └── rules-code/               # Python coding standards
```

## Quick Start

### Prerequisites

**Network Requirements:**
- **VPN connection to USA** may be required for accessing FEMA portal
- Some regions may experience access restrictions to FEMA's data portal
- Ensure stable internet connection for large data downloads

**Python Dependencies:**
```bash
pip install requests sqlite3 geopandas fiona shapely pyproj psutil
```

### Usage

1. **Extract States** (57 states/territories):
   ```bash
   python notebooks/01_get_all_state.py
   ```

2. **Extract Counties** (3,176 counties):
   ```bash
   python notebooks/02_get_all_counties.py
   ```

3. **Extract Communities** (30,704 communities):
   ```bash
   python notebooks/03_get_all_communities.py
   ```

4. **Collect Shapefile Data**:
   ```bash
   python notebooks/04_get_flood_risk_shapefiles.py
   ```
   **Automatic resume capability** - safely restarts after network interruptions

5. **Download All Shapefiles**:
   ```bash
   python notebooks/05_download_shapefiles.py
   ```
   Downloads to: `E:\FEMA_DOWNLOAD\{state}\{county}\`
   
   **Testing with limited downloads:**
   ```bash
   python notebooks/05_download_shapefiles.py --limit 10
   ```
   Downloads only the first 10 files not yet downloaded

6. **Extract and Process Shapefiles**:
   
   **Option 1 (Recommended): Four-step process**
   ```bash
   # Step 1: Extract ZIP files only
   python notebooks/06a_extract_zip_files.py
   
   # Step 2: Convert shapefiles to GPKG (must be run from OSGeo4W console)
   # Open OSGeo4W console first, then navigate to project directory
   cd /d d:\git\cityplanner-desktop\download-fema
   python notebooks/06b_convert_shapefiles_to_gpkg.py
   
   # Step 3: Create clean conversion tables for analysis
   python notebooks/06c_create_clean_conversion_table.py
   
   # Step 4: Merge GPKG files by filename group (must be run from OSGeo4W console)
   cd /d d:\git\cityplanner-desktop\download-fema
   python notebooks/06d_merge_gpkg_files.py
   ```
   
   **Important**: Scripts 06b and 06d require ogr2ogr which is available through OSGeo4W.
   You must run these scripts from an OSGeo4W console or environment where ogr2ogr is in the PATH.
   
   **Option 2 (Legacy): Combined extraction and merging**
   ```bash
   python notebooks/06_extract_and_merge_shapefiles.py
   ```
   
   Creates merged GPKG files: `E:\FEMA_SHAPEFILE_TO_GPKG\{product_name}\{shapefile_name}.gpkg`
   
   **Force rebuild (if new data found by scripts 04/05):**
   ```bash
   python notebooks/06a_extract_zip_files.py --force-rebuild
   python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild
   python notebooks/06c_create_clean_conversion_table.py --force-rebuild
   python notebooks/06d_merge_gpkg_files.py --force-rebuild
   ```
   Clears all processing logs and rebuilds from scratch

## Command Line Options

### Script 05: Download Shapefiles

```bash
# Basic usage
python notebooks/05_download_shapefiles.py

# Available options:
python notebooks/05_download_shapefiles.py [OPTIONS]

Options:
  --limit N              Download only first N files not yet downloaded (for testing)
  --config PATH          Use custom configuration file (default: config.json)
  -h, --help            Show help message and exit
```

**Examples:**
```bash
# Test with first 10 files
python notebooks/05_download_shapefiles.py --limit 10

# Use custom configuration
python notebooks/05_download_shapefiles.py --config my_config.json

# Download all remaining files
python notebooks/05_download_shapefiles.py
```

### Script 06a: Extract ZIP Files

```bash
# Basic usage
python notebooks/06a_extract_zip_files.py

# Available options:
python notebooks/06a_extract_zip_files.py [OPTIONS]

Options:
  --config PATH          Use custom configuration file (default: config.json)
  --states CODES         Process specific states only (comma-separated, e.g., 01,02,04)
  --resume              Resume interrupted processing (default behavior)
  --force-rebuild       Clear all extraction logs and rebuild from scratch
  --dry-run             Show what would be processed without doing it
  --verbose             Enable verbose logging
  --no-cleanup          Skip cleanup of temporary files
  -h, --help            Show help message and exit
```

**Examples:**
```bash
# Normal processing (resumes automatically)
python notebooks/06a_extract_zip_files.py

# Process specific states only
python notebooks/06a_extract_zip_files.py --states 01,02,04

# Force complete rebuild (when new data found)
python notebooks/06a_extract_zip_files.py --force-rebuild
```

### Script 06b: Convert Shapefiles to GeoPackage

```bash
# Basic usage
python notebooks/06b_convert_shapefiles_to_gpkg.py

# Available options:
python notebooks/06b_convert_shapefiles_to_gpkg.py [OPTIONS]

Options:
  --config PATH          Use custom configuration file (default: config.json)
  --products NAMES       Process specific products only (comma-separated)
  --resume              Resume interrupted processing (default behavior)
  --force-rebuild       Clear all conversion logs and rebuild from scratch
  --dry-run             Show what would be processed without doing it
  --verbose             Enable verbose logging
  --use-geopandas       Use GeoPandas instead of ogr2ogr for conversion
  --encoding ENCODING    Encoding to use for shapefile attribute data (default: UTF-8)
  --max-workers N        Maximum number of worker threads (default: 4)
  -h, --help            Show help message and exit
```

**Examples:**
```bash
# Normal processing (resumes automatically)
python notebooks/06b_convert_shapefiles_to_gpkg.py

# Process specific products only
python notebooks/06b_convert_shapefiles_to_gpkg.py --products FRD_01001C_Shapefiles,FRD_01003C_Shapefiles

# Force complete rebuild (when new data found)
python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild

# Use GeoPandas instead of ogr2ogr (default)
python notebooks/06b_convert_shapefiles_to_gpkg.py --use-geopandas

# Specify encoding for handling special characters
python notebooks/06b_convert_shapefiles_to_gpkg.py --encoding latin-1
```

### Script 06c: Create Clean Conversion Table

```bash
# Basic usage
python notebooks/06c_create_clean_conversion_table.py

# Available options:
python notebooks/06c_create_clean_conversion_table.py [OPTIONS]

Options:
  --config PATH          Use custom configuration file (default: config.json)
  --force-rebuild       Clear and recreate tables
  --verbose             Enable verbose logging
  -h, --help            Show help message and exit
```

**Examples:**
```bash
# Normal processing
python notebooks/06c_create_clean_conversion_table.py

# Force rebuild tables
python notebooks/06c_create_clean_conversion_table.py --force-rebuild

# Verbose logging
python notebooks/06c_create_clean_conversion_table.py --verbose
```

### Script 06d: Merge GeoPackage Files by Filename Group

```bash
# Basic usage (must be run from OSGeo4W console)
python notebooks/06d_merge_gpkg_files.py

# Available options:
python notebooks/06d_merge_gpkg_files.py [OPTIONS]

Options:
  --config PATH          Use custom configuration file (default: config.json)
  --filenames NAMES      Process specific filename groups (comma-separated)
  --force-rebuild       Overwrite existing merged files
  --verbose             Enable verbose logging
  -h, --help            Show help message and exit
```

**Examples:**
```bash
# Normal processing
python notebooks/06d_merge_gpkg_files.py

# Process specific filename groups only
python notebooks/06d_merge_gpkg_files.py --filenames S_FRD_Proj_Ar,S_HUC_Ar

# Force rebuild all merged files
python notebooks/06d_merge_gpkg_files.py --force-rebuild
```

### Script 06: Legacy Extract and Merge (Combined)

```bash
# Basic usage (legacy)
python notebooks/06_extract_and_merge_shapefiles.py

# Available options:
python notebooks/06_extract_and_merge_shapefiles.py [OPTIONS]

Options:
  --config PATH          Use custom configuration file (default: config.json)
  --states CODES         Process specific states only (comma-separated, e.g., 01,02,04)
  --resume              Resume interrupted processing (default behavior)
  --force-rebuild       Clear all processing logs and rebuild from scratch
  --dry-run             Show what would be processed without doing it
  --verbose             Enable verbose logging
  --no-cleanup          Skip cleanup of temporary files
  -h, --help            Show help message and exit
```

**Note:** The legacy script combines extraction and merging in one step. The new four-step process (06a + 06b + 06c + 06d) is recommended for better control and error handling.

## Common Workflows

### Initial Setup and Testing
```bash
# 1. Test download with small batch
python notebooks/05_download_shapefiles.py --limit 10

# 2. Test extraction on specific state
python notebooks/06a_extract_zip_files.py --states 01 --verbose
python notebooks/06b_convert_shapefiles_to_gpkg.py --verbose

# 3. If tests successful, download all
python notebooks/05_download_shapefiles.py

# 4. Extract and process all
python notebooks/06a_extract_zip_files.py
python notebooks/06b_convert_shapefiles_to_gpkg.py
python notebooks/06c_create_clean_conversion_table.py
python notebooks/06d_merge_gpkg_files.py
```

### Handling New Data Updates
```bash
# When script 04 finds new communities or script 05 downloads new files:

# Option 1: Force rebuild everything
python notebooks/06a_extract_zip_files.py --force-rebuild
python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild
python notebooks/06c_create_clean_conversion_table.py --force-rebuild
python notebooks/06d_merge_gpkg_files.py --force-rebuild

# Option 2: Force rebuild specific states/products only
python notebooks/06a_extract_zip_files.py --force-rebuild --states 01,02,04
python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild

# Option 3: Check what would be rebuilt (dry run)
python notebooks/06a_extract_zip_files.py --force-rebuild --dry-run
python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild --dry-run
# Note: Scripts 06c and 06d don't have dry-run option
```

### Troubleshooting and Debugging
```bash
# Check what would be processed without doing it
python notebooks/06a_extract_zip_files.py --dry-run --verbose
python notebooks/06b_convert_shapefiles_to_gpkg.py --dry-run --verbose

# Process with detailed logging and keep temp files
python notebooks/06a_extract_zip_files.py --verbose --no-cleanup
python notebooks/06b_convert_shapefiles_to_gpkg.py --verbose
python notebooks/06c_create_clean_conversion_table.py --verbose
python notebooks/06d_merge_gpkg_files.py --verbose

# Process specific problematic state/product with full logging
python notebooks/06a_extract_zip_files.py --states 01 --verbose --no-cleanup
python notebooks/06b_convert_shapefiles_to_gpkg.py --verbose

# Handle encoding issues with special characters
python notebooks/06b_convert_shapefiles_to_gpkg.py --encoding latin-1 --verbose
```

### Production Workflows
```bash
# Resume normal processing (default behavior)
python notebooks/06a_extract_zip_files.py
python notebooks/06b_convert_shapefiles_to_gpkg.py
python notebooks/06c_create_clean_conversion_table.py
python notebooks/06d_merge_gpkg_files.py

# Process specific states in production
python notebooks/06a_extract_zip_files.py --states 01,02,04,05
python notebooks/06b_convert_shapefiles_to_gpkg.py

# Full rebuild for data refresh
python notebooks/06a_extract_zip_files.py --force-rebuild
python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild
python notebooks/06c_create_clean_conversion_table.py --force-rebuild
python notebooks/06d_merge_gpkg_files.py --force-rebuild
```

## Configuration

The project uses [`config.json`](config.json:1) for customizable settings. Copy [`config.sample.json`](config.sample.json:1) to `config.json` and customize as needed:

```bash
cp config.sample.json config.json
```

```json
{
  "download": {
    "base_path": "E:\\FEMA_DOWNLOAD",
    "rate_limit_seconds": 0.2,
    "chunk_size_bytes": 8192,
    "timeout_seconds": 30
  },
  "processing": {
    "extraction_base_path": "E:\\FEMA_EXTRACTED",
    "merged_output_path": "E:\\FEMA_MERGED",
    "shapefile_to_gpkg_path": "E:\\FEMA_SHAPEFILE_TO_GPKG",
    "temp_directory": "E:\\FEMA_TEMP",
    "target_crs": "EPSG:4326",
    "chunk_size_features": 10000,
    "memory_limit_mb": 2048,
    "parallel_processing": true,
    "max_workers": 4,
    "use_geopandas": false,
    "shapefile_encoding": "UTF-8"
  },
  "validation": {
    "geometry_validation": true,
    "fix_invalid_geometries": true,
    "skip_empty_geometries": true,
    "coordinate_precision": 6
  },
  "database": {
    "path": "meta_results/flood_risk_shapefiles.db"
  },
  "api": {
    "base_url": "https://msc.fema.gov",
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
  }
}
```

**Configuration Options:**
- `download.base_path`: Root directory for downloaded files
- `download.rate_limit_seconds`: Delay between downloads (default: 0.2s)
- `download.chunk_size_bytes`: Download chunk size (default: 8KB)
- `download.timeout_seconds`: Request timeout (default: 30s)
- `processing.extraction_base_path`: Temporary extraction directory
- `processing.merged_output_path`: Legacy output directory for merged GPKG files
- `processing.shapefile_to_gpkg_path`: Output directory for converted GPKG files
- `processing.target_crs`: Target coordinate system (default: EPSG:4326)
- `processing.memory_limit_mb`: Memory limit for processing (default: 2048MB)
- `processing.use_geopandas`: Use GeoPandas instead of ogr2ogr (default: false)
- `processing.shapefile_encoding`: Encoding for shapefile attributes (default: UTF-8)
- `validation.geometry_validation`: Enable geometry validation
- `validation.fix_invalid_geometries`: Automatically fix invalid geometries
- `database.path`: SQLite database location
- `api.base_url`: FEMA portal base URL
- `api.user_agent`: HTTP user agent string

## Database Schema

The SQLite database (`meta_results/flood_risk_shapefiles.db`) contains:

### Tables
- **`states`**: State codes and names
- **`counties`**: County information linked to states
- **`communities`**: Community information linked to counties
- **`shapefiles`**: Flood risk shapefile metadata
- **`request_log`**: API call tracking and error logging
- **`download_log`**: Download tracking (from script 05)
- **`extraction_log`**: Legacy ZIP extraction tracking (from script 06)
- **`extraction_06a_log`**: ZIP extraction tracking (from script 06a)
- **`shapefile_processing_log`**: Legacy GPKG creation tracking (from script 06)
- **`conversion_06b_log`**: GPKG conversion tracking (from script 06b)
- **`clean_conversion_table`**: Clean table with product_name, gpkg_path, filename (from script 06c)
- **`gpkg_filename_groups`**: Filename groups with counts (from script 06c)
- **`merge_06d_log`**: Merge tracking for filename groups (from script 06d)
- **`shapefile_contributions`**: Legacy individual shapefile contributions (from script 06)

### Key Fields
- `product_ID`: Unique FEMA product identifier
- `product_NAME`: Shapefile product name
- `product_FILE_PATH`: Download path for shapefile
- `product_FILE_SIZE`: File size information
- `product_POSTING_DATE_STRING`: When data was published

## Data Analysis Examples

### SQL Queries

```sql
-- Count shapefiles by state
SELECT s.state_name, COUNT(*) as shapefile_count 
FROM shapefiles sf 
JOIN states s ON sf.state_code = s.state_code 
GROUP BY s.state_name 
ORDER BY shapefile_count DESC;

-- Find largest shapefiles
SELECT product_name, product_file_size, product_file_path 
FROM shapefiles 
WHERE product_file_size LIKE '%MB' 
ORDER BY CAST(REPLACE(product_file_size, 'MB', '') AS INTEGER) DESC;

-- View recent shapefile updates
SELECT * FROM shapefiles 
ORDER BY product_posting_date DESC 
LIMIT 10;
```

### Python Analysis

```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('meta_results/flood_risk_shapefiles.db')

# Load data into pandas
df = pd.read_sql_query("""
    SELECT s.state_name, c.county_name, cm.community_name, 
           sf.product_name, sf.product_file_size
    FROM shapefiles sf
    JOIN states s ON sf.state_code = s.state_code
    JOIN counties c ON sf.county_code = c.county_code
    JOIN communities cm ON sf.community_code = cm.community_code
""", conn)

# Analyze data
print(df.groupby('state_name').size().sort_values(ascending=False))
```

## API Integration

The scripts interact with FEMA's portal using:
- **Base URL**: `https://msc.fema.gov/portal/advanceSearch`
- **Method**: POST with form data
- **Rate Limiting**: 0.1-second delays between requests
- **Error Handling**: Comprehensive logging of failed requests

### Form Data Structure
```python
{
    'utf8': '✓',
    'affiliate': 'fema',
    'selstate': '01',        # State code
    'selcounty': '01001',    # County code  
    'selcommunity': '01001C', # Community code
    'method': 'search'
}
```

## Features

- ✅ **Complete Coverage**: All 57 states/territories, 3,176 counties, 30,704 communities
- ✅ **Structured Storage**: SQLite database with proper relationships
- ✅ **Error Handling**: Comprehensive logging and retry mechanisms
- ✅ **Progress Tracking**: County-level progress with completion summaries
- ✅ **Data Validation**: Filters for FLOOD_RISK_DB ShapeFiles only
- ✅ **Performance Optimized**: Efficient API calls with rate limiting

## Data Statistics

Based on collection runs:
- **Total Communities**: 30,704
- **Shapefiles Found**: 5,000+ (varies by availability)
- **Top States by Shapefiles**: Pennsylvania, Texas, Michigan, New York, Illinois
- **Processing Time**: ~3-4 hours for complete dataset

## Download Functionality

### Automated Download
The [`05_download_shapefiles.py`](notebooks/05_download_shapefiles.py:1) script automatically downloads all discovered shapefiles:

**Features:**
- **Organized Storage**: `E:\FEMA_DOWNLOAD\{state_code}\{county_code}\`
- **Resume Capability**: Continues interrupted downloads
- **Progress Tracking**: Shows download progress and statistics
- **Error Handling**: Logs failed downloads for retry
- **Duplicate Prevention**: Skips already downloaded files

**Folder Structure:**
```
E:\FEMA_DOWNLOAD\
├── 01\                    # Alabama
│   ├── 01001\            # Autauga County
│   │   ├── FRD_01001C_shapefiles_20140221.zip
│   │   └── ...
│   └── 01003\            # Baldwin County
├── 02\                    # Alaska
└── ...
```

**Download Tracking:**
- Creates `download_log` table in SQLite database
- Tracks success/failure status for each file
- Records file sizes and error messages
- Enables download resume and retry functionality

### Manual Download URLs

Individual shapefiles can be downloaded using:
```
https://msc.fema.gov/portal/downloadProduct?productTypeID=FLOOD_RISK_PRODUCT&productSubTypeID=FLOOD_RISK_DB&productID={product_name}
```

Example:
```
https://msc.fema.gov/portal/downloadProduct?productTypeID=FLOOD_RISK_PRODUCT&productSubTypeID=FLOOD_RISK_DB&productID=FRD_03150201_shapefiles_20140221
```

## Extraction and Processing Functionality

### Two-Step Processing (Recommended)

#### Step 1: ZIP Extraction
The [`06a_extract_zip_files.py`](notebooks/06a_extract_zip_files.py:1) script extracts downloaded ZIP files:

**Features:**
- **ZIP Extraction**: Extracts all downloaded ZIP files to organized directories
- **Duplicate Prevention**: Uses SQL query to avoid processing duplicate products
- **Detailed Logging**: Tracks each shapefile found within ZIP files
- **Simplified Structure**: Organizes by product name rather than state/county

**Output Structure:**
```
E:\FEMA_EXTRACTED\
├── FRD_01001C_Shapefiles\        # Product name as folder
│   ├── S_CSLF_Ar.shp
│   ├── S_CSLF_Ar.dbf
│   └── ... (all extracted files)
├── FRD_03150201_shapefiles_20140221\
└── ... (all products)
```

**Processing Tracking:**
- Creates `extraction_06a_log` table with detailed shapefile tracking
- Records both relative path within ZIP and shapefile name
- Enables resume capability for interrupted processing

#### Step 2: Shapefile Conversion
The [`06b_convert_shapefiles_to_gpkg.py`](notebooks/06b_convert_shapefiles_to_gpkg.py:1) script converts shapefiles to GeoPackage:

**Features:**
- **ogr2ogr Integration**: Uses ogr2ogr by default for robust conversion
- **Encoding Handling**: Supports different encodings for problematic files
- **Thread-Safe Processing**: Creates thread-local database connections
- **Parallel Processing**: Optional multi-threaded conversion
- **Detailed Logging**: Tracks conversion success/failure
- **Temporary Directory**: Uses fast disk for temporary operations
- **Strict Mode**: Option to stop on first error or warning

**Output Structure:**
```
E:\FEMA_SHAPEFILE_TO_GPKG\
├── FRD_01001C_Shapefiles\        # Product name as folder
│   ├── S_CSLF_Ar.gpkg
│   ├── S_FRD_Proj_Ar.gpkg
│   └── ... (all converted files)
├── FRD_03150201_shapefiles_20140221\
└── ... (all products)
```

**Processing Tracking:**
- Creates `conversion_06b_log` table with conversion tracking
- Records source shapefile and destination GPKG paths
- Enables resume capability for interrupted processing

#### Step 3: Create Clean Conversion Table
The [`06c_create_clean_conversion_table.py`](notebooks/06c_create_clean_conversion_table.py:1) script creates clean tables for analysis:

**Features:**
- **Clean Table Creation**: Creates simplified tables for analysis
- **Filename Extraction**: Extracts filename without path and extension
- **Grouping**: Creates table with filename groups and counts
- **Detailed Reporting**: Generates statistics about filename groups

**Output Tables:**
- `clean_conversion_table`: Contains product_name, gpkg_path, and filename
- `gpkg_filename_groups`: Contains filename and count for each unique filename

#### Step 4: Merge GeoPackage Files by Filename
The [`06d_merge_gpkg_files.py`](notebooks/06d_merge_gpkg_files.py:1) script merges GeoPackage files by filename group:

**Features:**
- **Filename-based Merging**: Merges files with the same filename across products
- **ogr2ogr Integration**: Uses ogr2ogr for efficient merging
- **Temporary Directory**: Uses fast disk for temporary operations
- **Detailed Logging**: Tracks merge success/failure

**Output Structure:**
```
E:\FEMA_MERGED\
├── S_FRD_Proj_Ar.gpkg           # Merged project area files
├── S_HUC_Ar.gpkg                # Merged HUC area files
└── ... (all merged files by filename)
```

**Processing Tracking:**
- Creates `merge_06d_log` table with merge tracking
- Records source files count and destination path
- Enables resume capability for interrupted processing

### Legacy Combined Processing
The [`06_extract_and_merge_shapefiles.py`](notebooks/06_extract_and_merge_shapefiles.py:1) script combines extraction and merging (legacy):

**Features:**
- **Combined Workflow**: Extracts and merges in one step
- **State-based Organization**: Organizes by state rather than product
- **Shapefile Merging**: Combines shapefiles by type within each state

**Output Structure:**
```
E:\FEMA_MERGED\
├── 01\                           # Alabama
│   ├── R_UDF_Losses_by_Building.gpkg
│   ├── R_UDF_Losses_by_Parcel.gpkg
│   ├── S_CSLF_Ar.gpkg
│   └── ... (all available shapefile types)
├── 02\                           # Alaska
└── ... (all 57 states/territories)
```

**Note:** The legacy script is maintained for backward compatibility, but the two-step process is recommended for better control and error handling.

## Development Standards

This project includes Roo development rules in [`.roo/rules/`](.roo/rules/:1) for consistent code quality:

### Project Standards ([`.roo/rules/01-project-standards.md`](.roo/rules/01-project-standards.md:1))
- Code quality and documentation requirements
- Database best practices and security
- API integration guidelines
- File organization and naming conventions
- Configuration management standards

### Python Standards ([`.roo/rules-code/02-python-standards.md`](.roo/rules-code/02-python-standards.md:1))
- PEP 8 compliance and style guidelines
- Function documentation with Google-style docstrings
- Import organization and naming conventions
- Database operations and API request patterns
- Error handling and progress tracking best practices

These rules automatically apply when using Roo Code for development, ensuring consistent code quality and maintainability.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Follow the development standards in `.roo/rules/`
4. Make your changes with proper documentation
5. Test with sample data
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [`LICENSE`](LICENSE:1) file for details.

### FEMA Data Usage
The FEMA flood risk data accessed by this software is in the public domain. According to FEMA's official policy: "All data disseminated by FEMA are considered public information and may be distributed freely, with appropriate citation."

Original data source: [FEMA Map Service Center](https://msc.fema.gov/portal/home)

## Support

For issues or questions:
1. Check the `request_log` table for API errors
2. Verify network connectivity to FEMA portal
3. Review rate limiting if experiencing timeouts
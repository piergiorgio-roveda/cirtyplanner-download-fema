# FEMA Flood Risk Shapefile Data Collector

A comprehensive Python toolkit for collecting and analyzing FEMA flood risk shapefile data across all US states, counties, and communities.

## Overview

This project provides automated tools to:
- Extract state, county, and community data from FEMA's portal
- Collect flood risk shapefile information for all jurisdictions
- Store data in a structured SQLite database for analysis
- Generate comprehensive reports and statistics

## Project Structure

```
├── notebooks/                    # Python scripts for data collection
│   ├── 01_get_all_state.py      # Extract all US states/territories
│   ├── 02_get_all_counties.py   # Extract counties for each state
│   ├── 03_get_all_communities.py # Extract communities for each county
│   ├── 04_get_flood_risk_shapefiles.py # Collect shapefile data
│   └── 05_download_shapefiles.py # Download all shapefile ZIP files
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
└── meta_results_sample/          # Sample data for testing
```

## Quick Start

### Prerequisites

```bash
pip install requests sqlite3
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

5. **Download All Shapefiles**:
   ```bash
   python notebooks/05_download_shapefiles.py
   ```
   Downloads to: `E:\FEMA_DOWNLOAD\{state}\{county}\`

## Database Schema

The SQLite database (`meta_results/flood_risk_shapefiles.db`) contains:

### Tables
- **`states`**: State codes and names
- **`counties`**: County information linked to states
- **`communities`**: Community information linked to counties
- **`shapefiles`**: Flood risk shapefile metadata
- **`request_log`**: API call tracking and error logging

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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with sample data
5. Submit a pull request

## License

This project is for educational and research purposes. FEMA data is public domain.

## Support

For issues or questions:
1. Check the `request_log` table for API errors
2. Verify network connectivity to FEMA portal
3. Review rate limiting if experiencing timeouts
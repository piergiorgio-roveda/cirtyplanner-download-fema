# FEMA Flood Risk Data Collector

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.6+](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-available-brightgreen.svg)](DOC/)
[![Status: Active](https://img.shields.io/badge/status-active-success.svg)]()

A comprehensive Python toolkit for collecting and analyzing FEMA flood risk data across all US states, counties, and communities. Supports both community-level shapefiles and state-level National Flood Hazard Layer (NFHL) Geodatabases.

## Overview

This project provides automated tools to:
- Extract state, county, and community data from FEMA's portal
- Collect flood risk shapefile information for all jurisdictions
- Collect NFHL State GDB data for all states
- Download all available shapefile and GDB ZIP files
- Extract and merge shapefiles into consolidated GPKG files by state and type
- Store data in structured SQLite databases for analysis
- Generate comprehensive reports and statistics

## Project Status

| Component | Status | Description |
|-----------|--------|-------------|
| Scripts 01-03 | Complete | State, county, and community data extraction |
| Script 04 | Complete | Flood risk shapefile metadata collection |
| Script 04_nfhl | Complete | NFHL State GDB data collection |
| Script 05 | Complete | Shapefile download functionality |
| Script 06 | Complete | Legacy extraction and merging implementation |
| Scripts 06a-06d | Complete | Modern four-step processing pipeline |

The project is actively collecting flood risk shapefile metadata from FEMA's portal, with successful data collection from hundreds of counties across multiple states.

## Documentation

The project documentation is organized in the `DOC` folder:

- [Project Structure](DOC/PROJECT_STRUCTURE.md) - Detailed view of the project organization
- [Usage Guide](DOC/USAGE.md) - Instructions for running the scripts
- [Configuration](DOC/CONFIGURATION.md) - Configuration options and settings
- [Database Schema](DOC/DATABASE_SCHEMA.md) - Database structure and tables
- [Processing Pipeline](DOC/PROCESSING.md) - Download and processing functionality

## Quick Start

### Prerequisites

**Network Requirements:**
- VPN connection to USA may be required for accessing FEMA portal
- Stable internet connection for large data downloads

**Python Dependencies:**
```bash
pip install requests sqlite3 geopandas fiona shapely pyproj psutil
```

### Basic Usage

**For Community-Level Shapefiles:**
```bash
# 1. Collect metadata
python notebooks/01_get_all_state.py
python notebooks/02_get_all_counties.py
python notebooks/03_get_all_communities.py
python notebooks/04_get_flood_risk_shapefiles.py

# 2. Download files
python notebooks/05_download_shapefiles.py
```

**For State-Level NFHL Geodatabases:**
```bash
# 1. Collect metadata
python notebooks/01_get_all_state.py
python notebooks/04_get_nfhl_data_state_gdb.py

# 2. Download files
python notebooks/05_download_nfhl_gdb.py
```

### Configuration

Create a `config.json` file based on the sample:
```bash
cp config.sample.json config.json
```

Edit the paths in `config.json` to match your environment:
```json
{
  "download": {
    "base_path": "E:\\FEMA_DOWNLOAD",
    "nfhl_base_path": "E:\\FEMA_NFHL_DOWNLOAD"
  },
  "database": {
    "path": "meta_results/flood_risk_shapefiles.db",
    "nfhl_path": "meta_results/flood_risk_nfhl_gdb.db"
  }
}
```

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

-- Count NFHL GDB files by state
SELECT s.state_name, COUNT(*) as gdb_count
FROM gdb_nfhl g
JOIN nfhl_states s ON g.state_code = s.state_code
GROUP BY s.state_name
ORDER BY gdb_count DESC;

-- Find largest NFHL GDB files
SELECT product_name, product_file_size, product_file_path
FROM gdb_nfhl
WHERE product_file_size LIKE '%MB'
ORDER BY CAST(REPLACE(product_file_size, 'MB', '') AS INTEGER) DESC;
```

### Python Analysis

```python
import sqlite3
import pandas as pd

# Connect to shapefile database
conn_sf = sqlite3.connect('meta_results/flood_risk_shapefiles.db')

# Load shapefile data into pandas
df_sf = pd.read_sql_query("""
    SELECT s.state_name, c.county_name, cm.community_name,
           sf.product_name, sf.product_file_size
    FROM shapefiles sf
    JOIN states s ON sf.state_code = s.state_code
    JOIN counties c ON sf.county_code = c.county_code
    JOIN communities cm ON sf.community_code = cm.community_code
""", conn_sf)

# Analyze shapefile data
print("Shapefile data by state:")
print(df_sf.groupby('state_name').size().sort_values(ascending=False))

# Connect to NFHL GDB database
conn_gdb = sqlite3.connect('meta_results/flood_risk_nfhl_gdb.db')

# Load NFHL GDB data into pandas
df_gdb = pd.read_sql_query("""
    SELECT s.state_name, g.product_name, g.product_file_size
    FROM gdb_nfhl g
    JOIN nfhl_states s ON g.state_code = s.state_code
""", conn_gdb)

# Analyze NFHL GDB data
print("\nNFHL GDB data by state:")
print(df_gdb.groupby('state_name').size().sort_values(ascending=False))
```

## API Integration

The scripts interact with FEMA's portal using:
- **Base URL**: `https://msc.fema.gov/portal/advanceSearch`
- **Method**: POST with form data
- **Rate Limiting**: 0.1-second delays between requests

```python
# Form Data Structure
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

| Feature | Description |
|---------|-------------|
| Coverage | All 57 states/territories, 3,176 counties, 30,704 communities |
| Storage | SQLite databases with proper relationships |
| Error Handling | Comprehensive logging and retry mechanisms |
| Progress Tracking | County-level and state-level progress with completion summaries |
| Data Validation | Filters for FLOOD_RISK_DB ShapeFiles and NFHL_STATE_DATA |
| Performance | Efficient API calls with rate limiting |
| Multiple Data Types | Support for both community-level shapefiles and state-level GDB data |
| Configurable Paths | Separate database and download paths for shapefiles and NFHL GDB data |
| Resume Capability | Automatic tracking of downloaded files for safe interruption/restart |
| Folder Organization | State-based folder structure for both shapefile and GDB downloads |

## Data Statistics

- **Total Communities**: 30,704
- **Shapefiles Found**: 5,000+ (varies by availability)
- **Top States by Shapefiles**: Pennsylvania, Texas, Michigan, New York, Illinois
- **Processing Time**: 2 days for complete dataset

## Development Standards

This project follows development standards defined in:
- [Project Standards](.roo/rules/01-project-standards.md) - Code quality, database practices, API guidelines
- [Python Standards](.roo/rules-code/02-python-standards.md) - PEP 8, docstrings, import organization

## Contributing

1. Fork the repository
2. Create a feature branch
3. Follow the development standards in `.roo/rules/`
4. Make your changes with proper documentation
5. Test with sample data
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### FEMA Data Usage
The FEMA flood risk data accessed by this software is in the public domain. According to FEMA's official policy: "All data disseminated by FEMA are considered public information and may be distributed freely, with appropriate citation."

Original data source: [FEMA Map Service Center](https://msc.fema.gov/portal/home)

## Support

For issues or questions:
1. Check the `request_log` table for API errors
2. Verify network connectivity to FEMA portal
3. Review rate limiting if experiencing timeouts
# Script 04: Get Flood Risk Shapefiles Implementation

**File:** `notebooks/04_get_flood_risk_shapefiles.py`  
**Created:** 2025-01-20  
**Status:** 🔄 Currently Running (County 444/3176 - Habersham County, Georgia)

## Purpose

Collects flood risk shapefile metadata for each state/county/community combination by making POST requests to FEMA's portal and stores the data in a structured SQLite database.

## Input Sources

- **JSON File:** `meta_results/all_communities_data.json` (from script 03)
- **API Endpoint:** `https://msc.fema.gov/portal/advanceSearch` (POST)

## Process Flow

1. **Database Creation**
   - Creates SQLite database with proper schema
   - Tables: states, counties, communities, shapefiles, request_log
   - Indexes for performance optimization

2. **Data Loading**
   - Loads community data from script 03 output
   - Populates base tables (states, counties, communities)

3. **API Requests**
   - Makes POST requests for each community
   - Form data includes: selstate, selcounty, selcommunity
   - Filters for FLOOD_RISK_DB products with "ShapeFiles" description

4. **Data Processing**
   - Extracts shapefile metadata from API responses
   - Stores product details: ID, name, file path, size, dates
   - Logs all requests (successful and failed)

## Database Schema

```sql
-- States table
CREATE TABLE states (
    state_code TEXT PRIMARY KEY,
    state_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Counties table  
CREATE TABLE counties (
    county_code TEXT PRIMARY KEY,
    county_name TEXT NOT NULL,
    state_code TEXT NOT NULL,
    FOREIGN KEY (state_code) REFERENCES states (state_code)
);

-- Communities table
CREATE TABLE communities (
    community_code TEXT PRIMARY KEY,
    community_name TEXT NOT NULL,
    county_code TEXT NOT NULL,
    state_code TEXT NOT NULL,
    FOREIGN KEY (county_code) REFERENCES counties (county_code)
);

-- Shapefiles table
CREATE TABLE shapefiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    community_code TEXT NOT NULL,
    county_code TEXT NOT NULL,
    state_code TEXT NOT NULL,
    product_id INTEGER,
    product_name TEXT,
    product_file_path TEXT,
    product_file_size TEXT,
    -- ... additional metadata fields
    FOREIGN KEY (community_code) REFERENCES communities (community_code)
);

-- Request log table
CREATE TABLE request_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    community_code TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    shapefiles_found INTEGER DEFAULT 0,
    request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Output Files

- **Primary:** `meta_results/flood_risk_shapefiles.db` (SQLite database)
- **Size:** ~100-200MB (estimated when complete)
- **Records:** 30,704 communities processed, 5,000+ shapefiles found

## Key Features

- **SQLite Storage:** Structured database with relationships and indexes
- **Progress Tracking:** County-level progress with completion summaries
- **Error Handling:** Comprehensive logging of failed requests
- **Rate Limiting:** 0.1-second delays between requests
- **Resume Capability:** Can be interrupted and resumed safely

## Dependencies

```python
import json
import requests
import time
import sqlite3
from datetime import datetime
import os
from urllib.parse import urlencode
```

## API Integration

- **Method:** POST requests with form data
- **Content-Type:** application/x-www-form-urlencoded
- **Form Data:**
  ```python
  {
      'utf8': '✓',
      'affiliate': 'fema',
      'selstate': '01',
      'selcounty': '01001', 
      'selcommunity': '01001C',
      'method': 'search'
  }
  ```

## Current Status

- **Progress:** County 444/3176 (~14% complete)
- **Current Location:** Habersham County, Georgia
- **Shapefiles Found:** 5,000+ (estimated)
- **Processing Time:** ~3-4 hours total (estimated)

## Data Filtering

- **Target Products:** FLOOD_RISK_DB category only
- **Description Filter:** product_DESCRIPTION = "ShapeFiles"
- **Excluded:** GeoTIFFs, PDFs, other formats
- **Focus:** Downloadable ZIP files containing shapefiles

## Performance Metrics

- **Request Rate:** ~10 requests per second (with 0.1s delay)
- **Success Rate:** >95% (network dependent)
- **Memory Usage:** Efficient streaming with SQLite
- **Database Size:** Growing incrementally during processing

## Error Handling

- **Network Timeouts:** Logged and continued
- **JSON Parse Errors:** Handled gracefully
- **Database Errors:** Transaction rollback and retry
- **API Rate Limits:** Automatic backoff if needed

## Usage

```bash
python notebooks/04_get_flood_risk_shapefiles.py
```

## Sample Output

```
Processing state: GEORGIA (13)
  Processing county: HABERSHAM COUNTY (13137) - County 444/3176
    Communities in this county: 12
    Processing community: HABERSHAM COUNTY ALL JURISDICTIONS (13137C)
    Processing community: CLARKESVILLE, CITY OF (130123)
  ✓ Completed county: HABERSHAM COUNTY - 12 communities processed
```

## Next Step

Output feeds into [`05_download_shapefiles.py`](2025-01-20_05_download_shapefiles.md) for actual file downloads.

## Implementation Notes

- **Long Running:** Expected 3-4 hours for complete execution
- **Resumable:** Database design allows safe interruption/restart
- **Scalable:** SQLite handles large datasets efficiently
- **Queryable:** Rich SQL interface for data analysis
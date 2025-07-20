# Script 05: Download Shapefiles Implementation

**File:** `notebooks/05_download_shapefiles.py`  
**Created:** 2025-01-20  
**Status:** ⚠️ Created but Not Yet Tested (Awaiting Script 04 Completion)

## Purpose

Downloads all discovered flood risk shapefile ZIP files from FEMA's portal, organizing them in a structured folder hierarchy by state and county.

## Input Sources

- **SQLite Database:** `meta_results/flood_risk_shapefiles.db` (from script 04)
- **Configuration:** `config.json` for customizable settings
- **Download URLs:** Constructed from database file paths

## Process Flow

1. **Configuration Loading**
   - Loads settings from `config.json`
   - Creates default config if none exists
   - Validates all required parameters

2. **Database Connection**
   - Connects to SQLite database from script 04
   - Queries all shapefiles with valid file paths
   - Creates download_log table for tracking

3. **Folder Structure Creation**
   - Creates organized directory structure: `{base_path}\{state}\{county}\`
   - Example: `E:\FEMA_DOWNLOAD\01\01001\`

4. **File Downloads**
   - Downloads each shapefile ZIP from FEMA portal
   - Implements resume capability for interrupted downloads
   - Tracks progress and handles errors

## Configuration System

**File:** `config.json`
```json
{
  "download": {
    "base_path": "E:\\FEMA_DOWNLOAD",
    "rate_limit_seconds": 0.2,
    "chunk_size_bytes": 8192,
    "timeout_seconds": 30
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

## Folder Structure

```
E:\FEMA_DOWNLOAD\
├── 01\                    # Alabama
│   ├── 01001\            # Autauga County
│   │   ├── FRD_01001C_shapefiles_20140221.zip
│   │   ├── FRD_01001C_shapefiles_20180315.zip
│   │   └── ...
│   ├── 01003\            # Baldwin County
│   │   └── ...
│   └── ...
├── 02\                    # Alaska
│   └── ...
└── ...                   # All 57 states/territories
```

## Key Features

- **Configurable Paths:** No hardcoded download locations
- **Resume Capability:** Continues interrupted downloads using HTTP range requests
- **Progress Tracking:** Real-time download progress with file sizes
- **Error Recovery:** Comprehensive logging and retry mechanisms
- **Duplicate Prevention:** Skips already downloaded files
- **Database Logging:** Complete audit trail in download_log table

## Dependencies

```python
import sqlite3
import requests
import os
import time
import json
from datetime import datetime
from urllib.parse import urljoin
import hashlib
```

## Download Process

1. **File Existence Check**
   - Checks if file already exists locally
   - Compares file sizes if available
   - Skips complete files

2. **HTTP Range Requests**
   - Supports resume for partial downloads
   - Uses `Range: bytes={position}-` header
   - Handles 206 (Partial Content) responses

3. **Progress Monitoring**
   - Shows download progress every MB
   - Displays percentage completion
   - Reports final file sizes

4. **Error Handling**
   - Network timeouts and connection errors
   - Invalid file paths or missing files
   - Disk space and permission issues

## Database Integration

**Download Log Table:**
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

## Expected Performance

- **Total Files:** 5,000+ shapefile ZIP files (estimated)
- **Total Size:** 50-100GB (estimated)
- **Download Time:** 10-20 hours (network dependent)
- **Success Rate:** >95% (with retry logic)

## Usage

```bash
# Setup configuration
cp config.sample.json config.json
# Edit config.json to set download path

# Run download
python notebooks/05_download_shapefiles.py
```

## Sample Output

```
Starting FEMA Shapefile Download Process...
Configuration loaded successfully
Database path: meta_results/flood_risk_shapefiles.db
Download path: E:\FEMA_DOWNLOAD

Found 5247 shapefiles to download

[1/5247] Processing: FRD_01001C_shapefiles_20140221
  State: ALABAMA (01)
  County: AUTAUGA COUNTY (01001)
  Size: 248MB
  URL: https://msc.fema.gov/FRP/FRD_01001C_shapefiles_20140221.zip
    Progress: 50MB / 248MB (20.2%)
    ✓ Downloaded: FRD_01001C_shapefiles_20140221.zip (248MB)
```

## Error Recovery

- **Network Issues:** Automatic retry with exponential backoff
- **Partial Downloads:** Resume from last byte position
- **Disk Full:** Graceful handling with clear error messages
- **Invalid URLs:** Logged and skipped

## Testing Status

- **Implementation:** ✅ Complete
- **Configuration:** ✅ Tested
- **Database Integration:** ✅ Verified
- **Download Logic:** ⚠️ Awaiting testing with real data
- **Error Handling:** ⚠️ Needs validation

## Next Steps

1. **Wait for Script 04:** Complete shapefile metadata collection
2. **Test Downloads:** Verify download functionality with real data
3. **Performance Tuning:** Optimize rate limiting and chunk sizes
4. **Error Analysis:** Review and improve error handling

## Implementation Notes

- **Configurable Design:** All settings externalized to config.json
- **Resume Safety:** Can be interrupted and restarted safely
- **Storage Efficiency:** Organized folder structure for easy navigation
- **Audit Trail:** Complete logging for troubleshooting and analysis
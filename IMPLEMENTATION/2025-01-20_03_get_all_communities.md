# Script 03: Get All Communities Implementation

**File:** `notebooks/03_get_all_communities.py`  
**Created:** 2025-01-20  
**Status:** ✅ Completed and Tested

## Purpose

Fetches all communities for each county by making API calls to FEMA's portal, creating the complete hierarchical dataset of states → counties → communities.

## Input Sources

- **JSON File:** `meta_results/all_counties_data.json` (from script 02)
- **API Endpoint:** `https://msc.fema.gov/portal/advanceSearch?getCommunity={county_code}&state={state_code}`

## Process Flow

1. **County Data Loading**
   - Reads county data from script 02 output
   - Processes 3,176+ counties across 57 states

2. **API Requests**
   - Makes GET requests for each county
   - URL pattern: `https://msc.fema.gov/portal/advanceSearch?getCommunity=01001&state=01`
   - Implements rate limiting (0.3 seconds between requests)

3. **Data Processing**
   - Parses JSON responses containing community arrays
   - Extracts community `label` (name) and `value` (code)
   - Handles counties with no communities

4. **Data Structure Creation**
   ```json
   {
     "metadata": {
       "total_states": 57,
       "total_counties": 3176,
       "total_communities": 30704,
       "fetch_timestamp": "2025-01-20 12:00:00 UTC"
     },
     "states": {
       "01": {
         "state_name": "ALABAMA",
         "state_code": "01",
         "county_count": 67,
         "community_count": 632,
         "counties": {
           "01001": {
             "county_name": "AUTAUGA COUNTY",
             "county_code": "01001",
             "community_count": 6,
             "communities": [
               {"label": "AUTAUGA COUNTY ALL JURISDICTIONS", "value": "01001C"},
               {"label": "AUTAUGA COUNTY UNINCORPORATED AREAS", "value": "010314"}
             ]
           }
         }
       }
     }
   }
   ```

## Output Files

- **Primary:** `meta_results/all_communities_data.json` (~50MB)
- **Summary:** `meta_results/communities_summary.json` (~200KB)
- **Records:** 30,704+ communities across all counties

## Key Features

- **Complete Coverage:** All communities in all US counties
- **Hierarchical Structure:** Maintains state → county → community relationships
- **Progress Tracking:** County-level progress updates every 10 counties
- **Error Resilience:** Handles API failures and empty responses
- **Statistical Analysis:** Comprehensive summary generation

## Dependencies

```python
import json
import requests
import time
from datetime import datetime
import os
```

## API Integration

- **Method:** GET requests with query parameters
- **Parameters:** `getCommunity={county_code}&state={state_code}`
- **Rate Limit:** 0.3 seconds between requests
- **Timeout:** 10 seconds per request
- **Error Handling:** Comprehensive logging and retry logic

## Usage

```bash
python notebooks/03_get_all_communities.py
```

## Output Statistics

- **Total Communities:** 30,704+
- **Top States by Communities:**
  1. Pennsylvania: 2,655 communities
  2. Texas: 1,700 communities
  3. Michigan: 1,674 communities
  4. New York: 1,587 communities
  5. Illinois: 1,506 communities

## Community Types

- **All Jurisdictions:** County-wide coverage (e.g., "01001C")
- **Unincorporated Areas:** Rural/unincorporated regions
- **Incorporated Cities:** Individual municipalities
- **Special Districts:** Flood control districts, etc.

## Data Validation

- **Community Code Format:** Various formats (5-6 digits, alphanumeric)
- **Name Consistency:** Standardized community naming
- **Relationship Integrity:** Validates county-community relationships
- **Completeness Check:** Ensures all counties are processed

## Performance Metrics

- **Processing Time:** ~3-4 hours for complete dataset
- **API Calls:** 3,176+ requests (one per county)
- **Success Rate:** >99% (occasional timeouts handled)
- **Memory Usage:** Efficient streaming processing

## Error Scenarios

- **Network Issues:** Automatic retry with exponential backoff
- **Empty Counties:** Logged but not treated as errors
- **Invalid JSON:** Skipped with detailed error logging
- **Rate Limiting:** Automatic delay adjustment

## Next Step

Output feeds into [`04_get_flood_risk_shapefiles.py`](2025-01-20_04_get_flood_risk_shapefiles.md) for shapefile metadata collection.

## Implementation Notes

- **Data Volume:** Largest dataset in the pipeline (~50MB JSON)
- **API Stability:** Generally reliable but requires robust error handling
- **Community Codes:** Not standardized format (varies by jurisdiction)
- **Processing Strategy:** Sequential processing with progress tracking
# Script 02: Get All Counties Implementation

**File:** `notebooks/02_get_all_counties.py`  
**Created:** 2025-01-20  
**Status:** ✅ Completed and Tested

## Purpose

Fetches all counties for each state/territory by making API calls to FEMA's portal and structures the data for further processing.

## Input Sources

- **JSON File:** `meta_results/states_data.json` (from script 01)
- **API Endpoint:** `https://msc.fema.gov/portal/advanceSearch?getCounty={state_code}`

## Process Flow

1. **State Data Loading**
   - Reads states data from script 01 output
   - Iterates through all 57 states/territories

2. **API Requests**
   - Makes GET requests to FEMA portal for each state
   - URL pattern: `https://msc.fema.gov/portal/advanceSearch?getCounty=01`
   - Implements rate limiting (0.3 seconds between requests)

3. **Data Processing**
   - Parses JSON responses containing county arrays
   - Extracts county `label` (name) and `value` (code)
   - Handles API errors and empty responses

4. **Data Structure Creation**
   ```json
   {
     "metadata": {
       "total_states": 57,
       "total_counties": 3176,
       "fetch_timestamp": "2025-01-20 11:00:00 UTC"
     },
     "states": {
       "01": {
         "state_name": "ALABAMA",
         "state_code": "01",
         "county_count": 67,
         "counties": [
           {"label": "AUTAUGA COUNTY", "value": "01001"},
           {"label": "BALDWIN COUNTY", "value": "01003"}
         ]
       }
     }
   }
   ```

## Output Files

- **Primary:** `meta_results/all_counties_data.json` (~500KB)
- **Summary:** `meta_results/counties_summary.json` (~50KB)
- **Records:** 3,176+ counties across all states

## Key Features

- **Complete Coverage:** All counties in all US states and territories
- **Error Handling:** Robust handling of API failures and timeouts
- **Rate Limiting:** Respectful API usage with delays
- **Progress Tracking:** Real-time progress updates during execution
- **Summary Generation:** Statistical analysis of county distribution

## Dependencies

```python
import json
import requests
import time
from datetime import datetime
import os
```

## API Integration

- **Method:** GET requests
- **Rate Limit:** 0.3 seconds between requests
- **Timeout:** 10 seconds per request
- **Error Handling:** Retry logic for failed requests
- **User Agent:** Standard browser user agent string

## Usage

```bash
python notebooks/02_get_all_counties.py
```

## Output Statistics

- **Total Counties:** 3,176+
- **Largest State:** Texas (254 counties)
- **Smallest State:** Delaware (3 counties)
- **Processing Time:** ~20-30 minutes

## Data Validation

- **County Code Format:** 5-digit FIPS codes (e.g., "01001")
- **Name Consistency:** Standardized county naming
- **Completeness Check:** Validates all states have counties
- **Duplicate Detection:** Ensures unique county codes

## Error Scenarios

- **Network Timeouts:** Logged and retried
- **Invalid JSON:** Skipped with error logging
- **Empty Responses:** Handled gracefully
- **Rate Limiting:** Automatic backoff implemented

## Next Step

Output feeds into [`03_get_all_communities.py`](2025-01-20_03_get_all_communities.md) for community extraction.

## Implementation Notes

- **API Reliability:** FEMA API is generally stable but occasional timeouts occur
- **Data Consistency:** County codes follow FIPS standard
- **Memory Usage:** Efficient processing of large datasets
- **Resumability:** Can be rerun safely (overwrites existing data)
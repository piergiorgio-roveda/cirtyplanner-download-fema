# Script 01: Get All States Implementation

**File:** `notebooks/01_get_all_state.py`  
**Created:** 2025-01-20  
**Status:** ✅ Completed and Tested

## Purpose

Extracts all US states and territories from FEMA's state dropdown HTML and converts them to structured JSON format.

## Input Sources

- **HTML File:** [`meta/state.html`](../meta/state.html) - FEMA state dropdown HTML
- **Source:** FEMA Map Service Center state selection dropdown

## Process Flow

1. **HTML Parsing**
   - Reads the HTML file containing FEMA's state dropdown
   - Uses BeautifulSoup to parse HTML structure
   - Extracts `<option>` elements with state codes and names

2. **Data Extraction**
   - Filters out the default "-- Select --" option
   - Extracts `value` attribute as state code (e.g., "01", "02")
   - Extracts text content as state name (e.g., "ALABAMA", "ALASKA")

3. **Data Structure Creation**
   ```json
   {
     "metadata": {
       "total_states": 57,
       "extraction_timestamp": "2025-01-20 10:30:00 UTC"
     },
     "states": {
       "01": {
         "state_code": "01",
         "state_name": "ALABAMA"
       },
       "02": {
         "state_code": "02", 
         "state_name": "ALASKA"
       }
     }
   }
   ```

## Output Files

- **Primary:** `meta_results/states_data.json`
- **Size:** ~2KB
- **Records:** 57 states and territories

## Key Features

- **Complete Coverage:** All 50 states plus territories (Puerto Rico, Virgin Islands, etc.)
- **Standardized Format:** Consistent state code and name structure
- **Metadata Tracking:** Includes extraction timestamp and totals
- **Error Handling:** Validates HTML structure and handles parsing errors

## Dependencies

```python
import json
import os
from bs4 import BeautifulSoup
from datetime import datetime
```

## Usage

```bash
python notebooks/01_get_all_state.py
```

## Output Example

```json
{
  "metadata": {
    "total_states": 57,
    "extraction_timestamp": "2025-01-20 10:30:00 UTC"
  },
  "states": {
    "01": {"state_code": "01", "state_name": "ALABAMA"},
    "02": {"state_code": "02", "state_name": "ALASKA"},
    "60": {"state_code": "60", "state_name": "AMERICAN SAMOA"}
  }
}
```

## Next Step

Output feeds into [`02_get_all_counties.py`](2025-01-20_02_get_all_counties.md) for county extraction.

## Implementation Notes

- **HTML Source:** Static HTML file from FEMA portal
- **Parsing Method:** BeautifulSoup for reliable HTML parsing
- **Data Validation:** Ensures all state codes are properly formatted
- **Territory Inclusion:** Includes US territories and districts
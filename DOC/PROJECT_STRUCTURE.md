# Project Structure

```
├── notebooks/                    # Python scripts for data collection
│   ├── 01_get_all_state.py      # Extract all US states/territories
│   ├── 02_get_all_counties.py   # Extract counties for each state
│   ├── 03_get_all_communities.py # Extract communities for each county
│   ├── 04_get_flood_risk_shapefiles.py # Collect shapefile data
│   ├── 05_download_shapefiles.py # Download all shapefile ZIP files
│   ├── 06a_extract_zip_files.py # Extract ZIP files only
│   ├── 06b_convert_shapefiles_to_gpkg.py # Convert shapefiles to GPKG
│   ├── 06c_create_clean_conversion_table.py # Create clean tables for analysis
│   ├── 06d_merge_gpkg_files.py # Merge GPKG files by filename group
│   └── legacy/                  # Legacy scripts
│       ├── 06_extract_and_merge_shapefiles.py # Legacy: Extract ZIPs and merge to GPKG
│       └── 06_extract_and_merge_shapefiles.py.backup # Backup of legacy script
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
├── .log/                         # Centralized log files directory
│   ├── extraction_06a.log       # Logs from script 06a
│   ├── conversion_06b.log       # Logs from script 06b
│   ├── clean_conversion_06c.log # Logs from script 06c
│   ├── merge_gpkg_06d.log       # Logs from script 06d
│   └── processing.log           # General processing logs
├── DOC/                          # Documentation files
│   └── DOC-Automate Bulk Download and Share.md # Documentation on bulk download
├── meta_results_sample/          # Sample data for testing
├── E:\FEMA_DOWNLOAD\            # Downloaded ZIP files (from script 05)
│   ├── 01\                      # Alabama
│   │   ├── 01001\               # Autauga County
│   │   └── ...                  # All counties
│   └── ...                      # All states
├── E:\FEMA_EXTRACTED\           # Extracted shapefiles (from script 06a)
│   ├── FRD_01001C_Shapefiles\   # Product name as folder
│   └── ...                      # All products
├── E:\FEMA_SHAPEFILE_TO_GPKG\   # Converted GPKG files (from script 06b)
│   ├── FRD_01001C_Shapefiles\   # Product name as folder
│   └── ...                      # All products
├── E:\FEMA_MERGED\              # Consolidated GPKG files by filename (from script 06d)
│   ├── s_frd_proj_ar.gpkg       # Merged project area files
│   ├── s_huc_ar.gpkg            # Merged HUC area files
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
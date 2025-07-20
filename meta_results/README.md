# Meta Results Directory

This directory contains generated data files from the FEMA flood risk collection scripts.

## Important Notes

- **Files are excluded from git** due to large size (50MB+ each)
- **Sample data available** in [`meta_results_sample/`](../meta_results_sample/) folder
- **Run scripts 01-04** to generate the full dataset files
- **Essential for project operation** - contains states, counties, communities data and SQLite database

## Generated Files

- `states_data.json` - All US states/territories
- `all_counties_data.json` - Counties for each state  
- `all_communities_data.json` - Communities for each county
- `flood_risk_shapefiles.db` - SQLite database with shapefile metadata
- `*_summary.json` - Statistics and analysis reports

See the main README.md for complete usage instructions.
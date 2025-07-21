# FEMA Database Schema Tools

This directory contains SQL schema definitions and tools for working with the FEMA Flood Risk Data Collector database.

## Schema Files

- `database_schema.md` - Comprehensive documentation of the database schema and workflow
- `database_schema.puml` - PlantUML Entity-Relationship Diagram of the database
- Individual `.sql` files for each table in the database

## Schema Generation Tools

Two Python scripts are provided for generating schema files from SQLite databases:

### 1. extract_schema.py

A simple script specifically designed for the `flood_risk_shapefiles.db` database. It extracts schema information and generates SQL files for each table.

**Usage:**

```bash
python extract_schema.py
```

This script will:
1. Connect to the `meta_results/flood_risk_shapefiles.db` database
2. Extract schema information for all tables
3. Generate SQL files in the `DATA_SCHEMA` directory

### 2. generate_schema.py

A more general-purpose script that can generate schema files for any SQLite database. It provides more detailed schema information and can also generate a markdown summary.

**Usage:**

```bash
python generate_schema.py <database_path> <output_directory> [--markdown]
```

**Arguments:**
- `database_path` - Path to the SQLite database file
- `output_directory` - Directory to write schema files to
- `--markdown` - (Optional) Generate a markdown summary of the database schema

**Example:**

```bash
python generate_schema.py meta_results/flood_risk_shapefiles.db DATA_SCHEMA --markdown
```

This will:
1. Connect to the specified database
2. Extract schema information for all tables
3. Generate SQL files in the specified output directory
4. Generate a markdown summary of the database schema

## Viewing the Entity-Relationship Diagram

The `database_schema.puml` file contains a PlantUML definition of the database schema. To view it as a diagram:

1. Install PlantUML: https://plantuml.com/
2. Use a PlantUML viewer or extension:
   - VS Code: PlantUML extension
   - Online: http://www.plantuml.com/plantuml/uml/
   - Command line: `java -jar plantuml.jar database_schema.puml`

## Database Structure

The database contains tables for:

1. **Core Data**:
   - `states` - Information about states
   - `counties` - Information about counties
   - `communities` - Information about communities
   - `shapefiles` - Information about available flood risk shapefiles

2. **Processing Workflow**:
   - `request_log` - API requests for flood risk shapefiles
   - `download_log` - Downloading of shapefile zip files
   - `extraction_06a_log` - Extraction of shapefiles from zip files
   - `conversion_06b_log` - Conversion of shapefiles to GeoPackage format
   - `clean_conversion_table` - Clean view of successfully converted GeoPackage files
   - `gpkg_filename_groups` - Groups of similar GeoPackage files for merging
   - `merge_06d_log` - Merging of similar GeoPackage files
   - `shapefile_processing_log` - Overall processing of shapefiles
   - `shapefile_contributions` - Individual shapefile contributions to merged GeoPackage files

For detailed information about each table and the relationships between them, see the `database_schema.md` file.
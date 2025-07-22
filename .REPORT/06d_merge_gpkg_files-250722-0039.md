# Report

================================================================================
2025-07-22 12:39:27,710 - INFO - MERGE COMPLETED - 235 successful, 0 failed
2025-07-22 12:39:27,710 - INFO - ================================================================================
2025-07-22 12:39:27,710 - INFO -
NOTE ON LAYER NAMING AND SCHEMA HANDLING:
2025-07-22 12:39:27,710 - INFO - - Using explicit layer naming (-nln option) to prevent multiple tables in output
2025-07-22 12:39:27,710 - INFO - - All features are stored in a single layer named after the filename group
2025-07-22 12:39:27,710 - INFO - - The 'product_name' field tracks the source of each feature
2025-07-22 12:39:27,711 - INFO - - Schema differences are handled by analyzing all files before merging
2025-07-22 12:39:27,711 - INFO - - All columns from all files are included in the merged output
2025-07-22 12:39:27,711 - INFO - - Missing columns are filled with NULL values
2025-07-22 12:39:27,711 - INFO - - New fields in subsequent files are properly included in the merged output
2025-07-22 12:39:27,711 - INFO - - Field names are sanitized: lowercase and non-alphanumeric chars replaced with underscores
2025-07-22 12:39:27,711 - INFO -
Process completed at: 2025-07-22 12:39:27.711082
2025-07-22 12:39:27,711 - INFO - Summary: 235 successful, 0 failed
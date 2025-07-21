# Report

```bash
Total communities in database: 29505
Total communities in database: 29505
Communities processed this run: 14457
Communities skipped (already processed): 16247
Total shapefiles found: 10082
Successful requests: 30504
Failed requests: 185

Database saved to: meta_results/flood_risk_shapefiles.db

Top 5 states by shapefile count:
1. PENNSYLVANIA (42): 1121 shapefiles
2. KENTUCKY (21): 783 shapefiles
3. TEXAS (48): 781 shapefiles
4. ILLINOIS (17): 705 shapefiles
5. NEW JERSEY (34): 531 shapefiles

Top 5 counties by shapefile count:
1. COOK COUNTY (17031), ILLINOIS: 165 shapefiles
2. ALLEGHENY COUNTY (42003), PENNSYLVANIA: 131 shapefiles
3. CHESTER COUNTY (42029), PENNSYLVANIA: 115 shapefiles
4. BERGEN COUNTY (34003), NEW JERSEY: 111 shapefiles
5. BERKS COUNTY (42011), PENNSYLVANIA: 81 shapefiles

Top 5 communities by shapefile count:
1. DALLAS, CITY OF (480171), ROCKWALL COUNTY, TEXAS: 30 shapefiles
2. HOUSTON, CITY OF (480296), MONTGOMERY COUNTY, TEXAS: 15 shapefiles
3. CHEROKEE NATION (400605), WASHINGTON COUNTY, OKLAHOMA: 14 shapefiles
4. ANDERSON COUNTY UNINCORPORATED AREAS (210002), ANDERSON COUNTY, KENTUCKY: 12 shapefiles
5. ANDERSON COUNTY ALL JURISDICTIONS (21005C), ANDERSON COUNTY, KENTUCKY: 12 shapefiles

You can now query the database using SQL:
sqlite3 meta_results/flood_risk_shapefiles.db

Example queries:
  SELECT * FROM shapefiles LIMIT 10;
  SELECT state_name, COUNT(*) FROM shapefiles sf JOIN states s ON sf.state_code = s.state_code GROUP BY state_name;
  SELECT * FROM shapefiles WHERE product_file_size LIKE '%MB' ORDER BY CAST(REPLACE(product_file_size, 'MB', '') AS INTEGER) DESC; 
```
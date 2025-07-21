-- shapefile_processing_log definition

CREATE TABLE shapefile_processing_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            shapefile_type TEXT NOT NULL,
            geometry_type TEXT,
            source_files_count INTEGER DEFAULT 0,
            total_features_merged INTEGER DEFAULT 0,
            output_gpkg_path TEXT,
            processing_success BOOLEAN NOT NULL,
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_size_bytes INTEGER,
            coordinate_system TEXT,
            error_message TEXT,
            FOREIGN KEY (state_code) REFERENCES states (state_code)
        );

CREATE INDEX idx_processing_log_state ON shapefile_processing_log (state_code);


-- Sample data:
-- |id|state_code|shapefile_type|geometry_type|source_files_count|total_features_merged|output_gpkg_path|processing_success|processing_timestamp|file_size_bytes|coordinate_system|error_message|
-- |--|--|--|--|--|--|--|--|--|--|--|--|
-- |475|01|Shapefiles/R_UDF_Losses_by_Building|Polygon|1|205603|E:\FEMA_MERGED\01\Shapefiles/R_UDF_Losses_by_Building.gpkg|1|2025-07-20 18:42:33|176631808|EPSG:4326|NULL|
-- |476|01|Shapefiles/R_UDF_Losses_by_Parcel|Polygon|1|158072|E:\FEMA_MERGED\01\Shapefiles/R_UDF_Losses_by_Parcel.gpkg|1|2025-07-20 18:43:04|190754816|EPSG:4326|NULL|
-- |477|01|Shapefiles/R_UDF_Losses_by_Point|Point|1|10708|E:\FEMA_MERGED\01\Shapefiles/R_UDF_Losses_by_Point.gpkg|1|2025-07-20 18:43:06|6782976|EPSG:4326|NULL|

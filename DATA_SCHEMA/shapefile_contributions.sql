-- shapefile_contributions definition

CREATE TABLE shapefile_contributions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            county_code TEXT NOT NULL,
            community_code TEXT NOT NULL,
            product_name TEXT NOT NULL,
            shapefile_type TEXT NOT NULL,
            source_shapefile_path TEXT NOT NULL,
            features_count INTEGER DEFAULT 0,
            merged_into_gpkg TEXT,
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (state_code) REFERENCES states (state_code),
            FOREIGN KEY (county_code) REFERENCES counties (county_code),
            FOREIGN KEY (community_code) REFERENCES communities (community_code)
        );

CREATE INDEX idx_contributions_state ON shapefile_contributions (state_code);


-- Sample data:
-- |id|state_code|county_code|community_code|product_name|shapefile_type|source_shapefile_path|features_count|merged_into_gpkg|processing_timestamp|
-- |--|--|--|--|--|--|--|--|--|--|
-- |1590|01|01001|01001C|FRD_03150201_shapefiles_20140221|Shapefiles/R_UDF_Losses_by_Building|E:\FEMA_EXTRACTED\01\01001\FRD_03150201_shapefiles_20140221\Shapefiles/R_UDF_Losses_by_Building.shp|205603|E:\FEMA_MERGED\01\Shapefiles/R_UDF_Losses_by_Building.gpkg|2025-07-20 18:42:14|
-- |1591|01|01001|01001C|FRD_03150201_shapefiles_20140221|Shapefiles/R_UDF_Losses_by_Parcel|E:\FEMA_EXTRACTED\01\01001\FRD_03150201_shapefiles_20140221\Shapefiles/R_UDF_Losses_by_Parcel.shp|158072|E:\FEMA_MERGED\01\Shapefiles/R_UDF_Losses_by_Parcel.gpkg|2025-07-20 18:42:49|
-- |1592|01|01001|01001C|FRD_03150201_shapefiles_20140221|Shapefiles/R_UDF_Losses_by_Point|E:\FEMA_EXTRACTED\01\01001\FRD_03150201_shapefiles_20140221\Shapefiles/R_UDF_Losses_by_Point.shp|10708|E:\FEMA_MERGED\01\Shapefiles/R_UDF_Losses_by_Point.gpkg|2025-07-20 18:43:05|

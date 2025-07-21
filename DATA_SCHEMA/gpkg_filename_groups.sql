-- gpkg_filename_groups definition

CREATE TABLE gpkg_filename_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            count INTEGER NOT NULL
        );

CREATE INDEX idx_gpkg_filename_groups ON gpkg_filename_groups (filename);


-- Sample data:
-- |id|filename|count|
-- |--|--|--|
-- |256|s_frd_proj_ar|535|
-- |257|s_huc_ar|509|
-- |258|s_cslf_ar|457|

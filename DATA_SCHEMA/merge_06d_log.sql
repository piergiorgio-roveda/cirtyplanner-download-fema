-- merge_06d_log definition

CREATE TABLE merge_06d_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename_group TEXT NOT NULL,
            source_files_count INTEGER NOT NULL,
            merged_gpkg_path TEXT NOT NULL,
            merge_success BOOLEAN NOT NULL,
            merge_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        );

CREATE INDEX idx_merge_06d_filename ON merge_06d_log (filename_group);
CREATE INDEX idx_merge_06d_success ON merge_06d_log (merge_success);


-- Sample data:
-- |id|filename_group|source_files_count|merged_gpkg_path|merge_success|merge_timestamp|error_message|
-- |--|--|--|--|--|--|--|
-- |1|county|17|E:\FEMA_MERGED\county.gpkg|0|2025-07-21 10:53:23|Error merging files: [WinError 17] The system cannot move the file to a different disk drive: 'D:\\git\\cityplanner-desktop\\download-fema\\.TMP_MERGE\\county_7417c816.gpkg' -> 'E:\\FEMA_MERGED\\county.gpkg'|
-- |2|counties|5|E:\FEMA_MERGED\counties.gpkg|0|2025-07-21 10:53:25|ogr2ogr error: ERROR 1: Cannot find OGR field for Arrow array CountyFips
ERROR 1: WriteArrowBatch() failed
ERROR 1: Terminating translation prematurely after failed
translation of layer Counties (use -skipfailures to skip errors)
|
-- |3|county|17|E:\FEMA_MERGED\county.gpkg|0|2025-07-21 10:55:59|Error merging files: name 'shutil' is not defined|

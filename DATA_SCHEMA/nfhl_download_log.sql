-- nfhl_download_log definition

CREATE TABLE nfhl_download_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            product_name TEXT NOT NULL,
            product_file_path TEXT NOT NULL,
            download_success BOOLEAN NOT NULL,
            download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_path TEXT,
            file_size_bytes INTEGER,
            error_message TEXT
        );

CREATE INDEX idx_nfhl_download_log_product ON nfhl_download_log (product_name);
CREATE INDEX idx_nfhl_download_log_success ON nfhl_download_log (download_success);
-- states definition

CREATE TABLE states (
            state_code TEXT PRIMARY KEY,
            state_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

-- Sample data:
-- |state_code|state_name|created_at|
-- |--|--|--|
-- |01|ALABAMA|2025-07-20 19:23:35|
-- |02|ALASKA|2025-07-20 19:23:35|
-- |04|ARIZONA|2025-07-20 19:23:35|

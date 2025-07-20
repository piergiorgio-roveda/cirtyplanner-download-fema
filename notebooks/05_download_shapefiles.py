#!/usr/bin/env python3
"""
Script to download all flood risk shapefiles from the SQLite database.

This script:
1. Reads shapefile data from the SQLite database
2. Downloads each shapefile ZIP from FEMA portal
3. Organizes files in folder structure: {base_path}\{state}\{county}\
4. Tracks download progress and handles errors
5. Resumes interrupted downloads
6. Uses configuration file for settings
"""

import sqlite3
import requests
import os
import time
import json
from datetime import datetime
from urllib.parse import urljoin
import hashlib

def load_config(config_path='config.json'):
    """Load configuration from JSON file."""
    if not os.path.exists(config_path):
        # Create default config if it doesn't exist
        default_config = {
            "download": {
                "base_path": "E:\\FEMA_DOWNLOAD",
                "rate_limit_seconds": 0.2,
                "chunk_size_bytes": 8192,
                "timeout_seconds": 30
            },
            "database": {
                "path": "meta_results/flood_risk_shapefiles.db"
            },
            "api": {
                "base_url": "https://msc.fema.gov",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        }
        
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        print(f"Created default configuration file: {config_path}")
        return default_config
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
    except Exception as e:
        raise ValueError(f"Error reading config file {config_path}: {e}")

def connect_database(db_path):
    """Connect to the SQLite database."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    conn = sqlite3.connect(db_path)
    return conn

def get_shapefiles_to_download(conn):
    """Get all shapefiles from database that need to be downloaded."""
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT DISTINCT 
            sf.state_code,
            sf.county_code,
            sf.community_code,
            sf.product_name,
            sf.product_file_path,
            sf.product_file_size,
            s.state_name,
            c.county_name,
            cm.community_name
        FROM shapefiles sf
        JOIN states s ON sf.state_code = s.state_code
        JOIN counties c ON sf.county_code = c.county_code
        JOIN communities cm ON sf.community_code = cm.community_code
        WHERE sf.product_file_path IS NOT NULL
        ORDER BY sf.state_code, sf.county_code, sf.community_code
    ''')
    
    return cursor.fetchall()

def create_download_folder(base_path, state_code, county_code):
    """Create folder structure for downloads."""
    folder_path = os.path.join(base_path, state_code, county_code)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

def get_download_url(product_file_path):
    """Construct the full download URL from the file path."""
    base_url = "https://msc.fema.gov"
    
    # Handle different path formats
    if product_file_path.startswith('/'):
        return urljoin(base_url, product_file_path)
    else:
        return urljoin(base_url + '/', product_file_path)

def get_file_hash(filepath):
    """Calculate MD5 hash of a file for integrity checking."""
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return None

def download_file(url, filepath, expected_size=None, config=None):
    """Download a file with progress tracking and resume capability."""
    if config is None:
        config = load_config()
    
    headers = {
        'User-Agent': config['api']['user_agent']
    }
    
    # Check if file already exists and get its size
    resume_pos = 0
    if os.path.exists(filepath):
        resume_pos = os.path.getsize(filepath)
        if expected_size and resume_pos >= expected_size:
            print(f"    File already complete: {os.path.basename(filepath)}")
            return True
        headers['Range'] = f'bytes={resume_pos}-'
    
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=config['download']['timeout_seconds'])
        
        # Handle range request responses
        if response.status_code == 206:  # Partial content
            mode = 'ab'
        elif response.status_code == 200:  # Full content
            mode = 'wb'
            resume_pos = 0
        else:
            response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0)) + resume_pos
        
        with open(filepath, mode) as f:
            downloaded = resume_pos
            for chunk in response.iter_content(chunk_size=config['download']['chunk_size_bytes']):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Progress update every MB
                    if downloaded % (1024 * 1024) == 0:
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            print(f"    Progress: {downloaded // (1024*1024)}MB / {total_size // (1024*1024)}MB ({percent:.1f}%)")
        
        print(f"    ✓ Downloaded: {os.path.basename(filepath)} ({downloaded // (1024*1024)}MB)")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"    ✗ Download failed: {e}")
        return False
    except Exception as e:
        print(f"    ✗ Unexpected error: {e}")
        return False

def create_download_log_table(conn):
    """Create table to track download progress."""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS download_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            county_code TEXT NOT NULL,
            community_code TEXT NOT NULL,
            product_name TEXT NOT NULL,
            product_file_path TEXT NOT NULL,
            download_success BOOLEAN NOT NULL,
            download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_path TEXT,
            file_size_bytes INTEGER,
            error_message TEXT
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_download_log_product ON download_log (product_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_download_log_success ON download_log (download_success)')
    conn.commit()

def log_download_result(conn, state_code, county_code, community_code, product_name, 
                       product_file_path, success, file_path=None, file_size=None, error_msg=None):
    """Log download result to database."""
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO download_log 
        (state_code, county_code, community_code, product_name, product_file_path, 
         download_success, file_path, file_size_bytes, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (state_code, county_code, community_code, product_name, product_file_path,
          success, file_path, file_size, error_msg))
    conn.commit()

def get_downloaded_files(conn):
    """Get set of already successfully downloaded files."""
    cursor = conn.cursor()
    cursor.execute('SELECT product_name FROM download_log WHERE download_success = 1')
    return set(row[0] for row in cursor.fetchall())

def parse_file_size(size_str):
    """Parse file size string like '248MB' to bytes."""
    if not size_str:
        return None
    
    size_str = size_str.upper().strip()
    if size_str.endswith('MB'):
        return int(float(size_str[:-2]) * 1024 * 1024)
    elif size_str.endswith('KB'):
        return int(float(size_str[:-2]) * 1024)
    elif size_str.endswith('GB'):
        return int(float(size_str[:-2]) * 1024 * 1024 * 1024)
    else:
        try:
            return int(size_str)
        except:
            return None

def main():
    """Main function to download all shapefiles."""
    print("Starting FEMA Shapefile Download Process...")
    print("=" * 60)
    
    # Load configuration
    try:
        config = load_config()
        print(f"Configuration loaded successfully")
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return
    
    # Configuration
    db_path = config['database']['path']
    download_base_path = config['download']['base_path']
    
    print(f"Database path: {db_path}")
    print(f"Download path: {download_base_path}")
    print("=" * 60)
    
    # Connect to database
    try:
        conn = connect_database(db_path)
        create_download_log_table(conn)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run 04_get_flood_risk_shapefiles.py first to create the database.")
        return
    
    # Get shapefiles to download
    shapefiles = get_shapefiles_to_download(conn)
    total_files = len(shapefiles)
    
    if total_files == 0:
        print("No shapefiles found in database.")
        return
    
    print(f"Found {total_files} shapefiles to download")
    print(f"Download location: {download_base_path}")
    print("=" * 60)
    
    # Get already downloaded files
    downloaded_files = get_downloaded_files(conn)
    
    # Create base download directory
    os.makedirs(download_base_path, exist_ok=True)
    
    # Download statistics
    downloaded_count = 0
    failed_count = 0
    skipped_count = 0
    total_size_downloaded = 0
    
    # Process each shapefile
    for i, shapefile in enumerate(shapefiles, 1):
        (state_code, county_code, community_code, product_name, 
         product_file_path, product_file_size, state_name, county_name, community_name) = shapefile
        
        print(f"\n[{i}/{total_files}] Processing: {product_name}")
        print(f"  State: {state_name} ({state_code})")
        print(f"  County: {county_name} ({county_code})")
        print(f"  Community: {community_name} ({community_code})")
        print(f"  Size: {product_file_size}")
        
        # Skip if already downloaded
        if product_name in downloaded_files:
            print(f"  ⏭ Already downloaded, skipping...")
            skipped_count += 1
            continue
        
        # Create download folder
        download_folder = create_download_folder(download_base_path, state_code, county_code)
        
        # Determine filename
        filename = f"{product_name}.zip"
        filepath = os.path.join(download_folder, filename)
        
        # Get download URL
        download_url = get_download_url(product_file_path)
        print(f"  URL: {download_url}")
        
        # Parse expected file size
        expected_size = parse_file_size(product_file_size)
        
        # Download file
        success = download_file(download_url, filepath, expected_size, config)
        
        if success:
            # Get actual file size
            actual_size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
            total_size_downloaded += actual_size
            downloaded_count += 1
            
            # Log success
            log_download_result(conn, state_code, county_code, community_code,
                              product_name, product_file_path, True, filepath, actual_size)
        else:
            failed_count += 1
            # Log failure
            log_download_result(conn, state_code, county_code, community_code,
                              product_name, product_file_path, False, error_msg="Download failed")
        
        # Progress summary
        if i % 10 == 0 or i == total_files:
            print(f"\n  Progress Summary: {i}/{total_files} processed")
            print(f"    Downloaded: {downloaded_count}")
            print(f"    Failed: {failed_count}")
            print(f"    Skipped: {skipped_count}")
            print(f"    Total size: {total_size_downloaded // (1024*1024)}MB")
        
        # Rate limiting
        time.sleep(config['download']['rate_limit_seconds'])
    
    # Final summary
    print("\n" + "=" * 60)
    print("DOWNLOAD PROCESS COMPLETE")
    print("=" * 60)
    print(f"Total files processed: {total_files}")
    print(f"Successfully downloaded: {downloaded_count}")
    print(f"Failed downloads: {failed_count}")
    print(f"Already existed (skipped): {skipped_count}")
    print(f"Total data downloaded: {total_size_downloaded // (1024*1024)}MB")
    print(f"Download location: {download_base_path}")
    
    # Show folder structure
    print(f"\nFolder structure created:")
    print(f"  {download_base_path}\\")
    print(f"    ├── 01\\ (Alabama)")
    print(f"    │   ├── 01001\\ (Autauga County)")
    print(f"    │   └── 01003\\ (Baldwin County)")
    print(f"    ├── 02\\ (Alaska)")
    print(f"    └── ... (other states)")
    
    # Close database connection
    conn.close()
    
    print(f"\nDownload log saved in database table: download_log")
    print(f"You can query download status with:")
    print(f"  SELECT * FROM download_log WHERE download_success = 0; -- Failed downloads")
    print(f"  SELECT state_code, COUNT(*) FROM download_log GROUP BY state_code; -- By state")

if __name__ == "__main__":
    main()
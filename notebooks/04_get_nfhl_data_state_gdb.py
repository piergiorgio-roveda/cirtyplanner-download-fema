#!/usr/bin/env python3
"""
Script to fetch NFHL State GDB data for each state.

This script:
1. Loads state data from sample files
2. Creates SQLite database with proper schema
3. Makes POST requests to FEMA portal for each state
4. Extracts EFFECTIVE items where product_SUBTYPE_ID = "NFHL_STATE_DATA"
5. Stores the relevant GDB information in SQLite database
6. Automatically resumes from where it left off if interrupted

Resume Capability:
- Checks nfhl_request_log table for already processed states
- Skips states that have been successfully processed
- Can be safely restarted after network interruptions
- Shows progress: processed this run vs. skipped (already done)
"""

import json
import requests
import time
import sqlite3
from datetime import datetime
import os
from urllib.parse import urlencode

def create_database(db_path):
    """Create SQLite database with proper schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create nfhl_states table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS nfhl_states (
            state_code TEXT PRIMARY KEY,
            state_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create gdb_nfhl table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS gdb_nfhl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            product_id INTEGER,
            product_type_id TEXT,
            product_subtype_id TEXT,
            product_name TEXT,
            product_description TEXT,
            product_effective_date INTEGER,
            product_issue_date INTEGER,
            product_effective_date_string TEXT,
            product_posting_date INTEGER,
            product_posting_date_string TEXT,
            product_issue_date_string TEXT,
            product_effective_flag TEXT,
            product_file_path TEXT,
            product_file_size TEXT,
            fetch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (state_code) REFERENCES nfhl_states (state_code)
        )
    ''')
    
    # Create nfhl_request_log table for tracking API calls
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS nfhl_request_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            success BOOLEAN NOT NULL,
            error_message TEXT,
            gdb_found INTEGER DEFAULT 0,
            request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (state_code) REFERENCES nfhl_states (state_code)
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_gdb_nfhl_state ON gdb_nfhl (state_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_gdb_nfhl_product_name ON gdb_nfhl (product_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_nfhl_request_log_timestamp ON nfhl_request_log (request_timestamp)')
    
    conn.commit()
    return conn

def load_data():
    """Load state, county, and community data from meta_results."""
    print("Loading data from meta_results...")
    
    # Load counties data
    with open('meta_results/all_counties_data.json', 'r') as f:
        counties_data = json.load(f)
    
    # Load communities data
    with open('meta_results/all_communities_data.json', 'r') as f:
        communities_data = json.load(f)
    
    return counties_data, communities_data

def populate_base_data(conn, communities_data):
    """Populate nfhl_states table."""
    cursor = conn.cursor()
    
    print("Populating base data...")
    
    # Insert states
    for state_code, state_info in communities_data['states'].items():
        cursor.execute('''
            INSERT OR REPLACE INTO nfhl_states (state_code, state_name)
            VALUES (?, ?)
        ''', (state_code, state_info['state_name']))
    
    conn.commit()
    print("Base data populated successfully.")

def create_form_data(state_code, county_code=None, community_code=None):
    """Create form data for POST request."""
    # If county_code and community_code are not provided, use default values
    if county_code is None:
        county_code = f"{state_code}001"  # Use first county in state
    
    if community_code is None:
        community_code = f"{county_code}C"  # Use default community code format
    
    return {
        'utf8': '✓',
        'affiliate': 'fema',
        'query': '',
        'selstate': state_code,
        'selcounty': county_code,
        'selcommunity': community_code,
        'jurisdictionkey': '',
        'jurisdictionvalue': '',
        'searchedCid': community_code,
        'searchedDateStart': '',
        'searchedDateEnd': '',
        'txtstartdate': '',
        'txtenddate': '',
        'method': 'search'
    }

def fetch_nfhl_state_data(conn, state_code, state_name, counties_data=None):
    """Fetch NFHL state GDB data for a specific state."""
    url = 'https://msc.fema.gov/portal/advanceSearch'
    cursor = conn.cursor()
    
    # Check if we already have a record for this state
    cursor.execute('''
        SELECT product_file_path FROM gdb_nfhl WHERE state_code = ?
    ''', (state_code,))
    existing_file_path = cursor.fetchone()
    
    if existing_file_path:
        print(f"  Skipping state {state_name} ({state_code}) - already have GDB data")
        return {
            'success': True,
            'gdb_found': 0,
            'skipped': True
        }
    
    # Try to find a valid county and community code for this state
    county_code = None
    community_code = None
    
    if counties_data and state_code in counties_data:
        # Get the first county in the state
        county_codes = list(counties_data[state_code].keys())
        if county_codes:
            county_code = county_codes[0]
            # Default community code format
            community_code = f"{county_code}C"
    
    # If we couldn't find county data, use default format
    if county_code is None:
        county_code = f"{state_code}001"
        community_code = f"{county_code}C"
    
    form_data = create_form_data(state_code, county_code, community_code)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    try:
        response = requests.post(url, data=form_data, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Check if response is HTML instead of JSON (error page)
        content_type = response.headers.get('Content-Type', '')
        if 'text/html' in content_type or response.text.strip().startswith(('<!DOCTYPE', '<html')):
            error_msg = f"Received HTML response instead of JSON for state {state_name} ({state_code})"
            print(error_msg)
            
            # Log failed request
            cursor.execute('''
                INSERT INTO nfhl_request_log (state_code, success, error_message, gdb_found)
                VALUES (?, ?, ?, ?)
            ''', (state_code, False, error_msg, 0))
            
            conn.commit()
            
            return {
                'success': False,
                'error': error_msg,
                'gdb_found': 0,
                'skipped': False
            }
        
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            # Save the response content for debugging
            debug_file = f"debug_response_{state_code}.txt"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            error_msg = f"JSON decode error: {str(e)}. Response saved to {debug_file}"
            print(error_msg)
            
            # Log failed request
            cursor.execute('''
                INSERT INTO nfhl_request_log (state_code, success, error_message, gdb_found)
                VALUES (?, ?, ?, ?)
            ''', (state_code, False, error_msg, 0))
            
            conn.commit()
            
            return {
                'success': False,
                'error': error_msg,
                'gdb_found': 0,
                'skipped': False
            }
        
        # Extract EFFECTIVE items with NFHL_STATE_DATA
        gdb_found = 0
        if 'EFFECTIVE' in data and 'NFHL_STATE_DATA' in data['EFFECTIVE']:
            for item in data['EFFECTIVE']['NFHL_STATE_DATA']:
                if item.get('product_SUBTYPE_ID') == 'NFHL_STATE_DATA':
                    # Insert GDB data
                    cursor.execute('''
                        INSERT INTO gdb_nfhl (
                            state_code,
                            product_id, product_type_id, product_subtype_id,
                            product_name, product_description,
                            product_effective_date, product_issue_date,
                            product_effective_date_string, product_posting_date,
                            product_posting_date_string, product_issue_date_string,
                            product_effective_flag, product_file_path, product_file_size
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        state_code,
                        item.get('product_ID'), item.get('product_TYPE_ID'), item.get('product_SUBTYPE_ID'),
                        item.get('product_NAME'), item.get('product_DESCRIPTION'),
                        item.get('product_EFFECTIVE_DATE'), item.get('product_ISSUE_DATE'),
                        item.get('product_EFFECTIVE_DATE_STRING'), item.get('product_POSTING_DATE'),
                        item.get('product_POSTING_DATE_STRING'), item.get('product_ISSUE_DATE_STRING'),
                        item.get('product_EFFECTIVE_FLAG'), item.get('product_FILE_PATH'), item.get('product_FILE_SIZE')
                    ))
                    gdb_found += 1
        
        # Log successful request
        cursor.execute('''
            INSERT INTO nfhl_request_log (state_code, success, gdb_found)
            VALUES (?, ?, ?)
        ''', (state_code, True, gdb_found))
        
        conn.commit()
        
        return {
            'success': True,
            'gdb_found': gdb_found,
            'skipped': False
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        print(f"Error fetching data for {state_name} ({state_code}): {error_msg}")
        
        # Log failed request
        cursor.execute('''
            INSERT INTO nfhl_request_log (state_code, success, error_message, gdb_found)
            VALUES (?, ?, ?, ?)
        ''', (state_code, False, error_msg, 0))
        
        conn.commit()
        
        return {
            'success': False,
            'error': error_msg,
            'gdb_found': 0,
            'skipped': False
        }
    # JSON error handling has been moved inside the try block

def get_processed_states(conn):
    """Get set of already processed states."""
    cursor = conn.cursor()
    cursor.execute('SELECT state_code FROM nfhl_request_log WHERE success = 1')
    return set(row[0] for row in cursor.fetchall())

def get_statistics(conn):
    """Get statistics from the database."""
    cursor = conn.cursor()
    
    # Total counts
    cursor.execute('SELECT COUNT(*) FROM nfhl_states')
    total_states = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM gdb_nfhl')
    total_gdb = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM nfhl_request_log WHERE success = 1')
    successful_requests = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM nfhl_request_log WHERE success = 0')
    failed_requests = cursor.fetchone()[0]
    
    # Top states by GDB count
    cursor.execute('''
        SELECT s.state_name, s.state_code, COUNT(g.id) as gdb_count
        FROM nfhl_states s
        LEFT JOIN gdb_nfhl g ON s.state_code = g.state_code
        GROUP BY s.state_code, s.state_name
        ORDER BY gdb_count DESC
        LIMIT 10
    ''')
    top_states = cursor.fetchall()
    
    return {
        'total_states': total_states,
        'total_gdb': total_gdb,
        'successful_requests': successful_requests,
        'failed_requests': failed_requests,
        'top_states': top_states
    }

def main():
    """Main function to process all states."""
    print("Starting NFHL State GDB data collection...")
    print("=" * 60)
    
    # Create output directory
    os.makedirs('meta_results', exist_ok=True)
    
    # Create database
    db_path = 'meta_results/flood_risk_nfhl_gdb.db'
    
    # Use config file if it exists
    if os.path.exists('config.json'):
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                if 'database' in config and 'nfhl_path' in config['database']:
                    db_path = config['database']['nfhl_path']
                    print(f"Using database path from config: {db_path}")
        except Exception as e:
            print(f"Error reading config file: {e}")
            print(f"Using default database path: {db_path}")
    
    conn = create_database(db_path)
    
    # Load data
    counties_data, communities_data = load_data()
    
    # Calculate totals for progress tracking
    total_states = len(communities_data['states'])
    
    print(f"Dataset Overview:")
    print(f"  Total States: {total_states}")
    print("=" * 60)
    
    # Populate base data
    populate_base_data(conn, communities_data)
    
    # Get already processed states for resume capability
    processed_states = get_processed_states(conn)
    print(f"Found {len(processed_states)} already processed states")
    
    total_processed = 0
    total_gdb_found = 0
    skipped_count = 0
    
    # Process each state
    for state_code, state_info in communities_data['states'].items():
        state_name = state_info['state_name']
        
        # Skip if already processed
        if state_code in processed_states:
            print(f"Skipping already processed state: {state_name} ({state_code})")
            skipped_count += 1
            continue
        
        print(f"\nProcessing state: {state_name} ({state_code})")
        
        # Fetch NFHL state GDB data
        result = fetch_nfhl_state_data(conn, state_code, state_name, counties_data)
        
        # Update counters
        if not result.get('skipped', False):
            total_processed += 1
            total_gdb_found += result['gdb_found']
        else:
            skipped_count += 1
        
        # Rate limiting - wait between requests
        time.sleep(0.5)
    
    # Get final statistics
    stats = get_statistics(conn)
    
    # Generate summary
    print("\n" + "=" * 60)
    print("NFHL STATE GDB DATA COLLECTION COMPLETE")
    print("=" * 60)
    print(f"Total states processed: {stats['total_states']}")
    print(f"States processed this run: {total_processed}")
    print(f"States skipped (already processed): {skipped_count}")
    print(f"Total GDB files found: {stats['total_gdb']}")
    print(f"Successful requests: {stats['successful_requests']}")
    print(f"Failed requests: {stats['failed_requests']}")
    
    print(f"\nDatabase saved to: {db_path}")
    
    # Display top results
    if stats['top_states']:
        print(f"\nTop 5 states by GDB count:")
        for i, (state_name, state_code, count) in enumerate(stats['top_states'][:5], 1):
            print(f"{i}. {state_name} ({state_code}): {count} GDB files")
    
    # Close database connection
    conn.close()
    
    print(f"\nYou can now query the database using SQL:")
    print(f"sqlite3 {db_path}")
    print(f"\nExample queries:")
    print(f"  SELECT * FROM gdb_nfhl LIMIT 10;")
    print(f"  SELECT state_name, COUNT(*) FROM gdb_nfhl g JOIN nfhl_states s ON g.state_code = s.state_code GROUP BY state_name;")
    print(f"  SELECT * FROM gdb_nfhl WHERE product_file_size LIKE '%MB' ORDER BY CAST(REPLACE(product_file_size, 'MB', '') AS INTEGER) DESC;")

if __name__ == "__main__":
    main()
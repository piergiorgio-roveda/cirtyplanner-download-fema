#!/usr/bin/env python
"""
Generate SQL schema files from any SQLite database.

This script connects to a SQLite database, extracts schema information for all tables,
and generates SQL files with CREATE TABLE statements, indices, and sample data.

Usage:
    python generate_schema.py <database_path> <output_directory>

Example:
    python generate_schema.py meta_results/my_database.db DATA_SCHEMA
"""

import sqlite3
import os
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any


def get_table_schema(db_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Extract schema information from SQLite database.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Dictionary with table names as keys and schema information as values
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} does not exist.")
        return {}
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = cursor.fetchall()
        
        schemas = {}
        for table in tables:
            table_name = table[0]
            
            # Skip SQLite internal tables
            if table_name.startswith('sqlite_'):
                continue
            
            # Get CREATE TABLE statement
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            create_stmt = cursor.fetchone()[0]
            
            # Get indices for this table
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='{table_name}' AND sql IS NOT NULL;")
            indices = cursor.fetchall()
            index_stmts = [idx[0] for idx in indices if idx[0] is not None]
            
            # Get sample data (first few rows)
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3;")
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                sample_data = []
                if rows:
                    sample_data.append("-- Sample data:")
                    sample_data.append(f"-- |{'|'.join(columns)}|")
                    sample_data.append(f"-- |{'--|' * len(columns)}")
                    for row in rows:
                        formatted_row = [str(cell) if cell is not None else "NULL" for cell in row]
                        sample_data.append(f"-- |{'|'.join(formatted_row)}|")
            except sqlite3.Error as e:
                sample_data = [f"-- Error getting sample data: {str(e)}"]
            
            schemas[table_name] = {
                'create_stmt': create_stmt,
                'indices': index_stmts,
                'sample_data': sample_data
            }
        
        return schemas
    
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return {}
    
    finally:
        if conn:
            conn.close()


def write_schema_files(schemas: Dict[str, Dict[str, Any]], output_dir: str) -> None:
    """
    Write schema information to SQL files.
    
    Args:
        schemas: Dictionary with table names as keys and schema information as values
        output_dir: Directory to write SQL files to
    """
    os.makedirs(output_dir, exist_ok=True)
    
    for table_name, schema_info in schemas.items():
        file_path = os.path.join(output_dir, f"{table_name}.sql")
        
        with open(file_path, 'w') as f:
            f.write(f"-- {table_name} definition\n\n")
            f.write(f"{schema_info['create_stmt']};\n\n")
            
            for index_stmt in schema_info['indices']:
                f.write(f"{index_stmt};\n")
            
            if schema_info['indices']:
                f.write("\n\n")
            
            for data_line in schema_info['sample_data']:
                f.write(f"{data_line}\n")
        
        print(f"Created schema file: {file_path}")


def generate_markdown_summary(schemas: Dict[str, Dict[str, Any]], db_path: str, output_dir: str) -> None:
    """
    Generate a markdown summary of the database schema.
    
    Args:
        schemas: Dictionary with table names as keys and schema information as values
        db_path: Path to the SQLite database file
        output_dir: Directory where schema files are written
    """
    db_name = os.path.basename(db_path)
    file_path = os.path.join(output_dir, "database_schema.md")
    
    with open(file_path, 'w') as f:
        f.write(f"# Database Schema: {db_name}\n\n")
        f.write("## Tables\n\n")
        
        for table_name in sorted(schemas.keys()):
            f.write(f"### {table_name}\n\n")
            f.write(f"[SQL Definition](./{table_name}.sql)\n\n")
            
            # Extract column information from CREATE TABLE statement
            create_stmt = schemas[table_name]['create_stmt']
            # Simple parsing to extract column definitions
            try:
                columns_part = create_stmt.split('(', 1)[1].rsplit(')', 1)[0].strip()
                # Handle multi-line definitions and remove trailing commas
                columns = []
                current_column = ""
                paren_count = 0
                
                for char in columns_part:
                    if char == '(' and not current_column.endswith("'") and not current_column.endswith('"'):
                        paren_count += 1
                    elif char == ')' and not current_column.endswith("'") and not current_column.endswith('"'):
                        paren_count -= 1
                    
                    current_column += char
                    
                    if char == ',' and paren_count == 0:
                        columns.append(current_column[:-1].strip())
                        current_column = ""
                
                if current_column.strip():
                    columns.append(current_column.strip())
                
                # Filter out constraints that aren't column definitions
                columns = [col for col in columns if not col.upper().startswith(('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK', 'CONSTRAINT'))]
                
                f.write("#### Columns\n\n")
                f.write("| Column | Type | Constraints |\n")
                f.write("|--------|------|-------------|\n")
                
                for col in columns:
                    parts = col.split(' ', 1)
                    column_name = parts[0].strip()
                    if len(parts) > 1:
                        rest = parts[1].strip()
                        # Try to separate type from constraints
                        type_parts = rest.split(' ', 1)
                        col_type = type_parts[0].strip()
                        constraints = type_parts[1].strip() if len(type_parts) > 1 else ""
                        f.write(f"| {column_name} | {col_type} | {constraints} |\n")
                    else:
                        f.write(f"| {column_name} | | |\n")
                
                f.write("\n")
            except Exception as e:
                f.write(f"Error parsing column information: {str(e)}\n\n")
            
            # List indices
            if schemas[table_name]['indices']:
                f.write("#### Indices\n\n")
                for idx in schemas[table_name]['indices']:
                    f.write(f"```sql\n{idx};\n```\n\n")
        
        f.write("## Relationships\n\n")
        f.write("Foreign key relationships between tables:\n\n")
        
        for table_name, schema_info in schemas.items():
            create_stmt = schema_info['create_stmt']
            # Extract foreign key constraints
            if "FOREIGN KEY" in create_stmt.upper():
                f.write(f"### {table_name} relationships\n\n")
                
                # Simple parsing to extract foreign key definitions
                try:
                    parts = create_stmt.split('FOREIGN KEY')
                    for i in range(1, len(parts)):
                        fk_def = parts[i].strip()
                        if fk_def.startswith('('):
                            # Extract the constraint
                            paren_count = 1
                            j = 1
                            while j < len(fk_def) and paren_count > 0:
                                if fk_def[j] == '(':
                                    paren_count += 1
                                elif fk_def[j] == ')':
                                    paren_count -= 1
                                j += 1
                            
                            local_col = fk_def[1:j-1].strip()
                            
                            # Extract the reference
                            ref_part = fk_def[j:].strip()
                            if ref_part.upper().startswith('REFERENCES'):
                                ref_part = ref_part[10:].strip()  # Remove "REFERENCES"
                                ref_table = ref_part.split('(')[0].strip()
                                ref_col = ref_part.split('(')[1].split(')')[0].strip()
                                
                                f.write(f"- `{local_col}` → `{ref_table}({ref_col})`\n")
                except Exception as e:
                    f.write(f"Error parsing foreign key relationships: {str(e)}\n\n")
                
                f.write("\n")
        
        print(f"Created markdown summary: {file_path}")


def main():
    parser = argparse.ArgumentParser(description='Generate SQL schema files from SQLite database')
    parser.add_argument('db_path', help='Path to the SQLite database file')
    parser.add_argument('output_dir', help='Directory to write schema files to')
    parser.add_argument('--markdown', action='store_true', help='Generate markdown summary')
    
    args = parser.parse_args()
    
    print(f"Extracting schema from {args.db_path}...")
    schemas = get_table_schema(args.db_path)
    
    if schemas:
        print(f"Found {len(schemas)} tables.")
        write_schema_files(schemas, args.output_dir)
        
        if args.markdown:
            generate_markdown_summary(schemas, args.db_path, args.output_dir)
        
        print("Schema extraction complete.")
    else:
        print("No tables found or error occurred.")


if __name__ == "__main__":
    main()
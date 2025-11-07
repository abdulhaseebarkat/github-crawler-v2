#!/usr/bin/env python3
"""Script to dump database contents to CSV and JSON."""

import logging
import sys
import os
import csv
import json
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def get_db_connection():
    """Get database connection."""
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "github_crawler")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    return conn


def dump_to_csv(output_file: str):
    """Dump database to CSV."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, owner, full_name, stars, url, 
                       created_at, updated_at, crawled_at
                FROM repositories
                ORDER BY stars DESC
            """)
            
            rows = cur.fetchall()
            
            if not rows:
                logger.warning("No data to dump")
                return
            
            # Write CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            
            logger.info(f"Dumped {len(rows)} repositories to {output_file}")
    finally:
        conn.close()


def dump_to_json(output_file: str):
    """Dump database to JSON."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, owner, full_name, stars, url, 
                       created_at, updated_at, crawled_at
                FROM repositories
                ORDER BY stars DESC
            """)
            
            rows = cur.fetchall()
            
            if not rows:
                logger.warning("No data to dump")
                return
            
            # Convert rows to list of dicts with serializable dates
            data = []
            for row in rows:
                row_dict = dict(row)
                # Convert datetime objects to ISO format strings
                for key, value in row_dict.items():
                    if isinstance(value, datetime):
                        row_dict[key] = value.isoformat()
                data.append(row_dict)
            
            # Write JSON
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Dumped {len(data)} repositories to {output_file}")
    finally:
        conn.close()


def main():
    """Dump database to CSV and JSON."""
    try:
        output_dir = os.getenv("OUTPUT_DIR", "artifacts")
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = os.path.join(output_dir, f"repositories_{timestamp}.csv")
        json_file = os.path.join(output_dir, f"repositories_{timestamp}.json")
        
        dump_to_csv(csv_file)
        dump_to_json(json_file)
        
        logger.info(f"Database dump completed. Files: {csv_file}, {json_file}")
        return 0
    except Exception as e:
        logger.error(f"Database dump failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())


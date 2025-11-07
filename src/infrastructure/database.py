"""Database connection and repository storage implementation."""

import logging
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from typing import List, Optional
import os

from src.domain.repository import Repository

logger = logging.getLogger(__name__)


class DatabaseRepository:
    """Repository for storing GitHub repository data in PostgreSQL."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database repository.
        
        Args:
            connection_string: PostgreSQL connection string. If None, uses env vars.
        """
        if connection_string is None:
            # Build connection string from environment variables
            db_host = os.getenv("POSTGRES_HOST", "localhost")
            db_port = os.getenv("POSTGRES_PORT", "5432")
            db_name = os.getenv("POSTGRES_DB", "github_crawler")
            db_user = os.getenv("POSTGRES_USER", "postgres")
            db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
            
            connection_string = (
                f"host={db_host} port={db_port} dbname={db_name} "
                f"user={db_user} password={db_password}"
            )
        
        self.connection_string = connection_string
        self.pool: Optional[ThreadedConnectionPool] = None
    
    def connect(self):
        """Initialize connection pool."""
        try:
            self.pool = ThreadedConnectionPool(1, 5, self.connection_string)
            logger.info("Database connection pool created")
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")
            raise
    
    def close(self):
        """Close connection pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")
    
    def _get_connection(self):
        """Get a connection from the pool."""
        if not self.pool:
            self.connect()
        return self.pool.getconn()
    
    def _return_connection(self, conn):
        """Return a connection to the pool."""
        if self.pool:
            self.pool.putconn(conn)
    
    def initialize_schema(self):
        """Create database tables if they don't exist."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # Create repositories table with flexible schema
                # Using ON CONFLICT for efficient upserts
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS repositories (
                        id VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        owner VARCHAR(255) NOT NULL,
                        full_name VARCHAR(512) NOT NULL UNIQUE,
                        stars INTEGER NOT NULL,
                        url TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL,
                        crawled_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT unique_full_name UNIQUE (full_name)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_repositories_stars ON repositories(stars);
                    CREATE INDEX IF NOT EXISTS idx_repositories_full_name ON repositories(full_name);
                    CREATE INDEX IF NOT EXISTS idx_repositories_owner ON repositories(owner);
                    CREATE INDEX IF NOT EXISTS idx_repositories_crawled_at ON repositories(crawled_at);
                """)
                conn.commit()
                logger.info("Database schema initialized")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error initializing schema: {e}")
            raise
        finally:
            self._return_connection(conn)
    
    def upsert_repositories(self, repositories: List[Repository]):
        """
        Insert or update repositories in the database.
        
        Uses PostgreSQL's ON CONFLICT for efficient upserts (only updates changed rows).
        
        Args:
            repositories: List of repository entities to store
        """
        if not repositories:
            return
        
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # Prepare data for bulk insert
                values = [
                    (
                        repo.id,
                        repo.name,
                        repo.owner,
                        repo.full_name,
                        repo.stars,
                        repo.url,
                        repo.created_at,
                        repo.updated_at,
                    )
                    for repo in repositories
                ]
                
                # Use ON CONFLICT to update only if stars or updated_at changed
                # The WHERE clause ensures minimal rows are affected - only rows with
                # actual changes will be updated
                execute_values(
                    cur,
                    """
                    INSERT INTO repositories (
                        id, name, owner, full_name, stars, url, created_at, updated_at
                    ) VALUES %s
                    ON CONFLICT (id) 
                    DO UPDATE SET
                        stars = EXCLUDED.stars,
                        updated_at = EXCLUDED.updated_at,
                        crawled_at = CURRENT_TIMESTAMP
                    WHERE repositories.stars != EXCLUDED.stars 
                       OR repositories.updated_at != EXCLUDED.updated_at
                    """,
                    values,
                    template=None,
                    page_size=1000
                )
                
                conn.commit()
                logger.info(f"Upserted {len(repositories)} repositories")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error upserting repositories: {e}")
            raise
        finally:
            self._return_connection(conn)
    
    def get_repository_count(self) -> int:
        """Get the total number of repositories in the database."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM repositories")
                count = cur.fetchone()[0]
                return count
        except Exception as e:
            logger.error(f"Error getting repository count: {e}")
            raise
        finally:
            self._return_connection(conn)


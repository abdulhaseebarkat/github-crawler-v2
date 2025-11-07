#!/usr/bin/env python3
"""Script to initialize PostgreSQL database schema."""

import logging
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.infrastructure.database import DatabaseRepository

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """Initialize database schema."""
    try:
        db_repo = DatabaseRepository()
        db_repo.connect()
        db_repo.initialize_schema()
        db_repo.close()
        logger.info("Database schema setup completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Failed to setup database schema: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())


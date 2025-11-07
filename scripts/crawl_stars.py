#!/usr/bin/env python3
"""Script to crawl GitHub repositories and store star counts."""

import logging
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.infrastructure.github_client import GitHubGraphQLClient
from src.infrastructure.database import DatabaseRepository
from src.application.crawler_service import CrawlerService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """Crawl GitHub repositories and store in database."""
    try:
        # Get GitHub token from environment (GitHub Actions provides GITHUB_TOKEN)
        github_token = os.getenv("GITHUB_TOKEN")
        if not github_token:
            logger.warning("GITHUB_TOKEN not found. Using unauthenticated requests (limited rate).")
        
        # Initialize clients
        github_client = GitHubGraphQLClient(token=github_token)
        db_repository = DatabaseRepository()
        db_repository.connect()
        
        # Ensure schema is initialized
        db_repository.initialize_schema()
        
        # Create crawler service
        crawler = CrawlerService(github_client, db_repository)
        
        # Crawl 100,000 repositories
        target_count = int(os.getenv("TARGET_COUNT", "100000"))
        crawled_count = crawler.crawl_repositories(target_count=target_count)
        
        # Log final count
        final_count = db_repository.get_repository_count()
        logger.info(f"Crawl completed. Total repositories in database: {final_count}")
        
        db_repository.close()
        return 0 if crawled_count > 0 else 1
        
    except Exception as e:
        logger.error(f"Crawl failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())


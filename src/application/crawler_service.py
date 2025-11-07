"""Application service for crawling GitHub repositories."""

import logging
import time
from typing import Optional

from src.infrastructure.github_client import GitHubGraphQLClient
from src.infrastructure.database import DatabaseRepository
from src.domain.repository import Repository

logger = logging.getLogger(__name__)


class CrawlerService:
    """Service for crawling GitHub repositories and storing them in the database."""
    
    BATCH_SIZE = 100  # Maximum repos per GraphQL query
    BATCH_COMMIT_SIZE = 1000  # Commit to DB every N repos
    
    # Multiple search queries to get around the 1,000 result limit per query
    # GitHub search is limited to 1,000 results per query, so we use different
    # search criteria to get more repositories
    SEARCH_QUERIES = [

    "stars:>0",
    "stars:>100",
    "stars:>1000",
    "stars:>5000",
    "stars:>10000",
    
    "stars:1..10",
    "stars:10..50",
    "stars:50..100",
    "stars:100..500",
    "stars:500..1000",
    "stars:1000..5000",
    
    "language:python stars:>0",
    "language:javascript stars:>0",
    "language:java stars:>0",
    "language:go stars:>0",
    "language:rust stars:>0",
    "language:typescript stars:>0",
    "language:cpp stars:>0",
    "language:c stars:>0",
    "language:csharp stars:>0",
    "language:php stars:>0",
    "language:ruby stars:>0",
    "language:swift stars:>0",
    "language:kotlin stars:>0",
    "language:scala stars:>0",
    "language:r stars:>0",
    "language:shell stars:>0",
    "language:dart stars:>0",
    "language:lua stars:>0",
    "language:perl stars:>0",
    "language:haskell stars:>0",
    "language:elixir stars:>0",
    "language:clojure stars:>0",
    "language:objective-c stars:>0",
    "language:vim-script stars:>0",
    "language:powershell stars:>0",
    
    "language:python stars:10..100",
    "language:python stars:100..1000",
    "language:javascript stars:10..100",
    "language:javascript stars:100..1000",
    "language:java stars:10..100",
    "language:typescript stars:10..100",
    "language:go stars:10..100",
    "language:rust stars:10..100",
    "language:cpp stars:10..100",
    "language:php stars:10..100",
    "language:ruby stars:10..100",
    "language:swift stars:10..100",
    "language:kotlin stars:10..100",
    
    "created:>2024-01-01",
    "created:2023-01-01..2024-01-01",
    "created:2022-01-01..2023-01-01",
    "created:2021-01-01..2022-01-01",
    "created:2020-01-01..2021-01-01",
    "pushed:>2024-06-01",
    "pushed:2024-01-01..2024-06-01",
    "pushed:2023-06-01..2024-01-01",
    "pushed:2023-01-01..2023-06-01",
    
    "topic:machine-learning stars:>0",
    "topic:deep-learning stars:>0",
    "topic:artificial-intelligence stars:>0",
    "topic:web stars:>0",
    "topic:webapp stars:>0",
    "topic:api stars:>0",
    "topic:mobile stars:>0",
    "topic:android stars:>0",
    "topic:ios stars:>0",
    "topic:frontend stars:>0",
    "topic:backend stars:>0",
    "topic:fullstack stars:>0",
    "topic:react stars:>0",
    "topic:vue stars:>0",
    "topic:angular stars:>0",
    "topic:nodejs stars:>0",
    "topic:docker stars:>0",
    "topic:kubernetes stars:>0",
    "topic:devops stars:>0",
    "topic:cloud stars:>0",
    "topic:aws stars:>0",
    "topic:game stars:>0",
    "topic:gaming stars:>0",
    "topic:bot stars:>0",
    "topic:cli stars:>0",
    "topic:tool stars:>0",
    "topic:framework stars:>0",
    "topic:library stars:>0",
    "topic:blockchain stars:>0",
    "topic:cryptocurrency stars:>0",
    "topic:security stars:>0",
    "topic:automation stars:>0",
    "topic:data-science stars:>0",
    "topic:data-analysis stars:>0",
    "topic:visualization stars:>0",
    
    "language:python topic:machine-learning",
    "language:python topic:data-science",
    "language:javascript topic:react",
    "language:javascript topic:nodejs",
    "language:typescript topic:react",
    "language:go topic:api",
    "language:rust topic:cli",
    "language:java topic:android",
    "language:swift topic:ios",
    "language:kotlin topic:android",
    
    "forks:>100 stars:>0",
    "forks:>500 stars:>0",
    "forks:>1000 stars:>0",
    "size:>1000 stars:>0",
    "size:>10000 stars:>0",
    
    "license:mit stars:>0",
    "license:apache-2.0 stars:>0",
    "license:gpl-3.0 stars:>0",
    "license:bsd-3-clause stars:>0",
    
    "archived:false stars:>10",
    "mirror:false stars:>10",
    "archived:false language:python",
    "archived:false language:javascript",
    
    "language:python created:>2023-01-01",
    "language:javascript created:>2023-01-01",
    "language:typescript created:>2023-01-01",
    "language:go created:>2023-01-01",
    "language:rust created:>2023-01-01",
    
    "good-first-issues:>0 stars:>0",
    "help-wanted-issues:>0 stars:>0",
    
    "react in:readme stars:>0",
    "vue in:readme stars:>0",
    "django in:readme stars:>0",
    "flask in:readme stars:>0",
    "express in:readme stars:>0",
    "spring in:readme stars:>0",
    "tensorflow in:readme stars:>0",
    "pytorch in:readme stars:>0",
    "fastapi in:readme stars:>0",
    "nextjs in:readme stars:>0",
]
    
    
    def __init__(
        self,
        github_client: GitHubGraphQLClient,
        database_repository: DatabaseRepository
    ):
        """
        Initialize crawler service.
        
        Args:
            github_client: GitHub API client
            database_repository: Database repository for storing data
        """
        self.github_client = github_client
        self.database_repository = database_repository
    
    def crawl_repositories(self, target_count: int = 100000) -> int:
        """
        Crawl GitHub repositories and store them in the database.
        
        Uses multiple search queries to get around the 1,000 result limit per query.
        
        Args:
            target_count: Target number of repositories to crawl
            
        Returns:
            Number of repositories successfully crawled
        """
        logger.info(f"Starting crawl for {target_count} repositories")
        
        total_crawled = 0
        batch_buffer: list[Repository] = []
        seen_repo_ids = set()  # Track seen repos to avoid duplicates
        
        # Try each search query until we reach target_count
        for search_query in self.SEARCH_QUERIES:
            if total_crawled >= target_count:
                break
                
            logger.info(f"Using search query: {search_query}")
            cursor = None
            query_results = 0
            max_results_per_query = 1000  # GitHub search limit
            
            while total_crawled < target_count and query_results < max_results_per_query:
                try:
                    # Calculate how many to fetch in this batch
                    remaining = min(target_count - total_crawled, max_results_per_query - query_results)
                    batch_size = min(self.BATCH_SIZE, remaining)
                    
                    # Fetch repositories from GitHub
                    repos, next_cursor, api_remaining = self.github_client.get_repositories(
                        limit=batch_size,
                        cursor=cursor,
                        search_query=search_query
                    )
                    
                    if not repos:
                        logger.warning(f"No repositories returned from API for query: {search_query}")
                        break
                    
                    # Filter out duplicates
                    new_repos = [repo for repo in repos if repo.id not in seen_repo_ids]
                    for repo in new_repos:
                        seen_repo_ids.add(repo.id)
                    
                    batch_buffer.extend(new_repos)
                    total_crawled += len(new_repos)
                    query_results += len(repos)  # Count all repos (including duplicates) for query limit
                    
                    logger.info(
                        f"Crawled {total_crawled}/{target_count} repositories "
                        f"({len(new_repos)} new, {len(repos) - len(new_repos)} duplicates). "
                        f"API calls remaining: {api_remaining}"
                    )
                    
                    # Commit to database in batches for efficiency
                    if len(batch_buffer) >= self.BATCH_COMMIT_SIZE:
                        self.database_repository.upsert_repositories(batch_buffer)
                        batch_buffer = []
                    
                    # Check rate limit
                    if api_remaining <= 100:
                        logger.warning(f"Low API rate limit: {api_remaining}. Pausing...")
                        time.sleep(60)  # Wait 1 minute
                    
                    # Move to next page
                    if next_cursor:
                        cursor = next_cursor
                    else:
                        logger.info(f"Reached end of results for query: {search_query}")
                        break
                        
                except Exception as e:
                    logger.error(f"Error during crawl with query '{search_query}': {e}")
                    # Continue with next query
                    break
        
        # Commit remaining repositories
        if batch_buffer:
            self.database_repository.upsert_repositories(batch_buffer)
        
        logger.info(f"Crawl completed. Total unique repositories crawled: {total_crawled}")
        return total_crawled


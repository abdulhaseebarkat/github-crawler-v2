"""GitHub GraphQL API client with rate limiting and retry logic."""

import time
import logging
import os
from typing import List, Optional, Dict, Any
import requests

from src.domain.repository import Repository
from datetime import datetime

logger = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Raised when GitHub API rate limit is exceeded."""
    pass


class GitHubGraphQLClient:
    """Client for GitHub GraphQL API with rate limiting and retry mechanisms."""
    
    # GitHub API rate limits: 5,000 points per hour for authenticated requests
    # Each repository query costs 1 point, so we can query up to 5,000 repos per hour
    # For unauthenticated: 60 requests per hour (we'll use authenticated via GITHUB_TOKEN)
    
    GRAPHQL_ENDPOINT = "https://api.github.com/graphql"
    MAX_RETRIES = 5
    RETRY_DELAY_SECONDS = 1
    RATE_LIMIT_BUFFER = 100  # Reserve some API calls for safety
    
    def __init__(self, token: Optional[str] = None):
        """
        Initialize GitHub GraphQL client.
        
        Args:
            token: GitHub personal access token. If None, uses GITHUB_TOKEN env var.
        """
        if token is None:
            token = os.getenv("GITHUB_TOKEN")
        
        self.token = token
        self.headers = {
            "Content-Type": "application/json",
        }
        
        # Add authorization header if token is available
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"
        
    def _execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a GraphQL query with retry logic.
        
        Args:
            query: GraphQL query string
            variables: Query variables
            
        Returns:
            GraphQL response data
            
        Raises:
            RateLimitExceeded: If rate limit is exceeded
            requests.RequestException: If request fails after retries
        """
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
            
        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.post(
                    self.GRAPHQL_ENDPOINT,
                    json=payload,
                    headers=self.headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check for GraphQL errors
                    if "errors" in data:
                        error_messages = [err.get("message", "") for err in data["errors"]]
                        
                        # Check for rate limit errors
                        if any("rate limit" in msg.lower() for msg in error_messages):
                            # Check rate limit info from response headers
                            remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
                            reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                            
                            if remaining <= self.RATE_LIMIT_BUFFER:
                                wait_time = max(reset_time - int(time.time()), 0) + 10
                                logger.warning(f"Rate limit approaching. Waiting {wait_time} seconds...")
                                time.sleep(wait_time)
                                continue
                            else:
                                raise RateLimitExceeded(f"Rate limit exceeded: {error_messages}")
                        
                        # Other GraphQL errors
                        raise Exception(f"GraphQL errors: {error_messages}")
                    
                    return data.get("data", {})
                
                elif response.status_code == 401:
                    raise Exception("Authentication failed. Check your GitHub token.")
                elif response.status_code == 403:
                    # Rate limit or forbidden
                    remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
                    reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                    
                    if remaining == 0:
                        wait_time = max(reset_time - int(time.time()), 0) + 10
                        logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds...")
                        if attempt < self.MAX_RETRIES - 1:
                            time.sleep(wait_time)
                            continue
                        raise RateLimitExceeded("Rate limit exceeded")
                    else:
                        raise Exception(f"Forbidden: {response.text}")
                
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    delay = self.RETRY_DELAY_SECONDS * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    raise
        
        raise Exception("Max retries exceeded")
    
    def get_repositories(self, limit: int = 100, cursor: Optional[str] = None, search_query: str = "stars:>0") -> tuple[List[Repository], Optional[str], int]:
        """
        Fetch repositories from GitHub using GraphQL.
        
        Args:
            limit: Maximum number of repositories to fetch (max 100 per query)
            cursor: Pagination cursor
            search_query: GitHub search query string (e.g., "stars:>0", "language:python")
            
        Returns:
            Tuple of (list of repositories, next cursor, remaining API calls)
        """
        query = """
        query($limit: Int!, $cursor: String, $searchQuery: String!) {
            search(query: $searchQuery, type: REPOSITORY, first: $limit, after: $cursor) {
                repositoryCount
                pageInfo {
                    hasNextPage
                    endCursor
                }
                nodes {
                    ... on Repository {
                        id
                        name
                        nameWithOwner
                        stargazerCount
                        url
                        createdAt
                        updatedAt
                    }
                }
            }
            rateLimit {
                remaining
                resetAt
            }
        }
        """
        
        variables = {
            "limit": min(limit, 100),
            "cursor": cursor,
            "searchQuery": search_query
        }
        data = self._execute_query(query, variables)
        
        search_result = data.get("search", {})
        nodes = search_result.get("nodes", [])
        page_info = search_result.get("pageInfo", {})
        rate_limit = data.get("rateLimit", {})
        
        repositories = []
        for node in nodes:
            # Parse owner from nameWithOwner
            owner, name = node["nameWithOwner"].split("/", 1)
            
            # Parse datetime strings
            created_at = datetime.fromisoformat(node["createdAt"].replace("Z", "+00:00"))
            updated_at = datetime.fromisoformat(node["updatedAt"].replace("Z", "+00:00"))
            
            repo = Repository(
                id=node["id"],
                name=name,
                owner=owner,
                full_name=node["nameWithOwner"],
                stars=node["stargazerCount"],
                url=node["url"],
                created_at=created_at,
                updated_at=updated_at
            )
            repositories.append(repo)
        
        next_cursor = page_info.get("endCursor") if page_info.get("hasNextPage") else None
        remaining = rate_limit.get("remaining", 0)
        
        return repositories, next_cursor, remaining


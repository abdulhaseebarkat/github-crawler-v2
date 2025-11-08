## GITHUB CRAWLER

## Features

- GraphQL API Integration**: Uses GitHub's GraphQL API for efficient data fetching
- Rate Limit Handling**: Implements retry logic and respects GitHub API rate limits
- Efficient Database Operations**: Uses PostgreSQL upserts with minimal row updates
- Clean Architecture**: Separation of concerns with domain, application, and infrastructure layers
- GitHub Actions Pipeline**: Automated daily crawling with database dumps

## Project Structure

github-crawler/
	 src/
 		domain/           # Domain entities (Repository)
		application/      # Application services (CrawlerService)
		infrastructure/   # External integrations (GitHub API, Database)
	scripts/
		setup_postgres.py    # Initialize database schema
		crawl_stars.py       # Main crawler script
		dump_database.py     # Export data to CSV/JSON
	.github/
		workflows/
			crawl.yml        # GitHub Actions pipeline
	requirements.txt


## Setup

## GitHub Actions Pipeline

How to Run:

1. Open Repository
2. Ensure `.github/workflows/crawl.yml` is in your repository.
3. Actions --> GitHub Repository Crawler --> Run Workflow Dropdown --> Click Run workflow

Monitor Progress

1. Click on the running workflow
2. Expand the "Crawl GitHub repositories" step to see logs
3. Wait for completion (see performance expectations below)

The pipeline automatically:
1. Sets up a PostgreSQL service container
2. Installs Python dependencies
3. Initializes the database schema
4. Crawls 100,000 GitHub repositories
5. Dumps the database contents to CSV and JSON
6. Uploads the artifacts

Download Artifacts

1. After the workflow completes, go to the workflow run
2. Scroll down to "Artifacts"
3. Click "repository-data" to download
4. Extract the ZIP file to get CSV and JSON files

The pipeline uses the default `GITHUB_TOKEN` provided by GitHub Actions and doesn't require elevated permissions.

## Database Schema

```sql
CREATE TABLE repositories (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    full_name VARCHAR(512) NOT NULL UNIQUE,
    stars INTEGER NOT NULL,
    url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    crawled_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_repositories_stars ON repositories(stars);
CREATE INDEX idx_repositories_full_name ON repositories(full_name);
CREATE INDEX idx_repositories_owner ON repositories(owner);
CREATE INDEX idx_repositories_crawled_at ON repositories(crawled_at);
```

The schema is designed for:
- **Efficient updates**: Uses `ON CONFLICT` for upserts that only update changed rows
- **Fast queries**: Indexes on commonly queried fields (stars, full_name, owner)
- **Future extensibility**: Can be extended with additional tables for issues, PRs, etc.


## Scaling to 500 Million Repositories

For scaling to 500 million repositories:

1. Distributed Crawling**: Use multiple workers/containers to parallelize crawling
2. Partitioning**: Partition the database table by date or repository ID ranges
3. Message Queue**: Use a message queue (e.g., RabbitMQ, Kafka) to distribute work
4. Incremental Updates**: Track last crawl time and only update changed repositories
5. Caching**: Cache API responses to reduce redundant API calls
6. Database Optimization**: Use read replicas, connection pooling, and query optimization
7. Monitoring**: Implement comprehensive monitoring for rate limits and errors

#

Schema Evolution for Additional Metadata

To efficiently gather and update additional metadata (issues, PRs, commits, comments, reviews, CI checks) while minimizing database updates, consider the following schema design:

**Normalized Schema with Separate Tables**

```sql
-- Core repository table (existing)
CREATE TABLE repositories (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    full_name VARCHAR(512) NOT NULL UNIQUE,
    stars INTEGER NOT NULL,
    url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    crawled_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Pull Requests table
CREATE TABLE pull_requests

-- Issues table
CREATE TABLE issues 

-- Comments table (for both PRs and Issues)
CREATE TABLE comments

-- Commits table (commits within PRs)
CREATE TABLE commits

-- Reviews table (reviews on PRs)
CREATE TABLE reviews

-- CI Checks table
CREATE TABLE ci_checks







from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import psycopg2.pool
import os
from typing import Optional
from db.adapters import get_adapter
from db.utils import deduplicate_posts, sort_and_paginate

app = FastAPI(title="Reddit API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "reddit-postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "reddit"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "secret!1234"),
}

TRINO_CONFIG = {
    "host": os.getenv("TRINO_HOST", "trino"),
    "port": int(os.getenv("TRINO_PORT", "8080")),
    "catalog": os.getenv("TRINO_CATALOG", "delta"),
    "schema": os.getenv("TRINO_SCHEMA", "reddit"),
}

pool: psycopg2.pool.SimpleConnectionPool = None

SUBREDDITS = ["news", "technology", "worldnews", "programmerhumor"]

SORT_COLUMNS = {
    "created_utc": "created_utc",
    "score": "score",
    "num_comments": "num_comments",
}


@app.on_event("startup")
def startup():
    global pool
    pool = psycopg2.pool.SimpleConnectionPool(1, 10, **DB_CONFIG)


@app.on_event("shutdown")
def shutdown():
    if pool:
        pool.closeall()


def get_conn():
    return pool.getconn()


def release_conn(conn):
    pool.putconn(conn)


@app.get("/api/sources")
def list_sources():
    """List available data sources."""
    return {
        "sources": [
            {
                "id": "postgres",
                "name": "Postgres (Clean)",
                "description": "Cleaned and processed data from Postgres database"
            },
            {
                "id": "trino",
                "name": "Trino (Raw)",
                "description": "Raw data from Delta Lake tables with deduplication"
            }
        ]
    }


@app.get("/api/subreddits")
def list_subreddits():
    return {"subreddits": SUBREDDITS}


@app.get("/api/posts")
def get_posts(
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    subreddit: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    sort: str = Query(default="created_utc"),
    order: str = Query(default="desc"),
    source: str = Query(default="postgres"),
):
    # Validate source
    if source not in ["postgres", "trino"]:
        raise HTTPException(status_code=400, detail=f"Invalid source: {source}. Must be 'postgres' or 'trino'")
    
    try:
        # Get appropriate adapter
        adapter = get_adapter(
            source=source,
            postgres_pool=pool if source == "postgres" else None,
            trino_config=TRINO_CONFIG if source == "trino" else None,
            subreddits=SUBREDDITS,
            sort_columns=SORT_COLUMNS,
        )
        
        # Fetch posts from adapter
        posts, raw_total = adapter.get_posts(
            limit=limit if source == "postgres" else 10000,  # Trino: fetch more for deduplication
            offset=offset if source == "postgres" else 0,    # Trino: paginate after dedup
            subreddit=subreddit,
            search=search,
            sort=sort,
            order=order,
        )
        
        # For Trino source, apply deduplication and then sort/paginate
        duplicate_count = 0
        if source == "trino":
            posts, duplicate_count = deduplicate_posts(posts, keep_strategy="highest_score")
            total = len(posts)
            posts = sort_and_paginate(posts, sort, order, limit, offset)
        else:
            total = raw_total
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "source": source,
            "duplicate_count": duplicate_count if source == "trino" else None,
            "posts": posts,
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching posts: {str(e)}")


@app.get("/health")
def health():
    """Health check endpoint that tests both Postgres and Trino connections."""
    health_status = {
        "status": "ok",
        "postgres": {"status": "unknown"},
        "trino": {"status": "unknown"}
    }
    
    # Check Postgres
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        release_conn(conn)
        health_status["postgres"] = {"status": "healthy"}
    except Exception as e:
        health_status["postgres"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"
    
    # Check Trino
    try:
        from trino.dbapi import connect as trino_connect
        conn = trino_connect(
            host=TRINO_CONFIG["host"],
            port=TRINO_CONFIG["port"],
            catalog=TRINO_CONFIG["catalog"],
            schema=TRINO_CONFIG["schema"],
            user="trino",
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        health_status["trino"] = {"status": "healthy"}
    except Exception as e:
        health_status["trino"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"
    
    return health_status

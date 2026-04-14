from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import psycopg2.pool
import os
from typing import Optional

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
):
    sort_col = SORT_COLUMNS.get(sort, "created_utc")
    order_dir = "DESC" if order.lower() == "desc" else "ASC"

    # Build UNION ALL across all four tables
    union_parts = []
    for sub in SUBREDDITS:
        union_parts.append(
            f"SELECT post_id, title, author, score, num_comments, created_utc, url, selftext, '{sub}' AS subreddit "
            f"FROM reddit_schema.{sub}"
        )
    union_sql = " UNION ALL ".join(union_parts)

    filters = []
    params = []

    if subreddit and subreddit in SUBREDDITS:
        filters.append("subreddit = %s")
        params.append(subreddit)

    if search:
        filters.append("(title ILIKE %s OR author ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    count_sql = f"SELECT COUNT(*) FROM ({union_sql}) AS all_posts {where_clause}"
    data_sql = (
        f"SELECT * FROM ({union_sql}) AS all_posts "
        f"{where_clause} "
        f"ORDER BY {sort_col} {order_dir} "
        f"LIMIT %s OFFSET %s"
    )

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(count_sql, params)
            total = cur.fetchone()["count"]

            cur.execute(data_sql, params + [limit, offset])
            rows = cur.fetchall()
    finally:
        release_conn(conn)

    posts = []
    for row in rows:
        post = dict(row)
        if post.get("created_utc"):
            post["created_utc"] = post["created_utc"].isoformat()
        posts.append(post)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "posts": posts,
    }


@app.get("/health")
def health():
    return {"status": "ok"}

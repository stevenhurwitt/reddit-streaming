from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import psycopg2.extras
from trino.dbapi import connect as trino_connect


class DataAdapter(ABC):
    @abstractmethod
    def get_posts(
        self,
        limit: int,
        offset: int,
        subreddit: Optional[str],
        search: Optional[str],
        sort: str,
        order: str,
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        Fetch posts from data source.
        Returns: (posts, total_count)
        """
        pass


class PostgresAdapter(DataAdapter):
    def __init__(self, pool, subreddits: List[str], sort_columns: Dict[str, str]):
        self.pool = pool
        self.subreddits = subreddits
        self.sort_columns = sort_columns

    def get_posts(
        self,
        limit: int,
        offset: int,
        subreddit: Optional[str],
        search: Optional[str],
        sort: str,
        order: str,
    ) -> tuple[List[Dict[str, Any]], int]:
        sort_col = self.sort_columns.get(sort, "created_utc")
        order_dir = "DESC" if order.lower() == "desc" else "ASC"

        # Build UNION ALL across all four tables
        union_parts = []
        for sub in self.subreddits:
            union_parts.append(
                f"SELECT post_id, title, author, score, num_comments, created_utc, url, selftext, '{sub}' AS subreddit "
                f"FROM reddit_schema.{sub}"
            )
        union_sql = " UNION ALL ".join(union_parts)

        filters = []
        params = []

        if subreddit and subreddit in self.subreddits:
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

        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(count_sql, params)
                total = cur.fetchone()["count"]

                cur.execute(data_sql, params + [limit, offset])
                rows = cur.fetchall()
        finally:
            self.pool.putconn(conn)

        posts = []
        for row in rows:
            post = dict(row)
            if post.get("created_utc"):
                post["created_utc"] = post["created_utc"].isoformat()
            posts.append(post)

        return posts, total


class TrinoAdapter(DataAdapter):
    def __init__(self, trino_config: Dict[str, Any], subreddits: List[str]):
        self.trino_config = trino_config
        self.subreddits = subreddits
        # Map subreddit names to raw table names
        self.table_map = {
            "technology": "technology_raw",
            "programmerhumor": "programmerhumor_raw",
            "news": "news_raw",
            "worldnews": "worldnews_raw",
        }

    def _get_connection(self):
        return trino_connect(
            host=self.trino_config["host"],
            port=self.trino_config["port"],
            catalog=self.trino_config["catalog"],
            schema=self.trino_config["schema"],
            user="trino",
        )

    def get_posts(
        self,
        limit: int,
        offset: int,
        subreddit: Optional[str],
        search: Optional[str],
        sort: str,
        order: str,
    ) -> tuple[List[Dict[str, Any]], int]:
        # Build UNION ALL query across raw tables
        # Note: raw tables use 'id' not 'post_id', and have more columns
        union_parts = []
        for sub in self.subreddits:
            table_name = self.table_map.get(sub, f"{sub}_raw")
            union_parts.append(
                f"SELECT id AS post_id, title, author, score, num_comments, created_utc, url, selftext, '{sub}' AS subreddit "
                f"FROM {table_name}"
            )
        union_sql = " UNION ALL ".join(union_parts)

        # Build WHERE clause
        filters = []
        if subreddit and subreddit in self.subreddits:
            filters.append(f"subreddit = '{subreddit}'")
        
        if search:
            # Trino uses LIKE, not ILIKE (case-insensitive search via LOWER)
            search_escaped = search.replace("'", "''")
            filters.append(
                f"(LOWER(title) LIKE LOWER('%{search_escaped}%') OR LOWER(author) LIKE LOWER('%{search_escaped}%'))"
            )

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        # Fetch ALL matching results (we'll deduplicate and paginate in Python)
        # To avoid fetching too much, we'll apply a reasonable upper limit
        query = f"SELECT * FROM ({union_sql}) AS all_posts {where_clause} LIMIT 10000"

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
        finally:
            conn.close()

        # Convert to list of dicts
        posts = []
        for row in rows:
            post = dict(zip(columns, row))
            # Normalize timestamp to ISO format
            if post.get("created_utc"):
                # Trino created_utc is usually a bigint (Unix timestamp)
                from datetime import datetime
                try:
                    if isinstance(post["created_utc"], (int, float)):
                        post["created_utc"] = datetime.fromtimestamp(post["created_utc"]).isoformat()
                    elif hasattr(post["created_utc"], "isoformat"):
                        post["created_utc"] = post["created_utc"].isoformat()
                except:
                    pass  # Keep original value if conversion fails
            posts.append(post)

        return posts, len(posts)


def get_adapter(source: str, postgres_pool=None, trino_config=None, subreddits=None, sort_columns=None) -> DataAdapter:
    """Factory function to get the appropriate adapter based on source."""
    if source == "postgres":
        if not postgres_pool:
            raise ValueError("Postgres pool required for postgres source")
        return PostgresAdapter(postgres_pool, subreddits, sort_columns)
    elif source == "trino":
        if not trino_config:
            raise ValueError("Trino config required for trino source")
        return TrinoAdapter(trino_config, subreddits)
    else:
        raise ValueError(f"Unknown source: {source}")

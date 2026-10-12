from typing import List, Dict, Any, Tuple


def deduplicate_posts(posts: List[Dict[str, Any]], keep_strategy: str = "highest_score") -> Tuple[List[Dict[str, Any]], int]:
    """
    Deduplicate posts by post_id.
    
    Args:
        posts: List of post dictionaries
        keep_strategy: Strategy for choosing which duplicate to keep
                      - "highest_score": Keep the post with highest score
                      - "most_recent": Keep the most recent post
    
    Returns:
        Tuple of (deduplicated_posts, duplicate_count)
    """
    if not posts:
        return [], 0
    
    original_count = len(posts)
    post_dict = {}
    
    for post in posts:
        post_id = post.get("post_id")
        if not post_id:
            continue
            
        if post_id not in post_dict:
            post_dict[post_id] = post
        else:
            # Decide which post to keep based on strategy
            existing = post_dict[post_id]
            
            if keep_strategy == "highest_score":
                existing_score = existing.get("score", 0) or 0
                new_score = post.get("score", 0) or 0
                if new_score > existing_score:
                    post_dict[post_id] = post
            
            elif keep_strategy == "most_recent":
                existing_time = existing.get("created_utc", "")
                new_time = post.get("created_utc", "")
                if new_time > existing_time:
                    post_dict[post_id] = post
    
    deduplicated = list(post_dict.values())
    duplicate_count = original_count - len(deduplicated)
    
    return deduplicated, duplicate_count


def sort_and_paginate(
    posts: List[Dict[str, Any]], 
    sort: str, 
    order: str, 
    limit: int, 
    offset: int
) -> List[Dict[str, Any]]:
    """
    Sort and paginate a list of posts.
    
    Args:
        posts: List of post dictionaries
        sort: Column name to sort by
        order: "asc" or "desc"
        limit: Number of posts to return
        offset: Number of posts to skip
    
    Returns:
        Paginated and sorted list of posts
    """
    # Define default sort value for missing fields
    def get_sort_key(post):
        value = post.get(sort)
        if value is None:
            # Return appropriate default based on sort column type
            if sort in ["score", "num_comments"]:
                return 0
            elif sort == "created_utc":
                return ""
            return ""
        return value
    
    # Sort
    reverse = order.lower() == "desc"
    sorted_posts = sorted(posts, key=get_sort_key, reverse=reverse)
    
    # Paginate
    return sorted_posts[offset:offset + limit]

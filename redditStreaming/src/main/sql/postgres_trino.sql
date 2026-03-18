SELECT 'news' as subreddit, * FROM reddit_schema.news
UNION ALL
-- SELECT 'ProgrammerHumor' as subreddit, * FROM reddit_schema.programmerhumor
-- UNION ALL
SELECT 'technology' as subreddit, * FROM reddit_schema.technology
UNION ALL
SELECT 'worldnews' as subreddit, * FROM reddit_schema.worldnews
ORDER BY created_utc DESC
LIMIT 100;



SELECT 'news' as subreddit, COUNT(*) FROM reddit_schema.news
UNION ALL
SELECT 'ProgrammerHumor' as subreddit, COUNT(*) FROM reddit_schema.programmerhumor
UNION ALL
SELECT 'technology' as subreddit, COUNT(*) FROM reddit_schema.technology
UNION ALL
SELECT 'worldnews' as subreddit, COUNT(*) FROM reddit_schema.worldnews


-- trino
SELECT subreddit, title, created_utc FROM reddit.news_raw
UNION ALL
SELECT subreddit, title, created_utc FROM reddit.programmerhumor_raw
UNION ALL
SELECT subreddit, title, created_utc FROM reddit.technology_raw
UNION ALL
SELECT subreddit, title, created_utc FROM reddit.worldnews_raw
ORDER BY created_utc DESC
LIMIT 100;

SELECT title, author, score, created_utc 
FROM news_clean 
ORDER BY score DESC 
LIMIT 10;

SELECT 'technology' as subreddit, COUNT(*) as posts FROM technology_clean
UNION ALL
SELECT 'programmerhumor', COUNT(*) FROM programmerhumor_clean
UNION ALL
SELECT 'news', COUNT(*) FROM news_clean
UNION ALL
SELECT 'worldnews', COUNT(*) FROM worldnews_clean;

SELECT 'technology' as subreddit, COUNT(*) as posts FROM technology_raw
UNION ALL
SELECT 'programmerhumor', COUNT(*) FROM programmerhumor_raw
UNION ALL
SELECT 'news', COUNT(*) FROM news_raw
UNION ALL
SELECT 'worldnews', COUNT(*) FROM worldnews_raw;
SELECT * FROM reddit_schema.technology
UNION ALL
SELECT * FROM reddit_schema.news
UNION ALL
SELECT * FROM reddit_schema.worldnews
UNION ALL
SELECT * FROM reddit_schema.programmer_humor
ORDER BY created_utc DESC;
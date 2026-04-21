select count(*) as post_count, date 
from reddit.technology 
group by date
order by date desc
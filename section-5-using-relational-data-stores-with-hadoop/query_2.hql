select r.movieid, n.title, count(r.movieid) as ratingcount, avg(r.rating) as ratingavg
from ratings r
join names n on r.movieid = n.movieid
group by r.movieid, n.title
having ratingcount > 10
order by ratingavg desc
limit 1;

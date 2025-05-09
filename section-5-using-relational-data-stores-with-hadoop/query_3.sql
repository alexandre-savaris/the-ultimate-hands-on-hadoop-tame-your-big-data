select r.movie_id, m.title, count(r.movie_id) as ratingcount, avg(r.rating) as ratingavg
from ratings r
inner join movies m on r.movie_id = m.id
group by r.movie_id, m.title
having ratingcount > 10
order by ratingavg desc
limit 1;

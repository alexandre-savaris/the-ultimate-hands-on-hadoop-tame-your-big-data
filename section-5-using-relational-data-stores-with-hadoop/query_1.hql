create or replace view topmovieids as
select movieid, count(movieid) as ratingcount
from ratings
group by movieid
order by ratingcount desc;

select n.title, t.ratingcount
from topmovieids t
join names n on t.movieid = n.movieid;

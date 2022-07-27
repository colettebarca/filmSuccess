 
drop table movie_success_weights
 
## Create the final table that has the success weights of each category by title
create table movie_success_weights as    
select imdb_title_id,
	max(case when category = 'actor' then avg_success_weight end) as actor_weight,
    max(case when category = 'director' then avg_success_weight end) as director_weight,
    max(case when category = 'writer' then avg_success_weight end) as writer_weight,
    max(case when category = 'composer' then avg_success_weight end) as composer_weight,
    max(case when category = 'producer' then avg_success_weight end) as producer_weight
from (
select ams.imdb_title_id,
	ams.category,
	avg(ams.success_weight) as avg_success_weight
from artist_movie_success_weight ams
group by 1,2  
   ) a 
group by 1

# Join to get the weights for each movie in movie_clean
select *
from movies_clean mc join movie_success_weights msw on (mc.imdb_title_id = msw.imdb_title_id)
where mc.original_title = 'Toy Story'

select *
from artist_movie_success_weight 
where imdb_title_id = 'tt0114709'




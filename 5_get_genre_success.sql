drop table genre_success_rate;

create table genre_success_rate as
select year,
	genre,
    sum(success_flag) as num_success,
    count(*) as num_movies,
    sum(success_flag) / count(*) as genre_success_rate
from movies_clean
group by year,genre;

# Check the Correct Number of Movies is Recorded
select sum(num_movies) from genre_success_rate;

# Windowing Function: Average within a Genre Across Years
select *,
 avg(genre_success_rate) over (partition by genre order by genre,year ROWS BETWEEN unbounded preceding and 1 preceding) as prior_success_rate
 from genre_success_rate
 where genre = "Drama, Sport"; # Just a Test!
 
# Add a Column for Genre Weights in the Movie Success Weights Table
Alter table movie_success_weights
ADD genre_weight float;
 
# Add Values to movie_success_weights
update movie_success_weights msw
	join (select mc.imdb_title_id, gsr.*,
		avg(gsr.genre_success_rate) over (partition by gsr.genre order by gsr.genre,gsr.year ROWS BETWEEN unbounded preceding and 1 preceding) as prior_success_rate
			from genre_success_rate gsr join movies_clean mc on (gsr.genre = mc.genre and gsr.year = mc.year)) a
		on msw.imdb_title_id = a.imdb_title_id
	set genre_weight = ifnull(a.prior_success_rate,0)
    where genre_weight is null;
    
select *
from movies_clean mc join movie_success_weights w on (mc.imdb_title_id = w.imdb_title_id)
where mc.genre = 'Animation, Adventure, Comedy'

drop table production_co_success_rate

create table production_co_success_rate as
select year,
	production_company,
    sum(success_flag) as num_success,
    count(*) as num_movies,
    sum(success_flag) / count(*) as prod_co_success_rate
from movies_clean
group by year,production_company;

select *
from production_co_success_rate

# Check the Correct Number of Movies is Recorded
select sum(num_movies) from production_co_success_rate;

# Windowing Function: Average within a Genre Across Years
select *,
 avg(prod_co_success_rate) over (partition by production_company order by production_company,year ROWS BETWEEN unbounded preceding and 1 preceding) as prior_success_rate
 from production_co_success_rate
 where production_company like "Pixar%"; # Just a Test!

# Add a Column for Production Company Weights in the Movie Success Weights Table
Alter table movie_success_weights
add production_co_weight float;

# Add Values to movie_success_weights
update movie_success_weights msw
	join (select mc.imdb_title_id, pcsr.*,
		avg(pcsr.prod_co_success_rate) over (partition by pcsr.production_company order by pcsr.production_company,pcsr.year ROWS BETWEEN unbounded preceding and 1 preceding) as prior_success_rate
			from production_co_success_rate pcsr join movies_clean mc on (pcsr.production_company = mc.production_company and pcsr.year = mc.year)) a
		on msw.imdb_title_id = a.imdb_title_id
	set production_co_weight = ifnull(a.prior_success_rate,0)
    where production_co_weight is null;
    
# Join to get the weights for each movie in movie_clean
select mc.success_flag, count(*)
from movies_clean mc join movie_success_weights msw on (mc.imdb_title_id = msw.imdb_title_id)
group by 1


select *
from movies_clean mc join movie_success_weights msw on (mc.imdb_title_id = msw.imdb_title_id)

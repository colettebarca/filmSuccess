drop table artist_movie_success_rate

# Create table of successes by movie and artist
# This is just an intermediary table to be used for other calculations later
create table artist_movie_success_rate as
select 	imdb_name_id,
	imdb_title_id,
    category,
    name,
    year,
    date_published,
    success_flag,
    movie_num,
    num_success,
    success_rate,
    avg(success_rate) over (partition by imdb_name_id order by imdb_name_id, date_published ROWS BETWEEN unbounded preceding and 1 preceding) as prior_success_rate
from (
select imdb_name_id,
	imdb_title_id,
    category,
    name,
    year,
    date_published,
    success_flag,
    movie_num,
    num_success,
    (num_success / movie_num) * 100 as success_rate
from (
select tp.imdb_name_id,
	tp.imdb_title_id,
    tp.category,
    a.name,
    mc.year,
    mc.date_published,
    mc.success_flag,
    row_number() OVER(PARTITION BY imdb_name_id order by mc.date_published) as movie_num,
    sum(mc.success_flag) over (partition by imdb_name_id order by mc.date_published) as num_success
from title_principals tp join artists a on (tp.imdb_name_id = a.imdb_name_id and tp.category = a.category)
						join movies_clean mc on (tp.imdb_title_id = mc.imdb_title_id)
      ) a
) b

drop table pct_success_year

# Create avg success rates and std of success rates for all actors, directors, etc per year
# Also just a temporary table to be used later
create table pct_success_year as
select a.year,
	a.category,
    a.avg_success,
    a.std_success,
    avg(b.avg_success) as avg_pct_success,
    avg(b.std_success) as avg_std_success
from (
select tp.year,
	tp.category,
	avg(tp.success_rate) as avg_success,
    std(tp.success_rate) as std_success
from artist_movie_success_rate tp 
group by 1,2
) a,
(
select tp.year,
	tp.category,
	avg(tp.success_rate) as avg_success,
    std(tp.success_rate) as std_success
from artist_movie_success_rate tp 
group by 1,2
) b
where a.year >= b.year and
	a.category = b.category and
    a.category not in ('self','archive_footage')
group by 1,2,3,4
order by 2,1


drop table artist_movie_success_weight

# Use the table above to create a weight for each category of artist
create table artist_movie_success_weight as
select ams.imdb_name_id,
	ams.imdb_title_id,
    ams.category,
    ams.name,
    ams.year,
    ams.date_published,
    ams.success_flag,
    ams.movie_num,
    ams.num_success,
    ams.prior_success_rate,
    psy.avg_pct_success,
    psy.avg_std_success,
    ifnull((ams.prior_success_rate - psy.avg_pct_success) / psy.avg_std_success, 0) as success_weight
from artist_movie_success_rate ams join pct_success_year psy on (ams.category = psy.category and ams.year - 1 = psy.year)



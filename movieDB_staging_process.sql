#######################################################################################
# DATA 601 - Intro to Data Science - Final Project
# Team Members: Osani, Barca, Wulster, Shrishan
#
# Data loaded into MySQL DB on AWS
# Host Name: data601-project.cr4hisqf1ara.us-east-2.rds.amazonaws.com
# Port: 3306
######################################################################################

######################################################################################
# Data Load Routine
######################################################################################
LOAD DATA LOCAL INFILE 'Users/keithosani/School/data601/Final Project/IMDb movies.csv'
INTO TABLE imdb.movies FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n' ignore 1 lines ;

LOAD DATA LOCAL INFILE 'Users/keithosani/School/data601/Final Project/IMDb title_principals.csv'
INTO TABLE imdb.title_principals FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n' ignore 1 lines ;

LOAD DATA LOCAL INFILE 'Users/keithosani/School/data601/Final Project/IMDb names_clean.csv'
INTO TABLE imdb.names FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n' ignore 1 lines ;

LOAD DATA LOCAL INFILE 'Users/keithosani/School/data601/Final Project/IMDb ratings.csv'
INTO TABLE imdb.ratings FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n' ignore 1 lines ;

create index idx1 on movies_clean (imdb_title_id(20));
create index idx1 on title_principals (imdb_title_id(20), imdb_name_id(20));
create index idx1 on artists (imdb_name_id(20));
create index idx1 on names (imdb_name_id(20));

######################################################################################
# Step 1: Create a clean version of the Movie table
# US Only with valide budget and Revenue #s
# Remove older movies with very little information < 1979
# Remove low budget movies under $2M
######################################################################################

# Inert into the new Movies Clean table with clean version of budget, US gross income and worldwide gross income
insert into movies_clean
select m.imdb_title_id,
	m.title,
    m.original_title,
    m.year,
    m.date_published,
    m.genre,
    m.duration,
    m.country,
    m.language,
    m.director,
    m.writer,
    m.production_company,
    m.actors,
    m.avg_vote,
    m.votes,
    substring(m.budget,3, length(m.budget)) as budget,
    substring(m.usa_gross_income,3, length(m.usa_gross_income)) as usa_gross_income,
    substring(m.worlwide_gross_income,3, length(m.worlwide_gross_income)) as worldwide_gross_income,
    0 as pct_profitable,
    m.metascore,
    m.reviews_from_users,
    m.reviews_from_critics,
    0 as success_flag
from movies m
where country like '%US%' and
	usa_gross_income <> '' and
    budget <> '' and
    left(budget,1) = '$';
    
# Get rid of movies with a budget of 0
delete from movies_clean
where budget = 0;

# Get rid of older movies prior to 1979 as this DB really doesn't have more than 1 or 2 movies in each of those
# early years anyway
delete from movies_clean
where year < 1979;

# Remove small budget movies as defined by a budget of less than 2M
delete from movies_clean
where budget < 2000000;

######################################################################################
# Step 2: Continue with some additional clean ups and create a pct_profitable &
# success flag. Apply % profitability to all Artists
######################################################################################

## Calculate percent profitable on movies_clean table
update movies_clean
set pct_profitable = ((worldwide_gross_income - budget) / budget) * 100;

# Set the success_flag based on 55%, 0% and 100%
# We will need to rerun the script for each of these
update movies_clean
set success_flag = case when pct_profitable >= 100 then 1 else 0 end;

## We found some carriage returns in the category field...let's get rid of them
update title_principals
set category = REPLACE(category,'\r','');

## Set actresses to actor category
update title_principals
set category = 'actor'
where category = 'actress';

drop table artists;

## This will end up being the profitablilty by artist, director, writer, composer table
## We don't really need this table as we really need pct success rates
create table artists as
select *,
	 ((worldwide_gross_income - budget) / budget) * 100 as pct_profitable
from (
select n.imdb_name_id,
	   n.name,
       n.birth_name ,
       tp.category,
       sum(m.worldwide_gross_income) as worldwide_gross_income,
       sum(budget) as budget
from names n join title_principals tp on (n.imdb_name_id = tp.imdb_name_id)
			join movies_clean m on (tp.imdb_title_id = m.imdb_title_id)
group by 1,2,3,4
) a;

create index idx1 on artists (imdb_name_id(20));

drop table pct_profitable_year;

# Create pct profitablilty of all artists 
create table pct_profitable_year as
select a.year,
	a.category,
    a.pct_profitable,
    a.std_profitable,
    avg(b.pct_profitable) as avg_pct_profitable,
    avg(b.std_profitable) as avg_std_profitable
from (
select m.year,
	a.category,
	avg(a.pct_profitable) as pct_profitable,
    std(a.pct_profitable) as std_profitable
from movies_clean m join title_principals tp on (m.imdb_title_id = tp.imdb_title_id)
					join artists a on (tp.imdb_name_id = a.imdb_name_id)
group by 1,2
) a,
(
select m.year,
	a.category,
	avg(a.pct_profitable) as pct_profitable,
    std(a.pct_profitable) as std_profitable
from movies_clean m join title_principals tp on (m.imdb_title_id = tp.imdb_title_id)
					join artists a on (tp.imdb_name_id = a.imdb_name_id)
group by 1,2
) b
where a.year >= b.year and
	a.category = b.category 
group by 1,2,3,4
order by 2,1;


######################################################################################
# Step 3: Generate Artist to movie success rates & associated weights based
# on our formula
######################################################################################

drop table artist_movie_success_rate;

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
) b;

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
order by 2,1;

drop table artist_movie_success_weight;

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
from artist_movie_success_rate ams join pct_success_year psy on (ams.category = psy.category and ams.year - 1 = psy.year);

######################################################################################
# Step 4: Create a table that has all the sucess weights by movie
######################################################################################
 
drop table movie_success_weights;
 
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
group by 1;

######################################################################################
# Step 5: Calculate the success weights for genre
######################################################################################

drop table genre_success_rate;

create table genre_success_rate as
select year,
	genre,
    sum(success_flag) as num_success,
    count(*) as num_movies,
    sum(success_flag) / count(*) as genre_success_rate
from movies_clean
group by year,genre;
 
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
   
######################################################################################
# Step 6: Calculate the success weights for Production Companies
######################################################################################

drop table production_co_success_rate;

create table production_co_success_rate as
select year,
	production_company,
    sum(success_flag) as num_success,
    count(*) as num_movies,
    sum(success_flag) / count(*) as prod_co_success_rate
from movies_clean
group by year,production_company;

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
    

######################################################################################
# Step 7: Final Step to create the result set to be exported to CSV and used in 
# our Python scripts for predictive modeling
######################################################################################

select *
from movies_clean mc join movie_success_weights msw on (mc.imdb_title_id = msw.imdb_title_id)










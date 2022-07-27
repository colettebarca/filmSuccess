## Calculate percent profitable on movies_clean table
update movies_clean
set pct_profitable = ((worldwide_gross_income - budget) / budget) * 100

# Set the success_flag based on 55
update movies_clean
set success_flag = case when pct_profitable >= 100 then 1 else 0 end

select success_flag, count(*)
from movies_clean
group by 1
-- 5728

## We found some carriage returns in the category field...let's get rid of them
update title_principals
set category = REPLACE(category,'\r','')

## Set actresses to actor category
update title_principals
set category = 'actor'
where category = 'actress'

#drop table artists

## This will end up being the profitablilty by artist, director, writer, composer table
## We don't really. need this table as we really need pct success rates
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
) a

#create index idx1 on artists (imdb_name_id(20))

#drop table pct_profitable_year

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
order by 2,1







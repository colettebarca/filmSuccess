
#truncate table movies_clean

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
    left(budget,1) = '$'
    
# Get rid of movies with a budget of 0
delete from movies_clean
where budget = 0

# Get rid of older movies prior to 1979 as this DB really doesn't have more than 1 or 2 movies in each of those
# early years anyway
delete from movies_clean
where year < 1979

# Remove small budget movies as defined by a budget of less than 2M
delete from movies_clean
where budget < 2000000

select count(*)
from movies_clean

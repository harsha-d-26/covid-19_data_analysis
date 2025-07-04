CREATE OR REPLACE PROCEDURE public.merge_data(
	)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
merge into dim_location t using location_stg s
                 on(t.country = s.country)
                 when matched then update set
                 continent = s.continent,
                 continent_id = s.continent_id,
                 cont_lat = s.cont_lat,
                 cont_long = s.cont_long,
                 country_id = s.country_id,
                 abreviation = s.abreviation,
                 count_lat = s.count_lat,
                 count_long = s.count_long
                 when not matched then insert
                 (continent,
                 continent_id,
                 cont_lat,
                 cont_long,
                 country,
                 country_id,
                 abreviation,
                 count_lat, 
                 count_long,
                 insert_time) 
                 values
                 (s.continent,
                 s.continent_id,
                 s.cont_lat,
                 s.cont_long,
                 s.country,
                 s.country_id,
                 s.abreviation,
                 s.count_lat,
                 s.count_long,
                 s.insert_time);

merge into fact_continent_cases t using continent_cases_stg s
on(s.continent = t.continent)
when matched then update set
population = s.population,
cases = s.cases,
deaths = s.deaths,
recovered = s.recovered,
active = s.active,
critical = s.critical,
tests = s.tests
when not matched then insert
(continent,
population,
cases,
deaths,
recovered,
active,
critical,
tests)
values(
s.continent,
s.population,
s.cases,
s.deaths,
s.recovered,
s.active,
s.critical,
s.tests
);

merge into fact_country_cases t using country_cases_stg s
on(s.country = t.country)
when matched then update set
population = s.population,
cases = s.cases,
deaths = s.deaths,
recovered = s.recovered,
active = s.active,
critical = s.critical,
tests = s.tests
when not matched then insert
(country,
population,
cases,
deaths,
recovered,
active,
critical,
tests)
values(
s.country,
s.population,
s.cases,
s.deaths,
s.recovered,
s.active,
s.critical,
s.tests
);

END;
$BODY$;
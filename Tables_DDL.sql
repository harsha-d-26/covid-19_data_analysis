CREATE TABLE public.dim_location (
    continent text,
    continent_id bigint,
    cont_lat double precision,
    cont_long double precision,
    country_id bigint,
    country text,
    abreviation text,
    count_lat double precision,
    count_long double precision,
    insert_time timestamp without time zone
);

CREATE TABLE public.fact_continent_cases (
    continent text,
    population bigint,
    cases bigint,
    deaths bigint,
    recovered bigint,
    active bigint,
    critical bigint,
    tests bigint
);

CREATE TABLE public.fact_country_cases (
    country text,
    population bigint,
    cases bigint,
    deaths bigint,
    recovered bigint,
    active bigint,
    critical bigint,
    tests bigint
);
drop table if exists districts;

create table districts (
  district_id int,
  type text,
  name text,
  state text,
  area_square_km float,
  population int,
  population_per_square_km int
);

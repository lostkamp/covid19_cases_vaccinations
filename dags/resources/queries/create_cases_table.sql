drop table if exists cases;

create table cases as
with base as (
  select to_date(meldedatumiso, 'YYYY-MM-DD', True) as date,
         idlandkreis::int as district_id,
         sum(AnzahlFall) as num_cases
  from staging_cases
  group by date, district_id
),

rolling_new_cases as (
    select *,
           sum(num_cases) over (partition by district_id order by date asc
               rows between 6 preceding and current row) as new_cases_last_7_days
    from base
)

select * from rolling_new_cases;

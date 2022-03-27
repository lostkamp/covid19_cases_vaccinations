drop table if exists vaccinations;

create table vaccinations as

with base as (
    select impfdatum           as vaccination_date,
           landkreisid_impfort as district_id,
           altergruppe         as age_group,
           impfschutz          as vaccination_type,
           anzahl              as count
    from staging_vaccinations
    where landkreisid_impfort != 'u'
),

casted as (
    select to_date(vaccination_date, 'YYYY-MM-DD', True) as vaccination_date,
            -- following expression from
            -- https://blog.getdbt.com/how-to-safely-convert-strings-to-integers-in-redshift/
           case when trim(district_id) ~ '^[0-9]+$'
                then trim(district_id)
                else null
           end::int as district_id,
           age_group,
           vaccination_type,
           count
    from base
),

grouped as (
    select vaccination_date, district_id, vaccination_type, sum(count) as count
    from casted
    group by vaccination_date, district_id, vaccination_type
),

pivoted as (
    select vaccination_date,
    district_id,
    sum(case when vaccination_type = 1 then count end) as incomplete_vaccination_count,
    sum(case when vaccination_type = 2 then count end) as complete_vaccination_count,
    sum(case when vaccination_type = 3 then count end) as booster_vaccination_count
    from grouped
    group by vaccination_date, district_id
)

select * from pivoted

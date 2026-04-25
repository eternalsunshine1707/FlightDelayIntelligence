with flights as (
    select * from {{ ref('fct_delays') }}
),

airport_stats as (
    select
        origin_airport                                                          as airport_code,
        count(*)                                                                as total_departures,
        sum(case when arrival_delay_mins > 15 then 1 else 0 end)               as total_delayed,
        sum(case when is_cancelled = 1 then 1 else 0 end)                      as total_cancelled,
        round(avg(departure_delay_mins), 2)                                     as avg_departure_delay_mins,
        round(
            sum(case when arrival_delay_mins > 15 then 1 else 0 end) * 100.0 / count(*), 2
        )                                                                       as delay_rate_pct,
        round(
            sum(case when is_cancelled = 1 then 1 else 0 end) * 100.0 / count(*), 2
        )                                                                       as cancellation_rate_pct
    from flights
    where is_cancelled = 0
    group by origin_airport
    having count(*) > 100
)

select * from airport_stats
order by delay_rate_pct desc
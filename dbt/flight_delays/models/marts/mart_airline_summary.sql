with flights as (
    select * from {{ ref('fct_delays') }}
),

summary as (
    select
        airline_code,
        airline_name,
        count(*)                                                                as total_flights,
        sum(case when arrival_delay_mins > 15 then 1 else 0 end)               as total_delayed,
        sum(case when is_cancelled = 1 then 1 else 0 end)                      as total_cancelled,
        round(avg(arrival_delay_mins), 2)                                       as avg_arrival_delay_mins,
        round(avg(departure_delay_mins), 2)                                     as avg_departure_delay_mins,
        round(
            sum(case when arrival_delay_mins > 15 then 1 else 0 end) * 100.0 / count(*), 2
        )                                                                       as delay_rate_pct,
        round(
            sum(case when is_cancelled = 1 then 1 else 0 end) * 100.0 / count(*), 2
        )                                                                       as cancellation_rate_pct,
        round(avg(distance_miles), 2)                                           as avg_distance_miles,
        round(avg(air_time_mins), 2)                                            as avg_air_time_mins,
        max(arrival_delay_mins)                                                 as worst_delay_mins
    from flights
    where is_cancelled = 0
    group by airline_code, airline_name
)

select * from summary
order by delay_rate_pct desc
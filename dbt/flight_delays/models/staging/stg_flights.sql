with source as (
    select * from flight_delays_db.flights_cleaned
),

staged as (
    select
        flight_date,
        airline_code,
        origin_airport,
        dest_airport,
        coalesce(dep_delay, 0)       as departure_delay_mins,
        coalesce(arr_delay, 0)       as arrival_delay_mins,
        cast(is_cancelled as int)    as is_cancelled,
        cancellation_code            as cancellation_reason,
        coalesce(air_time, 0)        as air_time_mins,
        coalesce(distance, 0)        as distance_miles,
        is_delayed,
        delay_category,
        day_of_week,
        month,
        year
    from source
    where flight_date is not null
      and airline_code is not null
)

select * from staged
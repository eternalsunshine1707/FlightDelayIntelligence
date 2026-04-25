with staged as (
    select * from {{ ref('stg_flights') }}
),

enriched as (
    select
        flight_date,
        airline_code,
        case airline_code
            when 'AA' then 'American Airlines'
            when 'DL' then 'Delta Air Lines'
            when 'UA' then 'United Airlines'
            when 'WN' then 'Southwest Airlines'
            when 'B6' then 'JetBlue Airways'
            when 'AS' then 'Alaska Airlines'
            when 'NK' then 'Spirit Airlines'
            when 'F9' then 'Frontier Airlines'
            when 'HA' then 'Hawaiian Airlines'
            when 'G4' then 'Allegiant Air'
            when 'MQ' then 'American Eagle'
            when 'OO' then 'SkyWest Airlines'
            when 'YX' then 'Midwest Express'
            when 'OH' then 'PSA Airlines'
            when 'YV' then 'Mesa Airlines'
            when '9E' then 'Endeavor Air'
            else airline_code
        end                                                     as airline_name,
        origin_airport,
        dest_airport,
        departure_delay_mins,
        arrival_delay_mins,
        is_cancelled,
        cancellation_reason,
        case cancellation_reason
            when 'A' then 'Carrier'
            when 'B' then 'Weather'
            when 'C' then 'National Air System'
            when 'D' then 'Security'
            else 'Not Cancelled'
        end                                                     as cancellation_reason_desc,
        air_time_mins,
        distance_miles,
        is_delayed,
        delay_category,
        day_of_week,
        month,
        year,
        case
            when arrival_delay_mins <= 0 then 'Early/On-time'
            when arrival_delay_mins <= 15 then 'Minor Delay'
            when arrival_delay_mins <= 45 then 'Moderate Delay'
            when arrival_delay_mins <= 120 then 'Severe Delay'
            else 'Extreme Delay'
        end                                                     as delay_severity,
        case when is_cancelled = 1 then 1 else 0 end           as flight_cancelled,
        case when arrival_delay_mins > 15 then 1 else 0 end    as flight_delayed
    from staged
)

select * from enriched
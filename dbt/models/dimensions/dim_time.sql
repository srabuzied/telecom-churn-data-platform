with times as (

    select distinct
        time(event_time) as full_time
    from {{ source('telecom', 'stg_usage_events') }}

    union distinct

    select distinct
        time(event_time)
    from {{ source('telecom', 'stg_network_events') }}

    union distinct

    select distinct
        time(event_time)
    from {{ source('telecom', 'stg_customer_care_calls') }}

    union distinct

    select distinct
        time(event_time)
    from {{ source('telecom', 'stg_churn_risk') }}

)

select

    row_number() over(order by full_time) as time_key,

    full_time,
    extract(hour from full_time) as hour,
    extract(minute from full_time) as minute,
    extract(second from full_time) as second

from times
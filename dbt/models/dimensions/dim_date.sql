with dates as (

    select distinct
        date(event_time) as full_date
    from {{ source('telecom', 'stg_usage_events') }}

    union distinct

    select distinct
        date(event_time)
    from {{ source('telecom', 'stg_network_events') }}

    union distinct

    select distinct
        date(event_time)
    from {{ source('telecom', 'stg_customer_care_calls') }}

    union distinct

    select distinct
        date(event_time)
    from {{ source('telecom', 'stg_churn_risk') }}

)

select

    row_number() over(order by full_date) as date_key,

    full_date,
    extract(year from full_date) as year,
    extract(month from full_date) as month,
    extract(day from full_date) as day,
    extract(quarter from full_date) as quarter

from dates
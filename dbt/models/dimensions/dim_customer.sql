with customers as (

    select distinct
        customer_id,
        income,
        marital,
        adults,
        area,
        prizm_social_one,
        prizm_social_mapped,
        household_segment

    from {{ source('telecom', 'stg_silver_telecom') }}

)

select
    row_number() over(order by customer_id) as customer_key,
    customer_id,
    income,
    marital,
    adults,
    area,
    prizm_social_one,
    prizm_social_mapped,
    household_segment

from customers
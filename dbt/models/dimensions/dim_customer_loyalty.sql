with loyalty as (

    select distinct
        customer_loyalty,
        clv_segment,
        crclscod,
        crclscod_bin,
        asl_flag

    from {{ source('telecom', 'stg_silver_telecom') }}

)

select
    row_number() over(
        order by
            customer_loyalty,
            clv_segment,
            crclscod,
            crclscod_bin,
            asl_flag
    ) as loyalty_key,

    customer_loyalty,
    clv_segment,
    crclscod,
    crclscod_bin,
    asl_flag

from loyalty
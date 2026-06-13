with devices as (

    select distinct
        refurb_new,
        device_age,
        eqpdays

    from {{ source('telecom', 'stg_silver_telecom') }}

)

select
    row_number() over(
        order by
            refurb_new,
            device_age,
            eqpdays
    ) as device_key,

    refurb_new,
    device_age,
    eqpdays

from devices
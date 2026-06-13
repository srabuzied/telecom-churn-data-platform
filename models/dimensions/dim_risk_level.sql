select

    row_number() over(order by risk_level) as risk_level_key,
    risk_level

from (

    select distinct
        risk_level
    from {{ source('telecom', 'stg_churn_risk') }}

)
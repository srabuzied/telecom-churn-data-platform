select

    row_number() over(order by issue_type) as issue_type_key,
    issue_type

from (

    select distinct
        issue_type
    from {{ source('telecom', 'stg_customer_care_calls') }}
    where issue_type is not null

)
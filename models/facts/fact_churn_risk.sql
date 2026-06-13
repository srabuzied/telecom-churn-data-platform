select

    row_number() over(
        order by
            cr.customer_id,
            cr.event_time
    ) as risk_event_key,

    c.customer_key,

    r.risk_level_key,

    d.date_key,

    t.time_key,

    cr.churn_score

from {{ source('telecom', 'stg_churn_risk') }} cr

left join {{ ref('dim_customer') }} c
    on cast(cr.customer_id as int64) = c.customer_id

left join {{ ref('dim_risk_level') }} r
    on cr.risk_level = r.risk_level

left join {{ ref('dim_date') }} d
    on date(cr.event_time) = d.full_date

left join {{ ref('dim_time') }} t
    on time(cr.event_time) = t.full_time
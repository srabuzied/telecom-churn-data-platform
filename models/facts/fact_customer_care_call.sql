select

    row_number() over(
        order by
            cc.customer_id,
            cc.event_time
    ) as call_key,

    c.customer_key,

    i.issue_type_key,

    d.date_key,

    t.time_key,

    cc.anger_rate,

    cc.call_duration_sec,

    cc.resolved

from {{ source('telecom', 'stg_customer_care_calls') }} cc

left join {{ ref('dim_customer') }} c
    on cast(cc.customer_id as int64) = c.customer_id

left join {{ ref('dim_issue_type') }} i
    on cc.issue_type = i.issue_type

left join {{ ref('dim_date') }} d
    on date(cc.event_time) = d.full_date

left join {{ ref('dim_time') }} t
    on time(cc.event_time) = t.full_time
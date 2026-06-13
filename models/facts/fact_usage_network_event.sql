select

    row_number() over(
        order by
            u.customer_id,
            u.event_time
    ) as usage_network_event_key,

    c.customer_key,
    d.date_key,
    t.time_key,

    u.voice_minutes,
    u.data_usage_mb,
    u.sms_count,
    u.package_usage_pct,

    n.signal_strength,
    n.dropped_calls,
    n.internet_speed,
    n.outage_flag

from {{ source('telecom', 'stg_usage_events') }} u

left join {{ source('telecom', 'stg_network_events') }} n
    on u.customer_id = n.customer_id
   and u.event_time = n.event_time

left join {{ ref('dim_customer') }} c
    on cast(u.customer_id as int64) = c.customer_id

left join {{ ref('dim_date') }} d
    on date(u.event_time) = d.full_date

left join {{ ref('dim_time') }} t
    on time(u.event_time) = t.full_time
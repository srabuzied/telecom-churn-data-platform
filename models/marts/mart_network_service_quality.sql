{{ config(materialized='table') }}

with usage_agg as (

    select
        customer_key,

        avg(data_usage_mb) as avg_data_usage_mb,
        avg(signal_strength) as avg_signal_strength,
        avg(internet_speed) as avg_internet_speed,
        avg(package_usage_pct) as avg_package_usage_pct

    from {{ ref('fact_usage_network_event') }}
    group by customer_key

)

select

    fs.customer_key,

    -- Customer Segmentation
    dc.area,
    dl.customer_loyalty,
    dl.clv_segment,

    -- Device Information
    dd.device_age,
    dd.refurb_new,

    -- Network Experience
    fs.network_quality,
    fs.data_experience,
    fs.is_roamer,

    -- Network Reliability
    fs.data_failure_rate,

    -- Call Quality Metrics
    fs.completed_call_rate,
    fs.blocked_call_rate,
    fs.drop_block_call_rate,

    -- Call Failure Metrics
    fs.drop_vce_mean,
    fs.drop_dat_mean,

    fs.blck_vce_mean,
    fs.blck_dat_mean,

    fs.attempt_mean,

    -- Customer Care
    fs.custcare_mean,

    -- Usage Metrics
    ua.avg_data_usage_mb,
    ua.avg_signal_strength,
    ua.avg_internet_speed,
    ua.avg_package_usage_pct

from {{ ref('fact_customer_snapshot') }} fs

left join {{ ref('dim_customer') }} dc
    on fs.customer_key = dc.customer_key

left join {{ ref('dim_customer_loyalty') }} dl
    on fs.loyalty_key = dl.loyalty_key

left join {{ ref('dim_device') }} dd
    on fs.device_key = dd.device_key

left join usage_agg ua
    on fs.customer_key = ua.customer_key
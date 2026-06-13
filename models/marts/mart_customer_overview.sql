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

),

care_agg as (

    select
        customer_key,

        avg(anger_rate) as avg_anger_rate,
        avg(call_duration_sec) as avg_call_duration_sec,

        avg(
            case
                when resolved = 'Yes' then 1
                else 0
            end
        ) as resolution_rate

    from {{ ref('fact_customer_care_call') }}
    group by customer_key

)

select

    fs.customer_key,

    -- Revenue Metrics
    fs.totrev,
    fs.avgrev,
    fs.rev_mean,
    fs.change_rev,

    -- Customer Metrics
    fs.churn,
    fs.network_quality,
    fs.data_experience,
    fs.is_roamer,
    fs.overage_flag,
    fs.overage_status,

    -- Customer Dimension
    dc.area,
    dc.household_segment,
    dc.prizm_social_mapped,

    -- Loyalty Dimension
    dl.customer_loyalty,
    dl.clv_segment,

    -- Device Dimension
    dd.device_age,
    dd.refurb_new,

    -- Aggregated Usage Metrics
    ua.avg_data_usage_mb,
    ua.avg_signal_strength,
    ua.avg_internet_speed,
    ua.avg_package_usage_pct,

    -- Aggregated Customer Care Metrics
    ca.resolution_rate,
    ca.avg_call_duration_sec,
    ca.avg_anger_rate

from {{ ref('fact_customer_snapshot') }} fs

left join {{ ref('dim_customer') }} dc
    on fs.customer_key = dc.customer_key

left join {{ ref('dim_customer_loyalty') }} dl
    on fs.loyalty_key = dl.loyalty_key

left join {{ ref('dim_device') }} dd
    on fs.device_key = dd.device_key

left join usage_agg ua
    on fs.customer_key = ua.customer_key

left join care_agg ca
    on fs.customer_key = ca.customer_key
select

    row_number() over(order by s.customer_id) as snapshot_key,

    c.customer_key,
    d.device_key,
    l.loyalty_key,

    dt.date_key,
    tm.time_key,

    s.churn,

    s.rev_mean,
    s.totrev,
    s.adjrev,
    s.avgrev,
    s.change_rev,

    s.totmrc_mean,

    s.refunds,

    s.mou_mean,
    s.adjmou,
    s.adjqty,
    s.change_mou,

    s.mou_cvce_mean,
    s.mou_cdat_mean,
    s.inonemin_mean,

    s.ovrmou_mean,
    s.ovrrev_mean,
    s.datovr_mean,

    s.overage_flag,
    s.overage_rate,
    s.overage_status,

    s.is_roamer,
    s.roam_mean,

    s.custcare_mean,
    s.cc_mou_mean,

    s.drop_vce_mean,
    s.drop_dat_mean,

    s.blck_vce_mean,
    s.blck_dat_mean,

    s.unan_dat_mean,

    s.drop_blk_mean,
    s.attempt_mean,

    s.completed_call_rate,
    s.blocked_call_rate,
    s.drop_block_call_rate,

    s.network_quality,
    s.data_failure_rate,
    s.data_experience

from {{ source('telecom','stg_silver_telecom') }} s

left join {{ ref('dim_customer') }} c
    on s.customer_id = c.customer_id

left join {{ ref('dim_device') }} d
    on s.device_age = d.device_age
   and s.refurb_new = d.refurb_new
   and s.eqpdays = d.eqpdays

left join {{ ref('dim_customer_loyalty') }} l
    on s.customer_loyalty = l.customer_loyalty
   and s.clv_segment      = l.clv_segment
   and s.crclscod         = l.crclscod
   and s.crclscod_bin     = l.crclscod_bin
   and s.asl_flag         = l.asl_flag

left join {{ ref('dim_date') }} dt
    on date(s.pipeline_run_ts) = dt.full_date

left join {{ ref('dim_time') }} tm
    on time(s.pipeline_run_ts) = tm.full_time
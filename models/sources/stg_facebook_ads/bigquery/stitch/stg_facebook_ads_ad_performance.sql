{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_facebook_ads_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_ad_performance_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_performance_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_facebook_ads_stitch_ad_performance_table'),unique_column='ad_id') }}
),
renamed as (
SELECT
    date_start                   as ad_serve_ts,
    ad_name                      as ad_name,
    adset_name                   as ad_adset_name,
    campaign_name                as ad_campaign_name,
    objective                    as ad_objective,
    cast(ad_id as string)        as ad_id,
    cast(adset_id as string)     as ad_adset_id,
    cast(campaign_id as string)  as ad_campaign_id,
    spend                        as ad_total_cost,
    clicks                       as ad_total_clicks,
    safe_divide(spend,clicks)    as ad_avg_cost,
    cast(null as timestamp)      as ad_avg_time_on_site,
    cast(null as float64)        as ad_bounce_rate,
    cast(null as int64)          as ad_total_assisted_conversions,
    cpp                          as ad_cpp,
    ctr                          as ad_ctr,
    cpc                          as ad_cpc,
    cpm                          as ad_cpm,
    impressions                  as ad_total_impressions,
    reach                        as ad_total_reach,
    frequency                    as ad_frequency,
    unique_clicks                as ad_total_unique_clicks,
    cast(null as float64)        as ad_total_conversion_value,
    video_play_curve_actions     as video_play_curve_actions,
    video_p100_watched_actions   as video_p100_watched_actions,
    video_p75_watched_actions    as video_p75_watched_actions,
    video_p50_watched_actions    as video_p50_watched_actions,
    video_p25_watched_actions    as video_p25_watched_actions,
    unique_actions               as unique_actions,
    action_values                as action_values,
    outbound_clicks              as outbound_clicks,
    video_30_sec_watched_actions as video_30_sec_watched_actions,
    'Facebook Ads'               as ad_network
FROM
  source
)

select
  *
from
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}

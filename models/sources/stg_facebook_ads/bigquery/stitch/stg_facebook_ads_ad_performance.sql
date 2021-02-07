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
    cast(ad_id as string)        as ad_id,
    safe_divide(spend,clicks)    as ad_avg_cost,
    cast(null as timestamp)      as ad_avg_time_on_site,
    cast(null as float64)        as ad_bounce_rate,
    cast(null as int64)          as ad_total_assisted_conversions,
    clicks                       as ad_total_clicks,
    impressions                  as ad_total_impressions,
    reach                        as ad_total_reach,
    unique_clicks                as ad_total_unique_clicks,
    cast(null as float64)        as ad_total_conversion_value,
    spend                        as ad_total_cost,
    'Facebook Ads'               as ad_network
FROM
  source)
select
  *
from
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}

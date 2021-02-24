{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_google_ads_etl") == 'transfer')
   )
}}
{% if var("marketing_warehouse_ad_performance_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_performance_sources") %}


WITH campaignBase AS (SELECT /* Antigo stg_campaignBase.sql    cb */
campaignId,
externalCustomerId,
Date,
{{gads_extract('ISOWEEK')}},
{{gads_extract('month')}},
{{gads_extract('year')}},
sum(clicks) Clicks, 
{{gads_cost()}}
FROM {{var('stg_google_ads_transfer_campaignStats')}}
GROUP BY 1,2,3,4,5,6),


campaignLookup AS (SELECT   /* Antigo stg_campaignLookup.sql     cm*/
campaignId, 
cName campaignName
FROM (SELECT 
campaignId, 
LAST_VALUE(campaignName) OVER(PARTITION BY campaignId order by _PARTITIONTIME asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) cName
FROM {{var('stg_google_ads_transfer_campaignStats')}}
GROUP BY 1, _partitiontime, campaignName)
GROUP BY 1, 2
ORDER BY 1 DESC),

campaignImpressions AS (SELECT   /* im */ 
campaignId, 
externalCustomerId,
Date,
{{gads_extract('ISOWEEK')}},
{{gads_extract('month')}},
{{gads_extract('year')}},
sum(Impressions) impr
FROM {{var('stg_google_ads_transfer_campaignStats')}}  /* de t_campaignStats para stg_google_ads_transfer_campaignStats*/
WHERE clickType = "URL_CLICKS"
GROUP BY 1,2,3,4,5,6),

campaignConversions AS (SELECT /* Antigo stg_campaignConversions    cc */
campaignId, 
externalCustomerId,
Date,
{{gads_extract('ISOWEEK')}},
{{gads_extract('month')}},
{{gads_extract('year')}},
SUM(Conversions) Conversions,
SUM(CASE WHEN ConversionTypeName = {{var('convName1')}} THEN Conversions ELSE 0 END) as conv_obrigado,
SUM(CASE WHEN ConversionTypeName = {{var('convName2')}} THEN Conversions ELSE 0 END) as conv_blog,
WITH campaignBase AS (SELECT /* Antigo stg_campaignBase.sql    cb */
campaignId,
externalCustomerId,
Date,
{{gads_extract('ISOWEEK')}},
{{gads_extract('month')}},
{{gads_extract('year')}},
sum(clicks) Clicks, 
{{cost()}}
FROM {{var('stg_google_ads_transfer_campaignStats')}}
GROUP BY 1,2,3,4,5,6),


campaignLookup AS (SELECT   /* Antigo stg_campaignLookup.sql     cm*/
campaignId, 
cName campaignName
FROM (SELECT 
campaignId, 
LAST_VALUE(campaignName) OVER(PARTITION BY campaignId order by _PARTITIONTIME asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) cName
FROM {{var('stg_google_ads_transfer_campaignStats')}}
GROUP BY 1, _partitiontime, campaignName)
GROUP BY 1, 2
ORDER BY 1 DESC),

campaignImpressions AS (SELECT   /* im */ 
campaignId, 
externalCustomerId,
Date,
{{gads_extract('ISOWEEK')}},
{{gads_extract('month')}},
{{gads_extract('year')}},
sum(Impressions) impr
FROM {{var('stg_google_ads_transfer_campaignStats')}}  /* de t_campaignStats para stg_google_ads_transfer_campaignStats*/
WHERE clickType = "URL_CLICKS"
GROUP BY 1,2,3,4,5,6),

campaignConversions AS (SELECT /* Antigo stg_campaignConversions    cc */
campaignId, 
externalCustomerId,
Date,
{{gads_extract('ISOWEEK')}},
{{gads_extract('month')}},
{{gads_extract('year')}},
SUM(Conversions) Conversions,
SUM(CASE WHEN ConversionTypeName = {{var('convName1')}} THEN Conversions ELSE 0 END) as conv_obrigado,
SUM(CASE WHEN ConversionTypeName = {{var('convName2')}} THEN Conversions ELSE 0 END) as conv_blog,
SUM(CASE WHEN ConversionTypeName = {{var('convName3')}} THEN Conversions ELSE 0 END) as conv_ebook
FROM {{var('stg_google_ads_transfer_t_campaignConv')}}  /* De t_campaignConv para stg_google_ads_transfer_t_campaignConv*/
GROUP BY 1,2,3,4,5,6) 


SELECT
cm.CampaignName, 
im.impr Impressions,
cb.*,
cc.Conversions,
cc.conv_obrigado, 
cc.conv_blog, 
cc.conv_ebook,
(sum(cb.clicks) / sum(NULLIF(im.impr,0))) ctr,
(sum(cb.cost) / sum(NULLIF(cb.clicks,0))) cpc,
(sum(cc.Conversions) / sum(NULLIF(cb.clicks,0))) convRate,
(sum(cb.Cost) / sum(NULLIF(cc.Conversions, 0))) costPerConv
FROM campaignBase cb
JOIN campaignLookup cm
ON cb.campaignId = cm.campaignId
JOIN campaignImpressions im
ON cb.campaignId = im.campaignId 
AND cb.externalCustomerId = im.externalCustomerId
AND cb.Date = im.Date
AND cb.ISOWEEK = im.ISOWEEK
AND cb.month = im.month
AND cb.year = im.year
LEFT JOIN campaignConversions cc
ON cb.campaignId = cc.campaignId
AND cb.ISOWEEK = cc.ISOWEEK
AND cb.month = cc.month
AND cb.year = cc.year
AND cb.Date = cc.Date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}

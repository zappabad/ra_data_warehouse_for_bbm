{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_google_ads_etl") == 'transfer')
   )
}}
{% if var("marketing_warehouse_ad_performance_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_performance_sources") %}

{% set relationzz = api.Relation.create(database='databases-297008', schema='gads_goes', identifier='p_AdGroupConversionStats_5726034613') %}
{% set nameslist = dbt_utils.get_column_values(relationzz, "ConversionTypeName") %}
{% set typeslist = dbt_utils.get_column_values(relationzz, "ConversionCategoryName") %}
{% set count = namespace(value=0) %}

WITH conv AS(
SELECT 
Date,
CampaignId,
AdGroupId,
externalCustomerId,
ConversionCategoryName,
ConversionTypeName,
sum(Conversions) as Conversions
FROM {{var('stg_google_ads_transfer_adGroupConv')}}
GROUP BY
    Date, CampaignId, AdGroupId, externalCustomerId, ConversionCategoryName, ConversionTypeName
ORDER BY
    Date, CampaignId, AdGroupId, externalCustomerId, ConversionCategoryName, ConversionTypeName
),
gridss AS(
SELECT
    CampaignId,
    AdGroupId,
    externalCustomerId,
    ConversionCategoryName,
    ConversionTypeName,
    {% for names in nameslist %}
        {% for types in typeslist %}
        {% set count.value = count.value + 1 %}
        sum(IF(ConversionCategoryName = "{{types}}", 
            IF(ConversionTypeName = "{{names}}", Conversions, NULL), NULL)) AS col{{count.value}},
        {% endfor %}
    {% endfor %}
    Date    
FROM conv
GROUP BY
    Date,
    CampaignId,
    AdGroupId,
    externalCustomerId,
    ConversionCategoryName,
    ConversionTypeName
)

SELECT 
    Date,
    CampaignId,
    AdGroupId,
    
    [
    STRUCT(
        STRUCT(
            [max(ConversionTypeName) OVER (PARTITION BY Date, CampaignId, AdGroupId ORDER BY Date, CampaignId, AdGroupId), '10'] AS ConversionCategoryName,
            SUM(col2) OVER (PARTITION BY Date, CampaignId, AdGroupId ORDER BY Date, CampaignId, AdGroupId) AS NOME_DA_CATEGORIA
        ) AS value
    ),
    STRUCT(
        STRUCT(
            [max(ConversionTypeName) OVER (PARTITION BY Date, CampaignId, AdGroupId ORDER BY Date, CampaignId, AdGroupId), '10'],
            SUM(col3) OVER (PARTITION BY Date, CampaignId, AdGroupId ORDER BY Date, CampaignId, AdGroupId) AS NOME_DA_CATEGORIA
        ) AS value
    )
    ] AS Conversions
FROM gridss 







{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% if var('marketing_warehouse_ad_campaign_sources') %}


with campaigns as
  (
    {% for source in var('marketing_warehouse_ad_campaign_sources') %}
      {% set relation_source = 'stg_' + source + '_campaigns' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select *,
       case when ad_network = 'Google Ads' then 'ppc'
            when ad_network = 'Facebook Ads' then 'paid_social'
            when ad_network in ('Mailchimp','Hubspot Email') then 'email'
            else null end as utm_medium
 from campaigns

 {% else %}

 {{
     config(
         enabled=false
     )
 }}


 {% endif %}

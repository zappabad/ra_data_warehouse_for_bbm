 {% macro gads_extract(v) -%}
extract({{v}} from date) {{v}}      
{%- endmacro %}

{% macro gads_ctr() -%}
sum(clicks / NULLIF(impressions,0)) ctr
{%- endmacro %}

{% macro gads_cpc() -%}
sum((cost/1000000) / NULLIF(clicks,0)) cpc
{%- endmacro %}

{% macro gads_cost() -%}
sum(cost/1000000) cost
{%- endmacro %}

{% macro gads_conversions() -%}
sum(Conversions) conversions
{%- endmacro %}

{% macro gads_convRate() -%}
sum(Conversions / NULLIF(clicks,0)) convRate
{%- endmacro %}

{% macro gads_costPerConv() -%}
sum(Conversions / NULLIF(clicks,0)) costPerConv
{%- endmacro %}

{% set relationzz = api.Relation.create(database='databases-297008', schema='gads_goes', identifier='p_AdGroupConversionStats_5726034613') %}
{% set deviceslist = dbt_utils.get_column_values(relationzz, "Device") %}
{% set nameslist = dbt_utils.get_column_values(relationzz, "ConversionTypeName") %}
{% set typeslist = dbt_utils.get_column_values(relationzz, "ConversionCategoryName") %}

SELECT
{% for devices in deviceslist %}
    {% for names in nameslist %}
        {% for types in typeslist  %}
            {{devices}}, {{names}}, {{types}}
        {% endfor %}
    {% endfor %}
{% endfor %}
FROM

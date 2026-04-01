{% macro truncate_date(date_column, period='month') %}
  DATE_TRUNC('{{ period }}', {{ date_column }})
{% endmacro %}

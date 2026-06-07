{% macro construct_array(exprs) %}
  {{ return(adapter.dispatch('construct_array', 'openhrefs')(exprs)) }}
{% endmacro %}


{% macro duckdb__construct_array(exprs) %}
  array_value({{ exprs | join(', ') }})
{% endmacro %}


{% macro spark__construct_array(exprs) %}
  array({{ exprs | join(', ') }})
{% endmacro %}

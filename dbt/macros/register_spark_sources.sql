{% macro register_spark_sources() %}
    {%- if target.type == 'spark' and execute -%}
        {%- set raw_path = env_var('RAW_PATH') -%}
        {%- if '://' not in raw_path and not raw_path.startswith('/') -%}
            {{ exceptions.raise_compiler_error(
                "RAW_PATH must be an absolute path for the Spark target (got: '"
                ~ raw_path ~ "'). Use /absolute/path, gs://, s3://, or file://"
            ) }}
        {%- endif -%}
        {%- do run_query('create database if not exists raw_cc') -%}
        {%- do run_query('drop table if exists raw_cc.cc_domain_authority') -%}
        {%- do run_query(
            "create table raw_cc.cc_domain_authority"
            ~ " using parquet location '" ~ raw_path ~ "/cc_domain_authority'"
        ) -%}
        {%- do run_query('drop table if exists raw_cc.cc_domain_pagerank') -%}
        {%- do run_query(
            "create table raw_cc.cc_domain_pagerank"
            ~ " using parquet location '" ~ raw_path ~ "/cc_domain_pagerank'"
        ) -%}
    {%- endif -%}
{% endmacro %}

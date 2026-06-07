{{
    config(
        materialized='external' if target.type == 'duckdb' else 'incremental',
        incremental_strategy='insert_overwrite',
        location=env_var('MARTS_PATH', 'target') ~ '/mart_domain_authority.parquet',
        location_root=env_var('MARTS_PATH', 'target'),
        file_format='parquet'
    )
}}

with pagerank as (

    select * from {{ ref('int_pagerank') }}

)

, crawl_window as (

    select * from {{ ref('int_crawl_window') }}

)

, final as (

    select
        pagerank.domain
        , crawl_window.window_id
        , crawl_window.window_end_crawl
        , crawl_window.window_crawls
        , pagerank.pagerank_score
        , pagerank.open_volume
        , pagerank.open_authority
        , pagerank.authority_ratio
    from pagerank
    inner join crawl_window
        on pagerank.crawl = crawl_window.window_end_crawl

)

select * from final

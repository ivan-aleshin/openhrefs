with crawls as (

    select distinct crawl from {{ ref('stg_cc_domain_pagerank') }}

)

, final as (

    select
        crawl as window_id
        , crawl as window_end_crawl
        , {{ construct_array(["crawl"]) }} as window_crawls
    from crawls

)

select * from final

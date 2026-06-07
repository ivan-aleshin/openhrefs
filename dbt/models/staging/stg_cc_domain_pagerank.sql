with source as (

    select * from {{ source('raw_cc', 'cc_domain_pagerank') }}

)

, final as (

    select
        domain
        , crawl
        , cast(pagerank_score as double) as pagerank_score
        , cast(in_degree as bigint) as in_degree
        , cast(out_degree as bigint) as out_degree
    from source

)

select * from final

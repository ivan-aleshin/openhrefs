with pagerank as (

    select * from {{ ref('stg_cc_domain_pagerank') }}

)

, authority as (

    select * from {{ ref('stg_cc_domain_authority') }}

)

, final as (

    select
        pagerank.domain
        , pagerank.crawl
        , pagerank.pagerank_score
        , authority.open_authority
        , authority.open_volume
        , case
            when authority.open_volume = 0 then null
            else authority.open_authority / authority.open_volume
        end as authority_ratio
    from pagerank
    left join authority
        on
            pagerank.domain = authority.domain
            and pagerank.crawl = authority.crawl

)

select * from final

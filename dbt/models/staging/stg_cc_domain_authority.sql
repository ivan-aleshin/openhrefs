with source as (

    select * from {{ source('raw_cc', 'cc_domain_authority') }}

)

, final as (

    select
        domain
        , crawl
        , cast(open_authority as double) as open_authority
        , cast(open_volume as double) as open_volume
    from source

)

select * from final

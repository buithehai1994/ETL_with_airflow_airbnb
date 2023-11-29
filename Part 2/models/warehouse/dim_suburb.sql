{{
    config(
        unique_key='lga_name'
    )
}}

select * from {{ ref('nsw_suburb_stg') }}
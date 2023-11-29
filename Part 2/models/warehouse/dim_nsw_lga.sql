{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('nsw_lga_stg') }}
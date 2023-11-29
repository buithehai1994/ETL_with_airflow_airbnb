{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('g01_stg') }}
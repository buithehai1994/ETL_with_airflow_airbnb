{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('g02_stg') }}
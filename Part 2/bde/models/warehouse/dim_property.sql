{{
    config(
        unique_key='unique_id'
    )
}}

select * from {{ ref('property_stg') }}
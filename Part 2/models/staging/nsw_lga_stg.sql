{{ config(
    unique_key='lga_code'
)}}

-- Staging model for nsw_lga
with
source as (
    
    select
        LGA_CODE,
        LGA_NAME
    from  {{ source('raw', 'nsw_lga_code') }}
    where lga_code>0
)

select*from source 


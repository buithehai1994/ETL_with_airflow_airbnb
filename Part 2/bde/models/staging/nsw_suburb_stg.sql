-- Define the staging model for nsw_lga_suburb data
{{ config(
    unique_key='lga_name'
)}}

-- Staging model for nsw_lga_suburb (2 columns)
with
source as (
    -- Replace with your source definition for the nsw_lga_suburb data
    select
        LGA_NAME as lga_name,
        SUBURB_NAME as suburb_name
    from {{ source('raw', 'nsw_lga_suburb') }}
)

-- Combine the renamed and unknown data
select * from source


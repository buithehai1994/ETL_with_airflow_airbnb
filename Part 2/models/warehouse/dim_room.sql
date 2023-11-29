{{
    config(
        unique_key='unique_id'
    )
}}

select * from {{ ref('room_stg') }}
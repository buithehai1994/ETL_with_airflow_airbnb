-- Define the staging model for G02 data
{{ config(
    unique_key='lga_code'
)}}

-- Staging model for G02 (All Columns)
with
source as (
    -- Replace with your source definition for the G02 data
    select * from {{ source('raw', 'census_g02') }}
),
stg_renamed as (
    select
        LGA_CODE_2016 as lga_code,
        coalesce(Median_age_persons, 0) as median_age_persons,
        coalesce(Median_mortgage_repay_monthly, 0) as median_mortgage_repay_monthly,
        coalesce(Median_tot_prsnl_inc_weekly, 0) as median_total_personal_income_weekly,
        coalesce(Median_rent_weekly, 0) as median_rent_weekly,
        coalesce(Median_tot_fam_inc_weekly, 0) as median_total_family_income_weekly,
        coalesce(Average_num_psns_per_bedroom, 0) as average_number_persons_per_bedroom,
        coalesce(Median_tot_hhd_inc_weekly, 0) as median_total_household_income_weekly,
        coalesce(Average_household_size, 0) as average_household_size
    from source
)
select * from stg_renamed

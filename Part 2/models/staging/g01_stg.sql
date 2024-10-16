{{ config(
    unique_key='lga_code'
)}}

with

source as (
   
    select * from {{ source('raw', 'census_g01') }}
),

stg_renamed as (
    select
        LGA_CODE_2016 as lga_code,
        coalesce(Tot_P_M, 0) as total_persons_male,
        coalesce(Tot_P_F, 0) as total_persons_female,
        coalesce( Tot_P_P , 0 )  as total_persons_total,
        coalesce( Age_0_4_yr_M , 0 )  as population_0_4_years_male,
        coalesce( Age_0_4_yr_F , 0 )  as population_0_4_years_female,
        coalesce( Age_0_4_yr_P , 0 )  as population_0_4_years_total,
        coalesce( Age_5_14_yr_M , 0 )  as population_5_14_years_male,
        coalesce( Age_5_14_yr_F , 0 )  as population_5_14_years_female,
        coalesce( Age_5_14_yr_P , 0 )  as population_5_14_years_total,
        coalesce( Age_15_19_yr_M , 0 )  as population_15_19_years_male,
        coalesce( Age_15_19_yr_F , 0 )  as population_15_19_years_female,
        coalesce( Age_15_19_yr_P , 0 )  as population_15_19_years_total,
        coalesce( Age_20_24_yr_M , 0 )  as population_20_24_years_male,
        coalesce( Age_20_24_yr_F , 0 )  as population_20_24_years_female,
        coalesce( Age_20_24_yr_P , 0 )  as population_20_24_years_total,
        coalesce( Age_25_34_yr_M , 0 )  as population_25_34_years_male,
        coalesce( Age_25_34_yr_F , 0 )  as population_25_34_years_female,
        coalesce( Age_25_34_yr_P , 0 )  as population_25_34_years_total,
        coalesce( Age_35_44_yr_M , 0 )  as population_35_44_years_male,
        coalesce( Age_35_44_yr_F , 0 )  as population_35_44_years_female,
        coalesce( Age_35_44_yr_P , 0 )  as population_35_44_years_total,
        coalesce( Age_45_54_yr_M , 0 )  as population_45_54_years_male,
        coalesce( Age_45_54_yr_F , 0 )  as population_45_54_years_female,
        coalesce( Age_45_54_yr_P , 0 )  as population_45_54_years_total,
        coalesce( Age_55_64_yr_M , 0 )  as population_55_64_years_male,
        coalesce( Age_55_64_yr_F , 0 )  as population_55_64_years_female,
        coalesce( Age_55_64_yr_P , 0 )  as population_55_64_years_total,
        coalesce( Age_65_74_yr_M , 0 )  as population_65_74_years_male,
        coalesce( Age_65_74_yr_F , 0 )  as population_65_74_years_female,
        coalesce( Age_65_74_yr_P , 0 )  as population_65_74_years_total,
        coalesce( Age_75_84_yr_M , 0 )  as population_75_84_years_male,
        coalesce( Age_75_84_yr_F , 0 )  as population_75_84_years_female,
        coalesce( Age_75_84_yr_P , 0 )  as population_75_84_years_total,
        coalesce( Age_85ov_M , 0 )  as population_statistics_aged_85_and_over_male,
        coalesce( Age_85ov_F , 0 )  as population_statistics_aged_85_and_over_female,
        coalesce( Age_85ov_P , 0 )  as population_statistics_aged_85_and_over_total,
        coalesce( Counted_Census_Night_home_M , 0 )  as  Counted_Census_Night_home_M,
        coalesce( Counted_Census_Night_home_F , 0 )  as Counted_Census_Night_home_F,
        coalesce( Counted_Census_Night_home_P , 0 )  as Counted_Census_Night_home_P,
        coalesce( Count_Census_Nt_Ewhere_Aust_M , 0 )  as Count_Census_Nt_Ewhere_Aust_M ,
        coalesce( Count_Census_Nt_Ewhere_Aust_F , 0 )  as Count_Census_Nt_Ewhere_Aust_F,
        coalesce( Count_Census_Nt_Ewhere_Aust_P , 0 )  as Count_Census_Nt_Ewhere_Aust_P,
        coalesce( Indigenous_psns_Aboriginal_M , 0 )  as Indigenous_psns_Aboriginal_M,
        coalesce( Indigenous_psns_Aboriginal_F , 0 )  as Indigenous_psns_Aboriginal_F,
        coalesce( Indigenous_psns_Aboriginal_P , 0 )  as Indigenous_psns_Aboriginal_P,
        coalesce( Indig_psns_Torres_Strait_Is_M , 0 )  as Indig_psns_Torres_Strait_Is_M,
        coalesce( Indig_psns_Torres_Strait_Is_F , 0 )  as Indig_psns_Torres_Strait_Is_F,
        coalesce( Indig_psns_Torres_Strait_Is_P , 0 )  as Indig_psns_Torres_Strait_Is_P,
        coalesce( Indig_Bth_Abor_Torres_St_Is_M , 0 )  as indigenous_birthplace_aboriginal_torres_strait_islander_male,
        coalesce( Indig_Bth_Abor_Torres_St_Is_F , 0 )  as indigenous_birthplace_aboriginal_torres_strait_islander_female,
        coalesce( Indig_Bth_Abor_Torres_St_Is_P , 0 )  as indigenous_birthplace_aboriginal_torres_strait_islander_total,
        coalesce( Indigenous_P_Tot_M , 0 )  as indigenous_population_total_male,
        coalesce( Indigenous_P_Tot_F , 0 )  as indigenous_population_total_female,
        coalesce( Indigenous_P_Tot_P , 0 )  as indigenous_population_total,
        coalesce( Birthplace_Australia_M , 0 )  as birthplace_australia_male,
        coalesce( Birthplace_Australia_F , 0 )  as birthplace_australia_female,
        coalesce( Birthplace_Australia_P , 0 )  as birthplace_australia_total,
        coalesce( Birthplace_Elsewhere_M , 0 )  as birthplace_elsewhere_male,
        coalesce( Birthplace_Elsewhere_F , 0 )  as birthplace_elsewhere_female,
        coalesce( Birthplace_Elsewhere_P , 0 )  as birthplace_elsewhere_total,
        coalesce( Lang_spoken_home_Eng_only_M , 0 )  as language_spoken_at_home_english_only_male,
        coalesce( Lang_spoken_home_Eng_only_F , 0 )  as language_spoken_at_home_english_only_female,
        coalesce( Lang_spoken_home_Eng_only_P , 0 )  as language_spoken_at_home_english_only_total,
        coalesce( Lang_spoken_home_Oth_Lang_M , 0 )  as language_spoken_at_home_other_languages_male,
        coalesce( Lang_spoken_home_Oth_Lang_F , 0 )  as language_spoken_at_home_other_languages_female,
        coalesce( Lang_spoken_home_Oth_Lang_P , 0 )  as language_spoken_at_home_other_languages_total,
        coalesce( Australian_citizen_M , 0 )  as australian_citizen_male,
        coalesce( Australian_citizen_F , 0 )  as australian_citizen_female,
        coalesce( Australian_citizen_P , 0 )  as australian_citizen_total,
        coalesce( Age_psns_att_educ_inst_0_4_M , 0 )  as age_attending_educational_institution_0_4_years_male,
        coalesce( Age_psns_att_educ_inst_0_4_F , 0 )  as age_attending_educational_institution_0_4_years_female,
        coalesce( Age_psns_att_educ_inst_0_4_P , 0 )  as age_attending_educational_institution_0_4_years_total,
        coalesce( Age_psns_att_educ_inst_5_14_M , 0 )  as age_attending_educational_institution_5_14_years_male,
        coalesce( Age_psns_att_educ_inst_5_14_F , 0 )  as age_attending_educational_institution_5_14_years_female,
        coalesce( Age_psns_att_educ_inst_5_14_P , 0 )  as age_attending_educational_institution_5_14_years_total,
        coalesce( Age_psns_att_edu_inst_15_19_M , 0 )  as age_attending_educational_institution_15_19_years_male,
        coalesce( Age_psns_att_edu_inst_15_19_F , 0 )  as age_attending_educational_institution_15_19_years_female,
        coalesce( Age_psns_att_edu_inst_15_19_P , 0 )  as age_attending_educational_institution_15_19_years_total,
        coalesce( Age_psns_att_edu_inst_20_24_M , 0 )  as age_attending_educational_institution_20_24_years_male,
        coalesce( Age_psns_att_edu_inst_20_24_F , 0 )  as age_attending_educational_institution_20_24_years_female,
        coalesce( Age_psns_att_edu_inst_20_24_P , 0 )  as age_attending_educational_institution_20_24_years_total,
        coalesce( Age_psns_att_edu_inst_25_ov_M , 0 )  as age_attending_educational_institution_25_and_over_years_male,
        coalesce( Age_psns_att_edu_inst_25_ov_F , 0 )  as age_attending_educational_institution_25_and_over,
        coalesce( High_yr_schl_comp_Yr_12_eq_M , 0 )  as high_school_completion_year_12_eq_male,
        coalesce( High_yr_schl_comp_Yr_12_eq_F , 0 )  as high_school_completion_year_12_eq_female,
        coalesce( High_yr_schl_comp_Yr_12_eq_P , 0 )  as high_school_completion_year_12_eq_total,
        coalesce( High_yr_schl_comp_Yr_11_eq_M , 0 )  as high_school_completion_year_11_eq_male,
        coalesce( High_yr_schl_comp_Yr_11_eq_F , 0 )  as high_school_completion_year_11_eq_female,
        coalesce( High_yr_schl_comp_Yr_11_eq_P , 0 )  as high_school_completion_year_11_eq_total,
        coalesce( High_yr_schl_comp_Yr_10_eq_M , 0 )  as high_school_completion_year_10_eq_male,
        coalesce( High_yr_schl_comp_Yr_10_eq_F , 0 )  as high_school_completion_year_10_eq_female,
        coalesce( High_yr_schl_comp_Yr_10_eq_P , 0 )  as high_school_completion_year_10_eq_total,
        coalesce( High_yr_schl_comp_Yr_9_eq_M , 0 )  as high_school_completion_year_9_eq_male,
        coalesce( High_yr_schl_comp_Yr_9_eq_F , 0 )  as high_school_completion_year_9_eq_female,
        coalesce( High_yr_schl_comp_Yr_9_eq_P , 0 )  as high_school_completion_year_9_eq_total,
        coalesce( High_yr_schl_comp_Yr_8_belw_M , 0 )  as high_school_completion_year_8_belw_male,
        coalesce( High_yr_schl_comp_Yr_8_belw_F , 0 )  as high_school_completion_year_8_belw_female,
        coalesce( High_yr_schl_comp_Yr_8_belw_P , 0 )  as high_school_completion_year_8_belw_total,
        coalesce( High_yr_schl_comp_D_n_g_sch_M , 0 )  as high_school_completion_did_not_go_to_school_male,
        coalesce( High_yr_schl_comp_D_n_g_sch_F , 0 )  as high_school_completion_did_not_go_to_school_female,
        coalesce( High_yr_schl_comp_D_n_g_sch_P , 0 )  as high_school_completion_did_not_go_to_school_total,
        coalesce( Count_psns_occ_priv_dwgs_M , 0 )  as count_persons_occupied_private_dwellings_male,
        coalesce( Count_psns_occ_priv_dwgs_F , 0 )  as count_persons_occupied_private_dwellings_female,
        coalesce( Count_psns_occ_priv_dwgs_P , 0 )  as count_persons_occupied_private_dwellings_total,
        coalesce( Count_Persons_other_dwgs_M , 0 )  as count_persons_other_dwellings_male,
        coalesce( Count_Persons_other_dwgs_F , 0 )  as count_persons_other_dwellings_female,
        coalesce( Count_Persons_other_dwgs_P , 0 )  as count_persons_other_dwellings_total
    from source
),

stg_unknown as (
    select 
        'UNKNOWN' as lga_code,
        0 as total_persons_male,
    0 as total_persons_female,
    0 as total_persons_total,
    0 as population_0_4_years_male,
    0 as population_0_4_years_female,
    0 as population_0_4_years_total,
    0 as population_5_14_years_male,
    0 as population_5_14_years_female,
    0 as population_5_14_years_total,
    0 as population_15_19_years_male,
    0 as population_15_19_years_female,
    0 as population_15_19_years_total,
    0 as population_20_24_years_male,
    0 as population_20_24_years_female,
    0 as population_20_24_years_total,
    0 as population_25_34_years_male,
    0 as population_25_34_years_female,
    0 as population_25_34_years_total,
    0 as population_35_44_years_male,
    0 as population_35_44_years_female,
    0 as population_35_44_years_total,
    0 as population_45_54_years_male,
    0 as population_45_54_years_female,
    0 as population_45_54_years_total,
    0 as population_55_64_years_male,
    0 as population_55_64_years_female,
    0 as population_55_64_years_total,
    0 as population_65_74_years_male,
    0 as population_65_74_years_female,
    0 as population_65_74_years_total,
    0 as population_75_84_years_male,
    0 as population_75_84_years_female,
    0 as population_75_84_years_total,
    0 as population_statistics_aged_85_and_over_male,
    0 as population_statistics_aged_85_and_over_female,
    0 as population_statistics_aged_85_and_over_total,
    0 as Counted_Census_Night_home_M,
    0 as Counted_Census_Night_home_F,
    0 as Counted_Census_Night_home_P,
    0 as Count_Census_Nt_Ewhere_Aust_M ,
    0 as Count_Census_Nt_Ewhere_Aust_F,
    0 as Count_Census_Nt_Ewhere_Aust_P,
    0 as Indigenous_psns_Aboriginal_M,
    0 as Indigenous_psns_Aboriginal_F,
    0 as Indigenous_psns_Aboriginal_P,
    0 as Indig_psns_Torres_Strait_Is_M,
    0 as Indig_psns_Torres_Strait_Is_F,
    0 as Indig_psns_Torres_Strait_Is_P,
    0 as indigenous_birthplace_aboriginal_torres_strait_islander_male,
    0 as indigenous_birthplace_aboriginal_torres_strait_islander_female,
    0 as indigenous_birthplace_aboriginal_torres_strait_islander_total,
    0 as indigenous_population_total_male,
    0 as indigenous_population_total_female,
    0 as indigenous_population_total,
    0 as birthplace_australia_male,
    0 as birthplace_australia_female,
    0 as birthplace_australia_total,
    0 as birthplace_elsewhere_male,
    0 as birthplace_elsewhere_female,
    0 as birthplace_elsewhere_total,
    0 as language_spoken_at_home_english_only_male,
    0 as language_spoken_at_home_english_only_female,
    0 as language_spoken_at_home_english_only_total,
    0 as language_spoken_at_home_other_languages_male,
    0 as language_spoken_at_home_other_languages_female,
    0 as language_spoken_at_home_other_languages_total,
    0 as australian_citizen_male,
    0 as australian_citizen_female,
    0 as australian_citizen_total,
    0 as age_attending_educational_institution_0_4_years_male,
    0 as age_attending_educational_institution_0_4_years_female,
    0 as age_attending_educational_institution_0_4_years_total,
    0 as age_attending_educational_institution_5_14_years_male,
    0 as age_attending_educational_institution_5_14_years_female,
    0 as age_attending_educational_institution_5_14_years_total,
    0 as age_attending_educational_institution_15_19_years_male,
    0 as age_attending_educational_institution_15_19_years_female,
    0 as age_attending_educational_institution_15_19_years_total,
    0 as age_attending_educational_institution_20_24_years_male,
    0 as age_attending_educational_institution_20_24_years_female,
    0 as age_attending_educational_institution_20_24_years_total,
    0 as age_attending_educational_institution_25_and_over_years_male,
    0 as age_attending_educational_institution_25_and_over,
    0 as high_school_completion_year_12_eq_male,
    0 as high_school_completion_year_12_eq_female,
    0 as high_school_completion_year_12_eq_total,
    0 as high_school_completion_year_11_eq_male,
    0 as high_school_completion_year_11_eq_female,
    0 as high_school_completion_year_11_eq_total,
    0 as high_school_completion_year_10_eq_male,
    0 as high_school_completion_year_10_eq_female,
    0 as high_school_completion_year_10_eq_total,
    0 as high_school_completion_year_9_eq_male,
    0 as high_school_completion_year_9_eq_female,
    0 as high_school_completion_year_9_eq_total,
    0 as high_school_completion_year_8_belw_male,
    0 as high_school_completion_year_8_belw_female,
    0 as high_school_completion_year_8_belw_total,
    0 as high_school_completion_did_not_go_to_school_male,
    0 as high_school_completion_did_not_go_to_school_female,
    0 as high_school_completion_did_not_go_to_school_total,
    0 as count_persons_occupied_private_dwellings_male,
    0 as count_persons_occupied_private_dwellings_female,
    0 as count_persons_occupied_private_dwellings_total,
    0 as count_persons_other_dwellings_male,
    0 as count_persons_other_dwellings_female,
    0 as count_persons_other_dwellings_total

     
)

select*from (
    select * from stg_unknown
    union all 
    select * from stg_renamed
) as final_data
where total_persons_female>0
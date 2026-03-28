COPY (
    SELECT 
    
        --expnyr, -- use if > 2012
        qredate, -- use if <2012
        
        newid,"year",year_mo,ucc,popwt, age, age_ref, bls_urbn, cost, "date", date_length,
        educ_ref,finlwt21,"i.data", "i.year", "i.year_mo", income_corrected, income_decile,
        income_equivalized, income_group, income_percentile, income_percentile_corrected, 
        income_percentile_equivalized, income_quintile, income_quintile_corrected, income_quintile_equivalized,
        income_raw, incomey1, incomey2, interview_year, interview_yearq, marital1, num_days,owners_rent,
        primary_rent, psu, pubflag, pub_flag, race, ref_date, ref_race, ref_year, region, secondary_rent, single_race, 
        single_race_households, "state", state_geographical, state_name, urban 
    FROM read_csv_auto('/path/to/your_project/ucc_spending1999.csv')
)
TO '/path/to/your_project/ucc_spending1999_light.csv' 
(FORMAT 'CSV', HEADER);
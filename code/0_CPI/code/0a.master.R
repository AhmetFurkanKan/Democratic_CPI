#This script:
#       -is the master file for the D-CPI project.

#Inputs:
#      - R Scripts:
#            - 0b.utils.R: defines basic functions used throughout the analysis;
#            - 1.yearly_item_spending_by_group.R: includes functions to calculate yearly spending and shares by group and item;
#            - 2.make_us_shares.R: provides functions to generate shares for different price indexes in the US;
#            - 3.make_price_series.R: provides functions to create the price series.

#Outputs:
#     - Final price series  saved in the directory: output/US/group_urban_name_level

#0. Set up your working environment:
  ##a. Packages:
    # Load required packages. Install them if not already installed.
packages <- c(
  "data.table", "dplyr", "stringr", "lubridate"
)

    # Install missing packages
install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
    
  }
}

    # Load packages
lapply(packages, install_if_missing)


  ##b. Paths:
    # Set  your working directory below:
    # Replace the path, "sharing_path" with your working directory.
  # Set your project root to match 00_master.R
project_root <- "path/to/your/project/"         # <<< Supervisor edits this

# 0_CPI base path
sharing_path <- paste0(project_root, "code/0_CPI/")

# Point to the RAW CEX data
raw_cex_dir <- paste0(project_root, "data/CEX_light/")

sharing_data_raw_path<-paste0(sharing_path, "processed_bls_series/")
sharing_data_process_path<-paste0(sharing_path, "processed/")
sharing_data_output_path<-paste0(sharing_path, "output/")
sharing_code_path<-paste0(sharing_path, "code/")
sharing_intermediate_base_path<-paste0(sharing_path, "intermediate/")
setwd(sharing_path)

  #c. Load the basic functions:
source(paste0(sharing_code_path , "0b.utils.R"))

  #d. Make necessary directories:
make_dir(sharing_data_process_path)
make_dir(sharing_data_output_path)
make_dir(sharing_intermediate_base_path)

  #e. Set constants:
    # Change "first_cpi_year" and "last_cpi_year" to dynamically select the first and last year of your price series data.
first_cpi_year <- 2002
first_us_cpi_year <- first_cpi_year-1
last_cpi_year <- 2023
last_cex_year <- 2021
first_cex_year<-1999


#1. Define your group of interest.
    ##a. Set your preferences for data cleaning:
      # Specify whether to include only urban areas or all US data.
      # Set `only_urban` to TRUE to include only urban areas, or FALSE to include the entire US.
only_urban<-FALSE


if (only_urban==FALSE){
  urban_name<-"all"
}else{
  urban_name<-"urban"
}

    ##b. Define your group of interest:
      # Create your own group below by modifying the code in Part A.
      # Use variables available in `fmli` and `fmld`.
      # (Refer to the variable dictionary from the Bureau of Labor Statistics (BLS), saved in "Documentation".)

ucc_spending_cleaning<-function(current_year, only_urban=FALSE){
  
  #Load the required datasets from the RAW CEX directory
  ucc_spending_db<-fread(paste0(raw_cex_dir, sprintf("ucc_spending%s_light.csv", current_year)), colClasses = c(ucc = "character"))
  
  #FMLI and FMLD grouping
  if (only_urban==TRUE){
    ucc_spending_db <- ucc_spending_db[urban == "Urban"]
  } 
  
  fmli <- ucc_spending_db %>%
    filter(i.data == "fmli")
  
  fmld <- ucc_spending_db %>%
    filter(i.data == "fmld")
  
  
  #Clean RAM:
  rm(ucc_spending_db)
  
  #A. Define your group from variables available in fmli and fmld or predefined group (e.g. income_quintile). 
  group<-"income_quintile"#"income_group"
  
  
  #Now, you have defined your group variable. You can close the function.
  
  
  #Clean the RAM by only keeping variables of interests:
  fmli<-fmli[, c("newid", "primary_rent", "secondary_rent", "psu", "finlwt21", "owners_rent", group, "interview_yearq", "date", "cost", "popwt", "year_mo", "ucc"), with = FALSE]
  if (current_year < 2012){
    fmld<-fmld[,c("newid", "cost", "ucc", "qredate", "pub_flag", "ref_date", "date_length", "ref_year", "popwt", "psu", group, "interview_yearq", "year_mo", "num_days"), with = FALSE]
  }else{
    fmld<-fmld[,c("newid", "cost", "ucc", "expnyr", "pub_flag", "ref_date", "date_length", "ref_year", "popwt", "psu", group, "interview_yearq", "year_mo", "num_days"), with = FALSE]
  }
  
  #Process the data (automatically done)
  fmli_panel <- fmli[, c("newid", "primary_rent", "secondary_rent", "psu", "finlwt21", "owners_rent", group, "interview_yearq", "date"), with = FALSE]
  
  fmli_panel <- fmli_panel[!duplicated(fmli_panel), ]
  all_dates <- do.call(c, lapply(fmli_panel[, date], function(x) {
    seq(x, length = 3, by = "-1 month")
  }))
  
  newids <- rep(fmli_panel[, newid], each = 3)
  newid_panel <- data.table(date = all_dates, newid = newids)
  newid_panel <- newid_panel[year(date) == current_year]
  
  fmli_panel <- newid_panel[fmli_panel, on = .(newid)]
  
  primary_rent_expend_grouped <- fmli_panel[,
                                            .(
                                              tot_expend = sum(owners_rent * finlwt21, na.rm = TRUE),
                                              expend = sum(owners_rent * finlwt21, na.rm = TRUE) / sum(finlwt21, na.rm = TRUE),
                                              group_popwt = sum(finlwt21, na.rm = TRUE)
                                            ),
                                            by = c("interview_yearq", "date", "psu", group)
  ]
  primary_rent_expend_grouped[, ucc := "910104"] 
  
  secondary_rent_expend_grouped <- fmli_panel[,
                                              .(
                                                tot_expend = sum(secondary_rent * finlwt21, na.rm = TRUE),
                                                expend = sum(secondary_rent * finlwt21, na.rm = TRUE) / sum(finlwt21, na.rm = TRUE),
                                                group_popwt = sum(finlwt21, na.rm = TRUE)
                                              ),
                                              by = c("interview_yearq", "date", "psu", group)
  ]
  
  
  secondary_rent_expend_grouped[, ucc := "910105"] 
  
  rent_expend_grouped <- rbind(primary_rent_expend_grouped, secondary_rent_expend_grouped)
  rent_expend_grouped[, year_mo := paste0(year(date), str_pad(month(date), 2, pad = "0"))]
  rent_expend_grouped[, year := current_year]
  rent_expend_grouped <- rent_expend_grouped[, !"date"]
  
  

  wt_cols <- c(group, "psu", "interview_yearq", "year_mo", "group_popwt")
  group_wts_fmli <- rent_expend_grouped[ucc == "910104", ..wt_cols]
  
  interview_ucc_expend <- fmli[,
                               .(tot_expend = sum(cost * popwt)),
                               by = c("interview_yearq", "year_mo", "ucc", "psu", group)
  ]
  
  group_wts_fmli$year_mo<-as.numeric(group_wts_fmli$year_mo)
  interview_ucc_expend <- interview_ucc_expend[group_wts_fmli,
                                               on = c(group, "psu", "interview_yearq", "year_mo"), nomatch = NULL
  ] %>%
    mutate(
      expend = tot_expend / group_popwt,
      year = current_year
    )
  
  interview_ucc_expend <- rbind(interview_ucc_expend, rent_expend_grouped)
  
  
  if (current_year<2012){
    fmld_exp <- fmld[, !c("cost", "ucc", "qredate", "pub_flag", "ref_date", "date_length", "ref_year"), with = FALSE]
  }else{
    fmld_exp <- fmld[, !c("cost", "ucc", "expnyr", "pub_flag", "ref_date", "date_length", "ref_year"), with = FALSE]
  }
  
  fmld_exp <- fmld_exp[!duplicated(fmld_exp),]

  
  group_wts_fmld <- fmld_exp[, .(group_popwt = sum(popwt, na.rm=TRUE)), by = c(
    group, "psu",
    "interview_yearq", "year_mo"
  )]
  
  
  diary_ucc_expend <- fmld[, .(tot_expend = sum(cost * popwt, na.rm = TRUE ) * max(num_days) / 7),
                           by = c(group, "psu", "ucc", "interview_yearq", "year_mo")
  ]
  
  diary_ucc_expend <- diary_ucc_expend[group_wts_fmld,
                                       on = c(group, "psu", "interview_yearq", "year_mo")
  ]
  
  diary_ucc_expend[, expend := tot_expend / group_popwt]
  
  diary_ucc_expend <- diary_ucc_expend[!(ucc=="")] 
  
  diary_ucc_expend[, year:=current_year]
  
  interview_ucc_expend[, "ucc_source" := "I"]
  diary_ucc_expend[, "ucc_source" := "D"]
  all_ucc_expend <- rbind(interview_ucc_expend, diary_ucc_expend)
  
  avg_all_expend <- all_ucc_expend[, .(
    group_popwt = sum(group_popwt),
    expend = sum(tot_expend) / sum(group_popwt),
    tot_expend = sum(tot_expend)
  ),
  by = .(psu, year, year_mo, interview_yearq, ucc, ucc_source)
  ]
  avg_all_expend[, (group) := 0]
  
  all_ucc_expend <- rbind(all_ucc_expend, avg_all_expend)
  

  group_wts_fmli[, ucc_source := "I"]
  group_wts_fmld[, ucc_source := "D"]
  all_wts <- copy(bind_rows(group_wts_fmli, group_wts_fmld))
  all_wts[, year := current_year]
  
  combined_wts <- all_wts[, .(
    group_popwt = sum(group_popwt)
  ), by = .(psu, year, ucc_source, interview_yearq, year_mo)]
  combined_wts[, (group) := 0]
  
  all_wts <- rbind(all_wts, combined_wts)
  

  all_ucc_expend[, expend_scaled := expend]
  
  base_group_path <- paste0(sharing_data_process_path, group, "_", urban_name, "_level/")
  group_output_path <- paste0(base_group_path, "CEX Spending/")
  make_dir(base_group_path)
  make_dir(group_output_path)
  
  fwrite(
    all_ucc_expend,
    paste0(group_output_path, "ucc_spending_", current_year, ".csv")
  )
  
  fwrite(
    all_wts,
    paste0(group_output_path, "all_monthly_weights_", current_year, ".csv")
  )
  rm(fmli)
  rm(fmld)
  rm(fmli_panel)
  rm(all_ucc_expend)
  rm(all_wts)
}

      # For the rest of the analysis, indicate:
      # The name of your group (as defined above, in ucc_spending_cleaning):
group <- "income_quintile"#"income_group"

    ##c. Generate all the ucc expenditures from the CEX Diary and Interview databases grouped by categories:
for (current_year in first_cex_year:last_cex_year) {
  print(current_year)
  ucc_spending_cleaning(current_year)
}


#2. Get the yearly item spending per items x group :
    ##a. Load the necessary functions and price series:
source(paste0(sharing_code_path, "1.yearly_item_spending_by_group.R"))

    ##b. At the US Level
get_yearly_item_spending(group, urban_name)


#3. Make US Weights Series:
      # If you have a high number of subgroups, you may face a RAM issue to generate the series.
      # This code was run on a 32GB RAM computer.

    ##a. Load the necessary functions:
print("Making US RI Series")
source(paste0(sharing_code_path, "2.make_us_shares.r"))

    ##b. CPI:
print("---CPI")
make_cpi_ri_series(group, urban_name)

    ##c. CES:
print("---CES")
make_ces_ri_series(group, urban_name)

    ##d. Laspeyres:
print("---Monthly Laspeyres")
make_monthly_ri_series(group, urban_name, type = "laspeyres")

    ##e. Paasche:
print("---Monthly Paasche")
make_monthly_ri_series(group, urban_name, type = "paasche")

    ##f. Tornqvist:
print("---Tornqvist")
make_tornqvist_ri_series(group, urban_name)


#4. Make US Price Series:
      # If you have a high number of subgroups, you may face a RAM issue to generate the price series.
      # This code was run on a 32GB RAM computer.

    ##a. Load the necessary functions:
print("Making US Price Series")
source(paste0(sharing_code_path, "3.make_price_series.r"))

    ##b. CPI:
print("---CPI")
make_cpi_price_series(group, first_cpi_year, urban_name)

    ##c. Laspeyres:
print("---Monthly Laspeyres")
make_monthly_price_series(group, first_cpi_year, urban_name, type = "laspeyres")

    ##d. Paasche:
print("---Monthly Paasche")
make_monthly_price_series(group, first_cpi_year, urban_name, type = "paasche")

    ##e. Tornqvist:
print("---Tornqvist")
make_tornqvist_price_series(group, first_cpi_year, urban_name)

    ##f. CES:
print("---CES")
make_ces_price_series(group, first_cpi_year, urban_name)

    ##g. Chained CPI:
print("---Chained CPI")
make_chained_cpi(group, urban_name)

    ##h. Wide Dataset :
print("Making US Wide Dataset")
all_wide_series <- c("cpi", "monthly_laspeyres", "monthly_paasche", "tornqvist", "ces_0.6")
for (ri_series_name in all_wide_series) {
  get_wide_data(group, urban_name, ri_series_name)
}

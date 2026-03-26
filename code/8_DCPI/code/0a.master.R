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
# Replace the path, "sharing_path" with your working directory.s
##b. Paths:
# Set your project root to match 00_master.R
project_root <- "path/to/your/project/"         

# 8_DCPI base path
sharing_path <- paste0(project_root, "code/8_DCPI/")

# Point to the FUSED CEX data created by Step 5 in 00_master.R
fused_cex_dir <- paste0(project_root, "intermediate/fused_CEX/")

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
  
  #Load the required datasets from the FUSED directory (Step 5 output)
  ucc_spending_db<-fread(paste0(fused_cex_dir, sprintf("ucc_spending%s.csv", current_year)), colClasses = c(ucc = "character"))
  
  #FMLI and FMLD grouping
  if (only_urban==TRUE){
    ucc_spending_db <- ucc_spending_db[urban == "Urban"]
  } 
  
  fmli <- ucc_spending_db %>%
    filter(i.data == "fmli")
  
  fmld <- ucc_spending_db %>%
    filter(i.data == "fmld")
  
  has_diary <- nrow(fmld) > 0
  
  #Clean RAM:
  rm(ucc_spending_db)
  
  #A. Define your group from variables available in fmli and fmld or predefined group (e.g. income_quintile). 
  group<- "income_quintile"#"income_group"
  
  
  #Now, you have defined your group variable. You can close the function.
  
  
  #Clean the RAM by only keeping variables of interests:
  fmli<-fmli[, c("newid", "primary_rent", "secondary_rent", "psu", "finlwt21", "owners_rent", group, "interview_yearq", "date", "cost", "popwt", "year_mo", "ucc"), with = FALSE]
  # Assert finlwt21 == popwt (democratic weighting relies on this equality)
  n_mismatch <- sum(fmli$finlwt21 != fmli$popwt, na.rm = TRUE)
  if (n_mismatch > 0) warning(paste(n_mismatch, "rows have finlwt21 != popwt in year", current_year))
  
  if (current_year < 2012){
    fmld<-fmld[,c("newid", "cost", "ucc", "qredate", "pub_flag", "ref_date", "date_length", "ref_year", "popwt", "psu", group, "interview_yearq", "year_mo", "num_days"), with = FALSE]
  }else{
    fmld<-fmld[,c("newid", "cost", "ucc", "expnyr", "pub_flag", "ref_date", "date_length", "ref_year", "popwt", "psu", group, "interview_yearq", "year_mo", "num_days"), with = FALSE]
  }
  
  # -------------------------------------------------------------
  # DEMOCRATIC CPI TRANSFORMATION: Convert expenditures to household shares
  
  # 1. Calculate Monthly Interview Totals (fmli)
  fmli_tot <- fmli[, .(
    sum_cost_fmli = sum(cost, na.rm = TRUE),
    owners_rent = first(owners_rent),
    secondary_rent = first(secondary_rent)
  ), by = newid]
  
  fmli_tot[is.na(owners_rent),    owners_rent    := 0]
  fmli_tot[is.na(secondary_rent), secondary_rent := 0]
  
  # Convert the quarterly fmli total to a monthly basis
  fmli_tot[, fmli_monthly_expend := (sum_cost_fmli + owners_rent + secondary_rent) / 3]
  
  # 2. Calculate Monthly Diary Totals (fmld)
  # (Assuming your fused diary 'cost' is already aggregated to a true monthly value)
  has_diary <- nrow(fmld) > 0
  if (has_diary) {
    fmld_diary_tot <- fmld[, .(
      fmld_monthly_expend = sum(cost * (max(num_days) / 7), na.rm = TRUE)
    ), by = newid]
  } else {
    fmld_diary_tot <- data.table(newid = character(), fmld_monthly_expend = numeric())
  }
  
  # 3. Create a Unified Total Monthly Expenditure per Household
  unified_tot <- merge(fmli_tot, fmld_diary_tot, by = "newid", all = TRUE)
  unified_tot[is.na(fmli_monthly_expend), fmli_monthly_expend := 0]
  unified_tot[is.na(fmld_monthly_expend), fmld_monthly_expend := 0]
  
  unified_tot[, true_hh_tot_expend_monthly := fmli_monthly_expend + fmld_monthly_expend]
  n_zero <- sum(unified_tot$true_hh_tot_expend_monthly <= 0, na.rm = TRUE)
  if (n_zero > 0) warning(paste(n_zero, "household(s) with zero/negative total monthly spending dropped in year", current_year))
  unified_tot <- unified_tot[true_hh_tot_expend_monthly > 0]
  
  # 4. Convert absolute dollars to budget shares using the UNIFIED total
  
  # A. Interview Shares (fmli is quarterly, so divide by True Monthly Total * 3)
  fmli <- fmli[unified_tot[, .(newid, true_hh_tot_expend_monthly)], on = "newid"]
  fmli[, cost          := (cost/3)           / (true_hh_tot_expend_monthly )]
  fmli[, owners_rent   := (owners_rent/3)    / (true_hh_tot_expend_monthly )]
  fmli[, secondary_rent := (secondary_rent/3) / (true_hh_tot_expend_monthly )]
  
  # Validation: Interview share check (should be < 1, as remainder is in Diary)
  share_check_fmli <- fmli[, .(
    ucc_share_sum        = sum(cost, na.rm = TRUE),
    owners_rent_share    = first(owners_rent),
    secondary_rent_share = first(secondary_rent)
  ), by = newid]
  
  # B. Diary Shares (fmld is monthly, so divide directly by True Monthly Total)
  if (has_diary) {
    fmld <- fmld[unified_tot[, .(newid, true_hh_tot_expend_monthly)], on = "newid"]
    fmld[, cost :=(cost* (max(num_days) / 7)) / true_hh_tot_expend_monthly]
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
                                              tot_expend = sum((owners_rent ) * finlwt21, na.rm = TRUE),
                                              expend = sum((owners_rent ) * finlwt21, na.rm = TRUE) / sum(finlwt21, na.rm = TRUE),
                                              group_popwt = sum(finlwt21, na.rm = TRUE)
                                            ),
                                            by = c("interview_yearq", "date", "psu", group)
  ]
  primary_rent_expend_grouped[, ucc := "910104"] 
  
  secondary_rent_expend_grouped <- fmli_panel[,
                                              .(
                                                tot_expend = sum((secondary_rent ) * finlwt21, na.rm = TRUE),
                                                expend = sum((secondary_rent ) * finlwt21, na.rm = TRUE) / sum(finlwt21, na.rm = TRUE),
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
  
  # 1. Merge the 3-month panel onto the full fmli dataset to distribute quarterly costs
  # Note: This gives us 3 rows per household, one for each month in their reference quarter.
  fmli_expanded <- newid_panel[fmli, on = "newid", allow.cartesian = TRUE]
  
  # 2. The 'cost' variable is a quarterly budget share, --so divide it by 3 for a monthly share
  fmli_expanded[, monthly_cost := cost]
  
  # 3. Create the correct monthly string (year_mo) from the newly expanded 'date' column
  fmli_expanded[, year_mo_expanded := paste0(year(date), str_pad(month(date), 2, pad = "0"))]
  
  # 4. Calculate interview_ucc_expend using the smoothed data and the new expanded dates
  interview_ucc_expend <- fmli_expanded[,
                                        .(tot_expend = sum(monthly_cost * popwt)),
                                        by = c("interview_yearq", "year_mo_expanded", "ucc", "psu", group)
  ]
  
  # 5. Rename year_mo_expanded back to year_mo to match your downstream weight merging
  setnames(interview_ucc_expend, "year_mo_expanded", "year_mo")
  
  # Ensure year_mo is numeric to match the transformation you apply to group_wts_fmli right below this
  interview_ucc_expend[, year_mo := as.numeric(year_mo)]
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
  
  
  interview_ucc_expend[, "ucc_source" := "I"]
  
  if (has_diary) {
    diary_ucc_expend <- fmld[, .(tot_expend = sum(cost * popwt, na.rm = TRUE)), # * 30 / max(num_days)),
                             by = c(group, "psu", "ucc", "interview_yearq", "year_mo")
    ]
    diary_ucc_expend <- diary_ucc_expend[group_wts_fmld,
                                         on = c(group, "psu", "interview_yearq", "year_mo")
    ]
    diary_ucc_expend[, expend := tot_expend / group_popwt]
    diary_ucc_expend <- diary_ucc_expend[!(ucc == "")]
    diary_ucc_expend[, year := current_year]
    diary_ucc_expend[, "ucc_source" := "D"]
    all_ucc_expend <- rbind(interview_ucc_expend, diary_ucc_expend)
  } else {
    all_ucc_expend <- interview_ucc_expend
  }
  
  
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
  
  if (has_diary) {
    group_wts_fmld[, ucc_source := "D"]
    all_wts <- copy(bind_rows(group_wts_fmli, group_wts_fmld))
  } else {
    all_wts <- copy(group_wts_fmli)
  }
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
make_chained_cpi(group, urban_name, first_cpi_year = first_cpi_year)

##h. Wide Dataset :
print("Making US Wide Dataset")
all_wide_series <- c("cpi", "monthly_laspeyres", "monthly_paasche", "tornqvist", "ces_0.6")
for (ri_series_name in all_wide_series) {
  get_wide_data(group, urban_name, ri_series_name)
}
#0.utils.R

#This script:
#      - provides basic functions used throughout the analysis.


#Inputs:
#      -  "ri_name_mapping.csv": mapping between item codes saved into "processed_bls_series";
#      -  "grouping.csv": importance of UCC across time saved into "processed_bls_series";
#      -  "xwalk_by_yearquarter.csv": crosswalk between UCC and Item codes saved into "processed_bls_series".



#1. Basic functions:
  ##a. Function to create a directory if it doesn't exist
make_dir <- function(path) {
  if (!file.exists(path)) {
    dir.create(path)
  }
}

  ##b. Function to add months to a Year-Month format (ym)
add_ym <- function(yms, num_months) {
  new_yms <- c()
  for (ym in yms) {
    year <- as.numeric(substr(ym, 1, 4))
    mo <- as.numeric(substr(ym, 5, 6))
    extra_years <- (num_months + mo - 1) %/% 12
    new_month <- (mo + num_months - 1) %% 12 + 1
    extra_years
    new_year <- year + extra_years
    new_ym <- paste0(new_year, str_pad(new_month, 2, pad = "0"))
    new_yms <- c(new_yms, new_ym)
  }
  return(new_yms)
}


  ##c. Function to standardize PSUs
map_psu <- function(psus, states) {
  new_psus <- c()
  for (i in seq_along(psus)) {
    psu <- psus[i]
    state <- states[i]
    if (is.na(psu) && !(state %in% c(2, 15))) {
      new_psu <- "OTHER"
    } else if (psu %in% names(psu_mapping)) {
      new_psu <- as.character(psu_mapping[as.character(psu)])
    } else if (is.na(state)) {
      new_psu <- "OTHER"
    } else if (state == 15) {
      new_psu <- "S49F"
    } else if (state == 2) {
      new_psu <- "S49G"
    } else {
      new_psu <- "OTHER"
    }
    new_psus <- c(new_psus, new_psu)
  }
  return(new_psus)
}


#2. Functions used to get a consistent nomenclature of products and geographical areas:
  ##a. Correspondence tables:
    ###i.between product names and product codes used in the official CPI
ri_names <- fread(paste0(sharing_data_raw_path, "ri_name_mapping.csv"))

    ###ii.Importance of UCC accrosss times
grouping<-fread(paste0(sharing_data_raw_path, "grouping.csv"))

    ###iii.Crosswalk between UCC and Item codes:
xwalk<-fread(paste0(sharing_data_raw_path, "xwalk_by_yearquarter.csv"), colClasses = list(character = "yearquarter", "ucc"))

  ##b. Definition of geographical areas:
northeast<-c("Maine", "New Hampshire", "Vermont", "Massachusetts", "Rhode Island", "Connecticut", "New York")
mid_atlantic<-c("New Jersey", "Pennsylvania", "Maryland", "Delaware", "District of Columbia", "West Virginia", "Virginia")
midwest<-c("Ohio", "Indiana", "Illinois", "Michigan", "Wisconsin", "Minnesota", "Iowa", "North Dakota", "South Dakota", "Nebraska")
southeast<-c("Kentucky", "Tennessee", "North Carolina", "South Carolina", "Georgia", "Florida", "Alabama", "Mississippi")
southwest<-c("New Mexico", "Arkansas", "Louisiana", "Texas", "Oklahoma")
mountain_plains<-c("Missouri", "Kansas", "Colorado", "Wyoming", "Montana", "Utah")
west<-c("Idaho", "Nevada", "Washington", "Oregon", "California", "Arizona", "Hawaii", "Alaska")

  ##c. Function generating the crosswalks:
combine_ucc_xwalk <- function(group, urban_name = "all") {

      ucc_base_path <- sprintf("%s%s_%s_level/CEX Spending/", sharing_data_process_path, group, urban_name)
      ucc_spending <- lapply(dir(ucc_base_path, pattern = "ucc_spending*", full.names = TRUE),
                             fread,
                             colClasses = c("ucc" = "character", "interview_yearq" = "character")
      ) %>%
        bind_rows()
      
      group_wts <- lapply(dir(ucc_base_path, pattern = "all_monthly_weights*", full.names = TRUE),
                          fread,
                          colClasses = c("interview_yearq" = "character")
      ) %>%
        bind_rows()
      
      yearly_weights <- group_wts[, .(group_popwt = sum(group_popwt)), by = c("ucc_source", "year", group)]
      yearly_local_weights <- group_wts[, .(group_popwt = sum(group_popwt)), by = c("ucc_source", "year", "psu", group)]
      monthly_weights <- group_wts[, .(group_popwt = sum(group_popwt)), by = c("ucc_source", "year", "year_mo", group)]
      
      last_cex_year <- max(ucc_spending[, year])
      
      ucc_spending <- ucc_spending[xwalk,
                                   on = c("interview_yearq" = "yearquarter", "ucc_source" = "int_diary", "ucc"),
                                   nomatch = NULL, allow.cartesian = TRUE
      ]
      colnames(ucc_spending)[colnames(ucc_spending) == "ItemCode"] <- "item_code"
      
  
  
  return(list(ucc_spending, yearly_weights, monthly_weights, yearly_local_weights))
}







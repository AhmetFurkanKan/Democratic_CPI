#2a.make_us_shares.R


#This script:
#       -generates the shares for different price indexes

#Inputs:
#       -item_price_series.csv: item price series saved into "processed_bls_series"
#       -ri_series.csv: yearly relative importance weights saved into "processed_bls_series"
#       -yearly_cex_ri.csv: yearly expenditures and shares per group x items saved into “intermediate/US/group_urban_name_level”
#       -item_ref_scales.csv: item price series rescaled saved into "processed_bls_series"

#Outputs:
#     saved into "intermediate/US/group_urban_name_level"
#     Weights for
#       - CPI: 
#             - Fixed shares adjusted by yearly expenditure weights prior to 2002: cpi_ri_series.csv 
#       - Paasche: 
#             - for years prior to 1999, set spending to the average for that year: monthly_paasche_ri_series.csv 
#       - Laspeyres:
#             - for years prior to 1999, set spending to the average for that year: monthly_laspeyres_ri_series.csv 
#       - CES (for different sigma, in this example, sigma=0.6)
#             - ces_0.6_ri_series.csv 
#       - Tornqvist 
#             - input: monthly_paasche_ri_series.csv
#             - tornqvist_ri_series.csv 


#1. Datasets and paths: 
output_prices_path <- paste0(sharing_data_raw_path, "item_price_series.csv")
us_prices_with_scale <- fread(output_prices_path,
                              colClasses = c("ym" = "character")
)

ym_mapping <- us_prices_with_scale[item_code == "SA0", .(ym, cpi_base_yr)]
colnames(ym_mapping) <- c("ym", "cpi_yr")


#2. Make shares:
    #a. CPI:
make_cpi_ri_series <- function(group, urban_name = "all", computation_method="Fixed shares") {
  first_cpi_us_ym <- paste0(first_us_cpi_year + 1, "01")
  intermediate_us_path <- paste0(sharing_intermediate_base_path, "US/", group, "_", urban_name, "_level/")
  
  cpi_yr_spending <- fread(paste0(intermediate_us_path, "yearly_cex_ri.csv"))
  ri_series <- fread(paste0(sharing_data_raw_path, "ri_series.csv"),
                     colClasses = c("ym" = "character")
  )
  
  prices <- fread(paste0(sharing_data_raw_path, "item_price_series.csv"),
                  colClasses = c("ym" = "character", "base_ref_ym" = "character")
  )
  
  price_ref_scale <- fread(paste0(sharing_data_raw_path, "item_ref_scales.csv"))
  
  cpi_yr_spending_scaled <- cpi_yr_spending[price_ref_scale,
                                            on = c(item_code_cpi = "item_code", "cpi_yr"), nomatch=NULL
  ]
  

  cpi_yr_spending_scaled[, `:=`(
    share = share * price_scale / sum(share * price_scale)
  ),
  by = c(group, "cpi_yr")
  ]
  
  
  # KEEP group == 0 (Do not filter it out)
  all_item_spending <- cpi_yr_spending_scaled
  
  all_item_spending[, base_ref_ym := as.character(ym)]
  
  item_ym_spending <- all_item_spending[, !"ym"][prices,
                                                 on = c("base_ref_ym", item_code_cpi = "item_code"),
                                                 allow.cartesian = TRUE, nomatch = NULL
  ]
  
  # Calculate raw relative importance (raw_ri) for all groups, including group 0
  item_ym_spending[, `:=`(
    raw_ri = share * cex_price_scale / sum(share * cex_price_scale)
  ),
  by = c(group, "ym")
  ]
  
  # Extract the true US Total (group == 0) to serve as the implied_tot_ri for BLS scaling
  us_totals <- item_ym_spending[get(group) == 0, .(cpi_yr, ym, item_code_cpi, implied_tot_ri = raw_ri)]
  
  # Merge the US Total implied_tot_ri back onto all subgroups
  item_ym_spending <- item_ym_spending[us_totals, on = c("cpi_yr", "ym", "item_code_cpi")]
  
  item_cols <- c(
    group, "item_code_cpi", "cpi_yr", "ym", "implied_tot_ri", "raw_ri"
  )
  
  item_ym_spending <- item_ym_spending[, ..item_cols]


  all_items <- unique(prices[, item_code])
  all_groups <- unique(item_ym_spending[, get(group)])
  all_yms <- unique(item_ym_spending[, ym])
  
  item_group_yms <- CJ(all_items, all_groups, all_yms)
  colnames(item_group_yms) <- c("item_code_cpi", group, "ym")
  
  item_ym_spending <- item_ym_spending[item_group_yms, on = c(group, "item_code_cpi", "ym")]
  item_ym_spending[, ym := as.character(ym)]
  

  ri_series_cpi <- ri_series[, .(ri = sum(ri)), by = .(ym, item_code_cpi)]
  
  group_ri_series <- item_ym_spending[ri_series_cpi,
                                      on = .(ym, item_code_cpi),
                                      allow.cartesian = TRUE,
                                      nomatch = NULL
  ]
  

  group_ri_series[is.na(raw_ri), raw_ri := 0]
  group_ri_series[is.na(implied_tot_ri), implied_tot_ri := 0]
  

  # Bypass the Plutocratic BLS adjustment to preserve the Democratic weights
  group_ri_series[, `:=`(
    implied_ri_gross = raw_ri
  )]
  
  group_ri_series[, `:=`(
    ri_cex = implied_ri_gross / sum(implied_ri_gross, na.rm = TRUE)
  ),
  by = c("ym", group)
  ]
  
  ri_cols <- c("ym", group, "item_code_cpi", "ri_cex", "raw_ri", "implied_tot_ri")
  group_ri_series <- group_ri_series[, ..ri_cols]
  
  

  #all_yms <- c(prices[ym < 200112, unique(ym)], as.numeric(paste0(first_us_cpi_year, "12")))
  

  all_yms <- c(prices[ym < 200200, unique(ym)])
  
  all_items <- prices[, unique(item_code)]
  all_groups <- group_ri_series[, unique(get(group))]
  old_ri_series <- CJ(ym = all_yms, item_code_cpi = all_items, group = all_groups)
  colnames(old_ri_series) <- c("ym", "item_code_cpi", group)
  
  
    old_shares <- group_ri_series[ym == 200112, !"ym"]
    old_ri_series <- old_ri_series[old_shares, on = c(group, "item_code_cpi")]
    group_ri_series <- bind_rows(old_ri_series, group_ri_series) %>% setDT()
    group_ri_series <- group_ri_series[ri_series, ,
                                       on = .(ym, item_code_cpi),
                                       allow.cartesian = TRUE,
                                       nomatch = NULL]
    
    # Bypass the Plutocratic BLS adjustment to preserve the Democratic weights
    group_ri_series[, `:=`(
      implied_ri_gross = raw_ri
    )]
    
    group_ri_series[, `:=`(
      ri_cex = implied_ri_gross / sum(implied_ri_gross, na.rm = TRUE)
    ),
    by = c("ym", group)
    ]
    
    group_ri_series<-unique(group_ri_series)
    
    
  
  
  
  group_ri_series[, date := as.Date(paste0(ym, "01"), format = "%Y%m%d")]
  group_ri_series <- group_ri_series[, !"ym"]
  
  
  setorder(prices, date)
  prices[, monthly_inflation := price / data.table::shift(price, 1), by = item_code]
  prices[, ref_date := date %m-% months(1)]
  prices <- prices[, .(ref_date, ym, date, item_code, monthly_inflation, price)]
  
  group_ri_series <- prices[group_ri_series, on = c(ref_date = "date", item_code = "item_code_cpi"), nomatch=NULL]
  group_ri_series[ym == first_cpi_us_ym, monthly_inflation := 1]
  
  
  names(group_ri_series)[names(group_ri_series) == 'item_code_cpi'] <- "item_code"
  
  output_cols <- c("ym", "date", group, "item_code", "ri_cex", "raw_ri", "monthly_inflation", "price", "implied_tot_ri")
  group_ri_series <- group_ri_series[, ..output_cols]
  
    fwrite(group_ri_series, paste0(sharing_intermediate_base_path, "US/", group, "_", urban_name, "_level/cpi_ri_series.csv"))

}

    #b. CES:
make_ces_ri_series <- function(group, urban_name = "urban", sigma = .6) {
    intermediate_us_path <- paste0(sharing_intermediate_base_path, "US/", group, "_", urban_name, "_level/")
    cpi_yr_spending <- fread(paste0(intermediate_us_path, "yearly_cex_ri.csv"))

    ri_series <- fread(paste0(sharing_data_raw_path, "yearly_ri_series.csv"),
        select = c("item_code_cpi", "year", "CPIU")
    )

    prices <- fread(paste0(sharing_data_raw_path, "item_price_series.csv"),
        select = c("date", "ym", "item_code", "cex_price_scale", "base_ref_ym", "price"),
        colClasses = c("ym" = "character")
    )
    price_ref_scale <- fread(paste0(sharing_data_raw_path, "item_ref_scales.csv"))


    cpi_yr_spending_scaled <- cpi_yr_spending[price_ref_scale,
        on = c(item_code_cpi = "item_code", "cpi_yr"), nomatch = NULL
    ]

    cpi_yr_spending_scaled[, `:=`(
        share = share * price_scale^(1 - sigma) / sum(share * price_scale^(1 - sigma))
    ),
    by = c(group, "cpi_yr")
    ]

    item_cols <- c(
        group, "item_code_cpi", "ym", "share"
    )

    cpi_yr_spending_scaled <- cpi_yr_spending_scaled[, ..item_cols]

    all_items <- unique(prices[, item_code])
    all_groups <- unique(cpi_yr_spending_scaled[, get(group)])
    all_years <- unique(cpi_yr_spending_scaled[, ym])

    item_group_years <- CJ(all_items, all_groups, all_years)
    colnames(item_group_years) <- c("item_code_cpi", group, "ym")

    cpi_yr_spending_scaled <- cpi_yr_spending_scaled[item_group_years, on = c(group, "item_code_cpi", "ym")]

    ri_series_cols <- c(group, "item_code_cpi", "ym", "share")
    group_ri_series <- cpi_yr_spending_scaled[, ..ri_series_cols]
    group_ri_series[is.na(share), share := 0]

    group_ri_series <- group_ri_series[prices[, !c("price", "ym")],
        on = c(item_code_cpi = "item_code", ym = "base_ref_ym"),
        allow.cartesian = TRUE, nomatch = NULL
    ]
    group_ri_series <- group_ri_series[, !"ym"]


    group_ri_series[, ri_cex := share * cex_price_scale^(1 - sigma) / sum(share * cex_price_scale^(1 - sigma)),
        by = c(group, "date")
    ]

    share_cols <- c(group, "item_code_cpi", "ri_cex", "date")
    group_ri_series <- group_ri_series[, ..share_cols]



    setorder(prices, date)
    prices[, monthly_inflation := price / data.table::shift(price, n = 1), by = item_code]
    prices[, ref_date := date %m-% months(1)]

    group_ri_series <- prices[, !c("cex_price_scale", "base_ref_ym", "ym")][group_ri_series,
        on = c(ref_date = "date", item_code = "item_code_cpi"),
        allow.cartesian = TRUE, nomatch = NULL
    ]

    group_ri_series[, monthly_inflation := monthly_inflation^(1 - sigma)]
    group_ri_series[, ym := paste0(year(date), str_pad(month(date), 2, pad = "0"))]

    output_cols <- c("ym", group, "item_code", "ri_cex", "monthly_inflation", "price")
    group_ri_series <- group_ri_series[, ..output_cols]

    fwrite(group_ri_series, paste0(intermediate_us_path, "ces_", sigma, "_ri_series.csv"))
}

    #c. Tornqvist:
make_tornqvist_ri_series <- function(group, urban_name = "urban") {
    intermediate_us_path <- paste0(sharing_intermediate_base_path, "US/", group, "_", urban_name, "_level/")

    monthly_cols <- c(group, "ym", "ri_cex", "item_code", "monthly_inflation", "price")
    monthly_shares <- fread(paste0(intermediate_us_path, "monthly_paasche_ri_series.csv"),
        select = monthly_cols
    )

    setorderv(monthly_shares, c(group, "item_code", "ym"))
    monthly_shares[, ri_cex_lag := data.table::shift(ri_cex, 1), by = c(group, "item_code")]
    # For the first period, no lag exists — use the current share alone (equivalent to Laspeyres for t=1)
    monthly_shares[, ri_cex := ifelse(is.na(ri_cex_lag), ri_cex, (ri_cex + ri_cex_lag) / 2)]
    monthly_shares[, ri_cex_lag := NULL]

    output_cols <- c("ym", group, "item_code", "ri_cex", "monthly_inflation", "price")

    tornqvist_ri_series <- monthly_shares[, ..output_cols]

    output_path <- paste0(intermediate_us_path, "tornqvist_ri_series.csv")
    fwrite(tornqvist_ri_series, output_path)
}

    #d. Paasche/Laspeyres:
make_monthly_ri_series <- function(group, urban_name = "urban", type = "laspeyres") {

    intermediate_us_path <- paste0(sharing_intermediate_base_path, "US/", group, "_", urban_name, "_level/")

    spending_weights <- combine_ucc_xwalk(group, urban_name)
    ucc_spending <- spending_weights[[1]]
    monthly_weights <- spending_weights[[3]]

    # gt from the crosswalk is an item-level UCC-to-CPI mapping weight.
    # After the democratic transformation, tot_expend is already a budget share.
    # Multiplying by wgt here allocates the share across CPI item codes proportionally,
    # which is correct — wgt represents the fraction of a UCC that maps to each item_code,
    # not a dollar-value weight. This is safe for democratic shares.
    # VERIFY: sum(wgt) by ucc × yearquarter should equal 1.0
    monthly_item_spending <- ucc_spending[, .(
      tot_expend = sum(tot_expend * wgt)
    ), by = c(group, "ucc_source", "year", "year_mo", "item_code")]
    
    # wgt is a proportional split of a UCC across CPI item codes.
    # Values <= 1 are expected — the remainder represents spending not covered by CPI sampling.
    # What would indicate a problem is wgt > 1, which would inflate democratic shares.
    wgt_check <- ucc_spending[, .(wgt_sum = sum(wgt)), by = c("ucc", "interview_yearq", "ucc_source")]
    if (any(wgt_check$wgt_sum > 1.01)) {
      warning("Some UCCs have wgt summing above 1 — democratic shares may be inflated")
    }

    monthly_item_spending <- monthly_item_spending[monthly_weights,
        on = c(group, "ucc_source", "year", "year_mo")
    ]

    prices <- fread(paste0(sharing_data_raw_path, "item_price_series.csv"),
        colClasses = c("ym" = "character")
    )
    setorder(prices, date)
    prices[, monthly_inflation := price / data.table::shift(price, n = 1), by = item_code]

    monthly_item_spending[, year_mo := as.character(year_mo)]
    monthly_weights[,     year_mo := as.character(year_mo)]
    monthly_item_spending[, ym := year_mo]
    monthly_item_spending <- monthly_item_spending[, .(
      expend = sum(tot_expend / group_popwt)
    ),
    by = c(group, "year", "ym", "item_code")
    ]
    monthly_cpi_spending <- monthly_item_spending[ri_names, on = .(item_code), nomatch = NULL]

    monthly_cpi_spending <- monthly_cpi_spending[, .(
        expend = sum(expend)
    ), by = c(group, "item_code_cpi", "year", "ym")]

    all_groups <- unique(monthly_cpi_spending[, get(group)])
    all_ym <- c(unique(prices[, ym]))
    all_items <- unique(monthly_cpi_spending[, item_code_cpi])
    item_groups <- CJ(all_groups, all_ym, all_items)
    colnames(item_groups) <- c(group, "ym", "item_code_cpi")

    monthly_cpi_spending <- monthly_cpi_spending[item_groups, on = c(group, "ym", "item_code_cpi")]
    monthly_cpi_spending[is.na(expend), expend := 0]
    monthly_cpi_spending[, year := substr(ym, 1, 4)]
    monthly_cpi_spending <- monthly_cpi_spending[item_code_cpi != ""]

    # for years prior to 1999, set spending to the average for that year
    cpi_spending_1999 <- monthly_cpi_spending[year == 1999, c(group, "ym", "item_code_cpi", "expend"), with = FALSE]
    cpi_spending_1999 <- cpi_spending_1999[, .(expend = mean(expend)), by = c(group, "item_code_cpi")]

    monthly_cpi_spending <- monthly_cpi_spending[cpi_spending_1999, on = c(group, "item_code_cpi")]
    monthly_cpi_spending[ym < "199901", expend := i.expend]
    monthly_cpi_spending <- monthly_cpi_spending[, !"i.expend"]

    # Getting spending shares
    monthly_cpi_spending[, `:=`(
        ri_cex = expend / sum(expend)
    ), by = c("year", "ym", group)]

    monthly_cpi_spending[, date := as.Date(paste0(ym, "01"), format = "%Y%m%d")]


    if (type == "laspeyres") {
        prices[, ref_date := date %m-% months(1)]
        output_name <- paste0(intermediate_us_path, "monthly_laspeyres_ri_series.csv")
    } else {
        prices[, ref_date := date]
        output_name <- paste0(intermediate_us_path, "monthly_paasche_ri_series.csv")
    }

    group_ri_series <- prices[monthly_cpi_spending,
        on = c(ref_date = "date", item_code = "item_code_cpi"), nomatch = NULL
    ]

    output_cols <- c("ym", group, "item_code", "ri_cex", "expend", "monthly_inflation", "price")
    monthly_ri_series <- group_ri_series[, ..output_cols]

    monthly_ri_series[, ri_cex := if_else(is.na(ri_cex), 0, ri_cex)]

    fwrite(monthly_ri_series, output_name)
}




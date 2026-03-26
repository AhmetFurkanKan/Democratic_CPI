#This script:
#       -defines the functions to generate all price series

#Inputs:
#       - Shares series generated with 2.make_us_shares.r saved into "intermediate/US/group_urban_name_level"
#       - yearly_ri_series.csv: Relative importance of items in the CPI saved into "processed_bls_series"

#Outputs:
#       - Price series (CES, CPI, Laspeyres, Paasche, Tornqvist, Chained CPI) saved into "output/US/group_urban_name_level"
#           - CPI: cpi_price_series.csv
#           - Chained CPI: chained_price_series.csv
#           - Tornqvist: tornqvist_price_series.csv
#           - Paasche: paasche_price_series.csv
#           - Laspeyres: laspeyres_price_series.csv
#           - CES: ces_sigma_price_series.csv

#       - Wide datasets (CES, CPI, Laspeyres, Paasche, Tornqvist) saved into "output/US/group_urban_name_level"
#           - Laspeyres: wide_monthly_laspeyres_ri_prices.csv
#           - Paasche: wide_monthly_paasche_ri_prices.csv
#           - Tornqvist: wide_monthly_tornqvist_ri_prices.csv
#           - CPI: wide_cpi_ri_prices.csv
#           - CES : wide_ces_0.6_ri_prices.csv




#1. CES:
make_ces_price_series <- function(group, first_cpi_year, urban_name = "all", exclude_name = "",
                                  local_name = "US", exclude_items = c(NULL), sigma = .6) {
    intermediate_data_path <- paste0(sharing_intermediate_base_path, local_name, "/", group, "_", urban_name, "_level/")
    make_dir(paste0(sharing_data_output_path, local_name))
    output_data_path <- paste0(sharing_data_output_path, local_name, "/", group, "_", urban_name, "_level/")
    make_dir(output_data_path)
    ri_path <- paste0(intermediate_data_path, "ces_", sigma, "_ri_series.csv")
    yearly_series <- fread(ri_path)

    if (local_name == "US") {
        yearly_series[, item_code_original := item_code]
    }
    yearly_series <- yearly_series[ym >= paste0(first_cpi_year, "01") & !(item_code_original %in% exclude_items)]
    yearly_series[, date := as.Date(paste0(ym, "01"), format = "%Y%m%d")]

    yearly_series[, ri_cex := ri_cex / sum(ri_cex), by = c(group, "ym")]

    ces_prices <- yearly_series[, .(
        monthly_inflation = sum(ri_cex * monthly_inflation)^(1 / (1 - sigma)) - 1
    ), by = c(group, "date", "ym")]

    ces_prices[ym == paste0(first_cpi_year, "01"), monthly_inflation := 0]
    ces_prices <- ces_prices[date >= as.Date(paste0(first_cpi_year, "01", "01"), format = "%Y%m%d")]

    setorder(ces_prices, ym)
    ces_prices[, price_index := cumprod(monthly_inflation + 1) * 100, by = c(group)]

    if (ces_prices[, min(ym)] > paste0(first_cpi_year, "01")) {
        first_prices <- data.table(ces_prices[, unique(get(group))])
        colnames(first_prices) <- c(group)

        first_prices[, `:=`(
            date = as.Date(paste0(first_cpi_year, "01", "01"), format = "%Y%m%d"),
            ym = as.integer(paste0(first_cpi_year, "01")),
            monthly_inflation = 0, price_index = 100
        )]

        ces_prices <- bind_rows(list(first_prices, ces_prices)) %>% setDT()
    }

    output_path <- paste0(output_data_path, "ces_", sigma, exclude_name, "_price_series.csv")
    fwrite(ces_prices, output_path)
    
}

#2. Tornqvist:
make_tornqvist_price_series <- function(group, first_cpi_year, urban_name = "all",
                                        local_name = "US", exclude_name = "", exclude_items = c(NULL)) {
  intermediate_data_path <- paste0(sharing_intermediate_base_path, local_name, "/", group, "_", urban_name, "_level/")
  make_dir(paste0(sharing_data_output_path, local_name))
    
    output_data_path <- paste0(sharing_data_output_path, local_name, "/", group, "_", urban_name, "_level/")
    make_dir(output_data_path)
    tornqvist_ri_series <- fread(paste0(intermediate_data_path, "tornqvist_ri_series.csv"))

    if (local_name == "US") {
        tornqvist_ri_series[, item_code_original := item_code]
    }

    tornqvist_ri_series <- tornqvist_ri_series[ym >= paste0(first_cpi_year, "01") & !(item_code_original %in% exclude_items)]

    tornqvist_ri_series[, ri_cex := ri_cex / sum(ri_cex), by = c(group, "ym")]

    tornqvist_price_series <- tornqvist_ri_series[,
        .(monthly_inflation = exp(sum(ri_cex * log(monthly_inflation))) - 1),
        by = c(group, "ym")
    ]

    setorder(tornqvist_price_series, ym)
    first_ym <- min(tornqvist_price_series[, ym])
    tornqvist_price_series[ym == first_ym, monthly_inflation := 0]

    setorder(tornqvist_price_series, ym)
    tornqvist_price_series[, price_index := cumprod(monthly_inflation + 1) * 100, by = c(group)]

    output_path <- sprintf("%stornqvist%s_price_series.csv", output_data_path, exclude_name)
    fwrite(tornqvist_price_series, output_path)
}

#3. Chained CPI:
make_chained_cpi <- function(group, urban_name = "all", sigma = .6, exclude_items = c(NULL),
                             local_name = "US", exclude_name = "", first_cpi_year = first_cpi_year) {
  intermediate_data_path <- paste0(sharing_intermediate_base_path, local_name, "/", group, "_", urban_name, "_level/")
  make_dir(paste0(sharing_data_output_path, local_name))
  
  output_data_path <- paste0(sharing_data_output_path, local_name, "/", group, "_", urban_name, "_level/")
  make_dir(output_data_path)
    tornqvist_ri_series <- fread(paste0(intermediate_data_path, "tornqvist_ri_series.csv"))

    if (local_name == "US") {
        tornqvist_ri_series[, item_code_original := item_code]
    }
    tornqvist_ri_series <- tornqvist_ri_series[ym >= paste0(first_cpi_year, "01") & !(item_code_original %in% exclude_items)]


    tornqvist_ri_series[, ri_cex := ri_cex / sum(ri_cex), by = c(group, "ym")]

    tornqvist_price_series <- tornqvist_ri_series[,
        .(monthly_inflation = exp(sum(ri_cex * log(monthly_inflation))) - 1),
        by = c(group, "ym")
    ]

    first_ym <- min(tornqvist_price_series[, ym])
    tornqvist_price_series[ym == first_ym, monthly_inflation := 0]

    last_torn_ym <- tornqvist_price_series[!is.na(monthly_inflation), max(ym)]
    
    tornqvist_price_series <- na.omit(tornqvist_price_series, cols = "monthly_inflation")


    ces_ri_path <- paste0(intermediate_data_path, "ces_", sigma, "_ri_series.csv")
    ces_series <- fread(ces_ri_path)

    if (local_name == "US") {
        ces_series[, item_code_original := item_code]
    }


    ces_series[, ri_cex := ri_cex / sum(ri_cex), by = c(group, "ym")]

    ces_prices <- ces_series[ym > last_torn_ym, .(
        monthly_inflation = sum(ri_cex * monthly_inflation)^(1 / (1 - sigma)) - 1
    ), by = c(group, "ym")]


    chained_cpi <- bind_rows(tornqvist_price_series, ces_prices) %>% setDT()

    setorder(chained_cpi, ym)
    chained_cpi[, price_index := cumprod(monthly_inflation + 1) * 100, by = c(group)]

    output_path <- paste0(output_data_path, "chained", exclude_name, "_price_series.csv")
    fwrite(chained_cpi, output_path)
}

#4. Laspeyres:
make_monthly_price_series <- function(group, first_cpi_year, urban_name = "all",
                                      local_name = "US", exclude_name = "",
                                      exclude_items = c(NULL), type = "laspeyres") {
  intermediate_data_path <- paste0(sharing_intermediate_base_path, local_name, "/", group, "_", urban_name, "_level/")
  make_dir(paste0(sharing_data_output_path, local_name))
  
  output_data_path <- paste0(sharing_data_output_path, local_name, "/", group, "_", urban_name, "_level/")
  make_dir(output_data_path)
    ri_path <- paste0(intermediate_data_path, "monthly_", type, "_ri_series", ".csv")

    monthly_ri_series <- fread(ri_path)

    if (local_name == "US") {
        monthly_ri_series[, item_code_original := item_code]
    }
    monthly_ri_series <- monthly_ri_series[ym >= paste0(first_cpi_year, "01") & !(item_code_original %in% exclude_items)]

    monthly_ri_series[, ri_cex := ri_cex / sum(ri_cex), by = c(group, "ym")]

    monthly_price_series <- monthly_ri_series[,
        .(monthly_inflation = sum(ri_cex * monthly_inflation) - 1),
        by = c(group, "ym")
    ]

    first_ym <- min(monthly_price_series[, ym])
    monthly_price_series[ym == first_ym, monthly_inflation := 0]

    setorder(monthly_price_series, ym)
    monthly_price_series[, price_index := cumprod(monthly_inflation + 1) * 100, by = c(group)]

    output_path <- paste0(output_data_path, type, "_", exclude_name, "price_series.csv")
    fwrite(monthly_price_series, output_path)
}

#4. CPI:
make_cpi_price_series <- function(group, first_cpi_year, urban_name = "all",
                                  local_name = "US", exclude_name = "", exclude_items = c(NULL), computation_method="Fixed shares") {
  intermediate_data_path <- paste0(sharing_intermediate_base_path, local_name, "/", group, "_", urban_name, "_level/")
  make_dir(paste0(sharing_data_output_path, local_name))
  
  output_data_path <- paste0(sharing_data_output_path, local_name, "/", group, "_", urban_name, "_level/")
  make_dir(output_data_path)
    
    
  
        ri_path <- paste0(intermediate_data_path, "cpi_ri_series.csv")

    cpi_ri_series <- fread(ri_path)
    
    if (local_name == "US") {
        cpi_ri_series[, item_code_original := item_code]

    }

    cpi_ri_series <- cpi_ri_series[ym >= paste0(first_cpi_year, "01") & !(item_code_original %in% exclude_items)]

    cpi_ri_series[, ri_cex := ri_cex / sum(ri_cex), by = c(group, "ym")]
    cpi_ri_series[, raw_ri := raw_ri / sum(raw_ri), by = c(group, "ym")]
    
    cpi_price_series <- cpi_ri_series[,
        .(
            monthly_inflation = sum(ri_cex * monthly_inflation) - 1,
            monthly_inflation_raw = sum(raw_ri * monthly_inflation) - 1
        ),
        by = c(group, "ym")
    ]
    
 

    first_ym <- min(cpi_price_series[, ym])
    cpi_price_series[ym == first_ym, monthly_inflation := 0]
    cpi_price_series[ym == first_ym, monthly_inflation_raw := 0]

    setorder(cpi_price_series, ym)
    cpi_price_series[, price_index := cumprod(monthly_inflation + 1) * 100, by = c(group)]
    cpi_price_series[, price_index_raw := cumprod(monthly_inflation_raw + 1) * 100, by = c(group)]
    
    cpi_price_series <- cpi_price_series %>%
      select(-monthly_inflation_raw, -price_index_raw)
    
    output_path <- paste0(output_data_path, "cpi", exclude_name, "_price_series.csv")
       
        
    fwrite(cpi_price_series, output_path)
}


#5. Create a wide dataset:
get_wide_data <- function(group, urban_name = "all", ri_series_name = "cpi",
                          local_name = "US") {
  intermediate_data_path <- paste0(sharing_intermediate_base_path, local_name, "/", group, "_", urban_name, "_level/")
  output_data_path <- paste0(sharing_data_output_path, local_name, "/", group, "_", urban_name, "_level/")
  
  ri_series <- fread(paste0(sharing_data_raw_path, "yearly_ri_series.csv"),
                     colClasses = c("year" = "character"),
                     select = c("item_code_cpi", "item_name_ri", "name_3", "name_1")
  )
  cex_ri_series <- fread(paste0(intermediate_data_path, ri_series_name, "_ri_series.csv"),
                         colClasses = c("ym" = "character")
  )
  
  cex_ri_series[, ri_cex := ri_cex / sum(ri_cex), by = c(group, "ym")]
  
  ri_series_cpi <- ri_series
  ri_series_cpi[item_code_cpi == "", `:=`(item_name_ri = "Unsampled", name_1 = "Unsampled", name_3 = "Unsampled")]
  ri_series_cpi <- unique(ri_series_cpi)
  
  
  if (local_name == "US") {
    cex_ri_series[, psu := "US"]
    cex_ri_series[, item_code_original := item_code]
  }
  
  cex_series_cols <- c(
    group, "ri_cex", "ym", "year", "month", "item_code",
    "item_code_original", "psu", "price", "monthly_inflation"
  )
  
  
  cex_ri_series[, (group) := paste("ri", group, get(group), sep = "_")]
  cex_ri_series[, year := substr(ym, 1, 4)]
  cex_ri_series[, month := substr(ym, 5, 6)]
  cex_ri_series <- cex_ri_series[, ..cex_series_cols]

  widen_formula <- as.formula(paste0(paste(cex_series_cols[3:10], collapse = "+"), "~", group))
  cex_series_wide <- dcast(cex_ri_series, widen_formula, value.var = "ri_cex", fun.aggregate = mean)
  
  ri_wide_cols <- colnames(cex_series_wide)
  colnames(cex_series_wide)[ri_wide_cols == paste("ri", group, 0, sep = "_")] <- "ri"
  
  last_ym <- max(cex_ri_series[, ym])
  
  cex_series_wide <- ri_series_cpi[cex_series_wide,
                                   on = c(item_code_cpi = "item_code_original"),
                                   allow.cartesian = TRUE
  ]
  
  cex_series_wide[item_code_cpi == "", `:=`(
    item_name_ri = "Unsampled",
    name_1 = "Unsampled",
    name_3 = "Unsampled"
  )]
  
  cex_series_wide <- cex_series_wide[ym <= last_ym]
  colnames(cex_series_wide)[colnames(cex_series_wide) == "item_code_cpi"] <- "item_code_original"
  output_name <- sprintf("wide_%s_ri_prices.csv", ri_series_name)
  
  fwrite(cex_series_wide, paste0(output_data_path, output_name))
}

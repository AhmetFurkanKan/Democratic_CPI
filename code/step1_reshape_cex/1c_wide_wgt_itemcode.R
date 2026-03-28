################################################################################
# Step 1c — Wide ItemCode pivot (weighted crosswalk)
#
# Purpose : Join raw CEX UCC spending to the quarterly UCC→ItemCode crosswalk,
#           apply the crosswalk weight unconditionally (cost * wgt), aggregate
#           to ItemCode level, and pivot to a wide table (one row per household
#           × month, one column per ItemCode).  Demographics are merged back in.
#
#           Key difference from step 1b: the crosswalk weight is ALWAYS applied
#           here (cost := cost * wgt), with no conditional guard.
#
# Inputs  : cex_raw_dir/ucc_spending{YYYY}_light.csv   (one file per year)
#           xwalk_path   (xwalk_by_yearquarter.csv — defined in 00_master.R)
# Outputs : pre_hurdle_dir/{YYYY}/ItemCode_WGT_spending{YYYY}_light_WIDE.csv
#
# Called from : 00_master.R (RUN_STEP_1C = TRUE)
# Standalone  : define cex_raw_dir, pre_hurdle_dir, xwalk_path, and
#               years_to_process before sourcing this script.
################################################################################

library(data.table)

#    The stopifnot() calls below catch the case where this script is run standalone without them being set.

stopifnot(
  "cex_raw_dir not found — source 00_master.R first, or define it manually." =
    exists("cex_raw_dir"),
  "pre_hurdle_dir not found — source 00_master.R first, or define it manually." =
    exists("pre_hurdle_dir"),
  "xwalk_path not found — source 00_master.R first, or define it manually." =
    exists("xwalk_path")
)

# Years to process, can be overridden by 00_master.R before sourcing.
if (!exists("years_to_process")) {
  years_to_process <- as.character(1999:2021)
}

# ------------------------------------------------------------------------------
# Main loop


for (target_year in years_to_process) {

  # Output folder for this year
  year_folder <- paste0(pre_hurdle_dir, target_year, "/")
  if (!dir.exists(year_folder)) {
    dir.create(year_folder, recursive = TRUE)
  }

  # 1) Load files
  input_file <- paste0(cex_raw_dir, "ucc_spending", target_year, "_light.csv")
  dt <- fread(input_file, colClasses = c(ucc = "character", year_mo = "character"))

  xwalk <- fread(xwalk_path,
                 colClasses = c(ucc = "character", yearquarter = "character"))

  # Calculate quarter from year_mo
  dt[, qtr_temp := ceiling(as.numeric(substr(year_mo, 5, 6)) / 3)]
  dt[, yearquarter_calc := paste0(substr(year_mo, 1, 4), qtr_temp)]

  # 2) Join the crosswalk
  # Using yearquarter_calc to match the crosswalk's yearquarter
  dt_joined <- merge(dt, xwalk,
                     by.x = c("ucc", "yearquarter_calc"),
                     by.y = c("ucc", "yearquarter"),
                     all.x = TRUE,
                     allow.cartesian = TRUE)

  # 3) Apply weights & filter
  dt_joined <- dt_joined[!is.na(ItemCode)]
  dt_joined[, cost := cost * wgt]

  # 4) Aggregate spending by newid, year_mo, and ItemCode
  dt_agg <- dt_joined[, .(cost = sum(cost, na.rm = TRUE)),
                      by = .(newid, year_mo, ItemCode)]

  # 5) Pivot wide: newid + year_mo ~ ItemCode
  dt_wide_item <- dcast(
    dt_agg,
    newid + year_mo ~ ItemCode,
    value.var     = "cost",
    fun.aggregate = sum,
    fill          = 0
  )

  # 6) Prefix ItemCode columns
  item_cols <- setdiff(names(dt_wide_item), c("newid", "year_mo"))
  setnames(dt_wide_item, item_cols, paste0("item_", item_cols))

  # 7) Merge back demographics
  demo_cols <- setdiff(names(dt), c("ucc", "cost", "qtr_temp", "yearquarter_calc", "interview_yearq"))
  dt_demo <- unique(dt[, .SD, .SDcols = demo_cols], by = c("newid", "year_mo"))
  dt_wide_item <- merge(dt_demo, dt_wide_item, by = c("newid", "year_mo"), all.y = TRUE)

  # 8) Save
  output_file <- paste0(year_folder, "ItemCode_WGT_spending", target_year, "_light_WIDE.csv")
  fwrite(dt_wide_item, output_file)

  #clear memory for the next year
  rm(dt, dt_joined, dt_agg, dt_wide_item, dt_demo)
  message("Step 1c done: ", target_year, " → ", output_file)
}

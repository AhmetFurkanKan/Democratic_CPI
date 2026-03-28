################################################################################
# Step 6 — Post-fusion wide Category pivot, weighted, with rent
#
# Purpose : Functionally equivalent to step 2b but operates on the FUSED CEX
#           spending file (step 5 output) and covers the full 1999–2021 range.
#
#           Key differences from step 2b:
#           - Input comes from fused_cex_dir (step 5), not raw CEX files
#           - Housing rows (OER/rent) have i.data forced to "fmli" so they go
#             through Interview quarterly annualisation in step 7
#           - Hierarchy join uses Level_1 (not Level_2_5)
#           - Aggregation and dcast formula include i.data as a grouping key
#
# Inputs  : fused_cex_dir/ucc_spending{YYYY}.csv
#           xwalk_path      (xwalk_by_yearquarter.csv)
#           hierarchy_path  (XWALK_Furkan_itemcode_to_category.csv)
# Outputs : post_hurdle_dir/{YYYY}/2_category_WGT_spending{YYYY}_light_WIDE_with_rent.csv
#
# Called from : 00_master.R (RUN_STEP_6 = TRUE)
# Standalone  : define fused_cex_dir, post_hurdle_dir, xwalk_path, hierarchy_path,
#               and years_to_process before sourcing this script.
################################################################################

library(data.table)

# ------------------------------------------------------------------------------
# 0) Configuration
#    When sourced from 00_master.R these variables already exist in the session.
#    The stopifnot() calls below catch the case where this script is run
#    standalone without them being set.
# ------------------------------------------------------------------------------

stopifnot(
  "fused_cex_dir not found — source 00_master.R first, or define it manually." =
    exists("fused_cex_dir"),
  "post_hurdle_dir not found — source 00_master.R first, or define it manually." =
    exists("post_hurdle_dir"),
  "xwalk_path not found — source 00_master.R first, or define it manually." =
    exists("xwalk_path"),
  "hierarchy_path not found — source 00_master.R first, or define it manually." =
    exists("hierarchy_path")
)

# Years to process — can be overridden by 00_master.R before sourcing.
if (!exists("years_to_process")) {
  #years_to_process <- c("2013","2014","2015","2016","2017","2018","2019") # Add your years here
  years_to_process <- 1999:2021
}

# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------

for (year_val in years_to_process) {

  # ── Output folder for this year ─────────────────────────────────────────────
  year_folder <- paste0(post_hurdle_dir, year_val, "/")
  if (!dir.exists(year_folder)) {
    dir.create(year_folder, recursive = TRUE)
  }

  # ── 1) Load primary data files ───────────────────────────────────────────────
  dt_path <- paste0(fused_cex_dir, "ucc_spending", year_val, ".csv")

  # Safety check: skip year if input file is missing
  if (!file.exists(dt_path)) {
    message(paste("Skipping year", year_val, "- input file not found."))
    next
  }

  dt <- fread(dt_path, colClasses = c(ucc = "character", year_mo = "character"))

  xwalk <- fread(xwalk_path,
                 colClasses = c(ucc = "character", yearquarter = "character"))

  hierarchy <- fread(hierarchy_path)
  setnames(hierarchy, "Item_codes", "ItemCode")

  # ── Housing logic for OER and secondary rent ─────────────────────────────────

  # 1. Extract unique rent by household-month ONLY.
  # DO NOT include i.data here, or you will get two rows for fused households!
  housing_dt <- unique(dt[, .(newid, year_mo, owners_rent, secondary_rent)])

  # 2. Create the long-format rows and FORCE i.data = "fmli".
  # Rent/OER are Interview concepts. Forcing "fmli" ensures they only go
  # through the correct quarterly annualization math in Script 7.
  oer_dt  <- housing_dt[, .(newid, year_mo, i.data = "fmli", ucc = "910104", cost = owners_rent)]
  rent_dt <- housing_dt[, .(newid, year_mo, i.data = "fmli", ucc = "910105", cost = secondary_rent)]

  # 3. Combine housing back into the main dataset
  dt <- rbindlist(list(dt, oer_dt, rent_dt), fill = TRUE)

  # ── Continue with original workflow ──────────────────────────────────────────

  dt[, qtr_temp := ceiling(as.numeric(substr(year_mo, 5, 6)) / 3)]
  dt[, yearquarter_calc := paste0(substr(year_mo, 1, 4), qtr_temp)]

  # ── 3) Join the UCC crosswalk ────────────────────────────────────────────────
  dt_joined <- merge(dt, xwalk,
                     by.x = c("ucc", "yearquarter_calc"),
                     by.y = c("ucc", "yearquarter"),
                     all.x = TRUE,
                     allow.cartesian = TRUE)

  # ── 4) Join the hierarchy ────────────────────────────────────────────────────
  dt_joined <- merge(dt_joined, hierarchy[, .(ItemCode, Level_1)],
                     by = "ItemCode",
                     all.x = TRUE)

  # ── 5) Handle missing categories ─────────────────────────────────────────────
  dt_joined[is.na(Level_1), Level_1 := "Uncategorized_Other"]

  # Apply the weight to the cost
  dt_joined[, cost := cost * wgt]

  # ── 6) Aggregate spending including i.data to separate the fused sources ─────
  dt_agg_cat <- dt_joined[, .(cost = sum(cost, na.rm = TRUE)),
                          by = .(newid, year_mo, i.data, Level_1)]

  # ── 7) Pivot wide (i.data is on the left side of the formula) ────────────────
  dt_wide_cat <- dcast(
    dt_agg_cat,
    newid + year_mo + i.data ~ Level_1,
    value.var     = "cost",
    fun.aggregate = sum,
    fill          = 0
  )

  # ── 8) Prefix and clean category names ───────────────────────────────────────
  cat_cols    <- setdiff(names(dt_wide_cat), c("newid", "year_mo", "i.data"))
  clean_names <- paste0("cat_", gsub("[[:space:][:punct:]]+", "_", cat_cols))
  setnames(dt_wide_cat, cat_cols, clean_names)

  # ── 9) Merge back demographics ───────────────────────────────────────────────
  # We MUST exclude i.data from the demo columns so we don't duplicate it or break the merge
  demo_cols <- setdiff(names(dt), c("ucc", "cost", "qtr_temp", "yearquarter_calc", "interview_yearq", "owners_rent", "secondary_rent", "i.data"))

  dt_demo <- unique(dt[, .SD, .SDcols = demo_cols], by = c("newid", "year_mo"))
  dt_wide_cat <- merge(dt_demo, dt_wide_cat, by = c("newid", "year_mo"), all.y = TRUE)

  # ── 10) Save ─────────────────────────────────────────────────────────────────
  output_filename <- paste0("2_category_WGT_spending", year_val, "_light_WIDE_with_rent.csv")
  output_path     <- paste0(year_folder, output_filename)

  fwrite(dt_wide_cat, output_path)

  message(paste0("Processing complete for year ", year_val, ". OER (910104) and Rent (910105) integrated."))

  # Clean up memory before the next year
  rm(dt, dt_joined, dt_agg_cat, dt_wide_cat, dt_demo, housing_dt, oer_dt, rent_dt)
}

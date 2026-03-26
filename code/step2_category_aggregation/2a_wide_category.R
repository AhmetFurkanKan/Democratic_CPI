################################################################################
# Step 2a — Wide Category pivot, unweighted, with rent (exploratory)
#
# Purpose : Join raw CEX UCC spending to the quarterly UCC→ItemCode crosswalk,
#           map ItemCodes to Level_2_5 categories via the hierarchy crosswalk,
#           inject OER (UCC 910104) and secondary rent (UCC 910105) from
#           household-level columns, aggregate to category level, and pivot to
#           a wide table.  No crosswalk weight is applied (unweighted variant).
#
#           NOTE: this script is exploratory / for validation.  It is NOT
#           consumed by any downstream step.  The weighted variant (step 2b)
#           feeds the main pipeline.
#
# Inputs  : cex_raw_dir/ucc_spending{YYYY}_light.csv       (one file per year)
#           xwalk_path      (xwalk_by_yearquarter.csv)
#           hierarchy_path  (XWALK_Furkan_itemcode_to_category.csv)
# Outputs : pre_hurdle_dir/{YYYY}/2_category_spending{YYYY}_light_WIDE_with_rent.csv
#
# Called from : 00_master.R (RUN_STEP_2A = TRUE)
# Standalone  : define cex_raw_dir, pre_hurdle_dir, xwalk_path, hierarchy_path,
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
  "cex_raw_dir not found — source 00_master.R first, or define it manually." =
    exists("cex_raw_dir"),
  "pre_hurdle_dir not found — source 00_master.R first, or define it manually." =
    exists("pre_hurdle_dir"),
  "xwalk_path not found — source 00_master.R first, or define it manually." =
    exists("xwalk_path"),
  "hierarchy_path not found — source 00_master.R first, or define it manually." =
    exists("hierarchy_path")
)

# Years to process — can be overridden by 00_master.R before sourcing.
if (!exists("years_to_process")) {
  years_to_process <- as.character(1999:2021)
}

# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------

for (year_val in years_to_process) {

  # ── Output folder for this year ─────────────────────────────────────────────
  year_folder <- paste0(pre_hurdle_dir, year_val, "/")
  if (!dir.exists(year_folder)) {
    dir.create(year_folder, recursive = TRUE)
  }

  # ── 1) Load primary data files ───────────────────────────────────────────────
  dt_path <- paste0(cex_raw_dir, "ucc_spending", year_val, "_light.csv")
  dt <- fread(dt_path, colClasses = c(ucc = "character", year_mo = "character"))

  xwalk <- fread(xwalk_path,
                 colClasses = c(ucc = "character", yearquarter = "character"))

  hierarchy <- fread(hierarchy_path)
  setnames(hierarchy, "Item_codes", "ItemCode")

  # ── Housing logic for OER and secondary rent ─────────────────────────────────
  # We extract these specific variables and assign them their official UCCs
  # [cite_start]UCC 910104 = Owners' equivalent rent of Secondary residence [cite: 1887]
  # [cite_start]UCC 910105 = Rent of Secondary residence [cite: 1887]

  # Extract unique household-month records to avoid duplication of housing costs
  housing_dt <- unique(dt[, .(newid, year_mo, owners_rent, secondary_rent)])

  # Create long-format entries for housing to match the UCC structure
  oer_dt  <- housing_dt[, .(newid, year_mo, ucc = "910104", cost = owners_rent)]
  rent_dt <- housing_dt[, .(newid, year_mo, ucc = "910105", cost = secondary_rent)]

  # Combine housing back into the main dataset
  # Note: Ensure you aren't double-counting rent if it already exists under a different UCC in 'dt'
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
  dt_joined <- merge(dt_joined, hierarchy[, .(ItemCode, Level_2_5)],
                     by = "ItemCode",
                     all.x = TRUE)

  # ── 5) Handle missing categories ─────────────────────────────────────────────
  dt_joined[is.na(Level_2_5), Level_2_5 := "Uncategorized_Other"]

  # ── 6) Aggregate spending including the new housing categories ───────────────
  dt_agg_cat <- dt_joined[, .(cost = sum(cost, na.rm = TRUE)),
                          by = .(newid, year_mo, Level_2_5)]

  # ── 7) Pivot wide ────────────────────────────────────────────────────────────
  dt_wide_cat <- dcast(
    dt_agg_cat,
    newid + year_mo ~ Level_2_5,
    value.var     = "cost",
    fun.aggregate = sum,
    fill          = 0
  )

  # ── 8) Prefix and clean category names ───────────────────────────────────────
  cat_cols    <- setdiff(names(dt_wide_cat), c("newid", "year_mo"))
  clean_names <- paste0("cat_", gsub("[[:space:][:punct:]]+", "_", cat_cols))
  setnames(dt_wide_cat, cat_cols, clean_names)

  # ── 9) Merge back demographics (excluding cost-related columns) ──────────────
  demo_cols <- setdiff(names(dt), c("ucc", "cost", "qtr_temp", "yearquarter_calc", "interview_yearq", "owners_rent", "secondary_rent"))
  dt_demo <- unique(dt[, .SD, .SDcols = demo_cols], by = c("newid", "year_mo"))
  dt_wide_cat <- merge(dt_demo, dt_wide_cat, by = c("newid", "year_mo"), all.y = TRUE)

  # ── 10) Save ─────────────────────────────────────────────────────────────────
  output_path <- paste0(year_folder, "2_category_spending", year_val, "_light_WIDE_with_rent.csv")
  fwrite(dt_wide_cat, output_path)

  message("Processing complete for year ", year_val, ". OER (910104) and Rent (910105) integrated.")

  # Clean up memory before the next year
  rm(dt, dt_joined, dt_agg_cat, dt_wide_cat, dt_demo, housing_dt, oer_dt, rent_dt)
}

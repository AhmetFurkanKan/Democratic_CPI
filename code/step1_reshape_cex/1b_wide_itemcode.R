################################################################################
# Step 1b — Wide ItemCode pivot (unweighted crosswalk)
#
# Purpose : Join raw CEX UCC spending to the quarterly UCC→ItemCode crosswalk,
#           optionally apply weights if a wgt column is present, aggregate to
#           ItemCode level, and pivot to a wide table (one row per household ×
#           month, one column per ItemCode).  Demographics are merged back in.
#
# Inputs  : cex_raw_dir/ucc_spending{YYYY}_light.csv   (one file per year)
#           xwalk_path   (xwalk_by_yearquarter.csv — defined in 00_master.R)
# Outputs : pre_hurdle_dir/{YYYY}/ItemCode_spending{YYYY}_light_WIDE.csv
#
# Called from : 00_master.R (RUN_STEP_1B = TRUE)
# Standalone  : define cex_raw_dir, pre_hurdle_dir, xwalk_path, and
#               years_to_process before sourcing this script.
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
    exists("xwalk_path")
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

  # ── 1) Load files ────────────────────────────────────────────────────────────
  dt_path <- paste0(cex_raw_dir, "ucc_spending", year_val, "_light.csv")
  dt <- fread(dt_path, colClasses = c(ucc = "character", year_mo = "character"))

  xwalk <- fread(xwalk_path, colClasses = c(ucc = "character", yearquarter = "character"))

  # ── Calculate quarter from year_mo ───────────────────────────────────────────
  dt[, qtr_temp := ceiling(as.numeric(substr(year_mo, 5, 6)) / 3)]
  dt[, yearquarter_calc := paste0(substr(year_mo, 1, 4), qtr_temp)]

  # ── 2) Join the crosswalk ────────────────────────────────────────────────────
  dt_joined <- merge(dt, xwalk,
                     by.x = c("ucc", "yearquarter_calc"),
                     by.y = c("ucc", "yearquarter"),
                     all.x = TRUE,
                     allow.cartesian = TRUE)

  # ── 3) Apply weights & filter ────────────────────────────────────────────────
  dt_joined <- dt_joined[!is.na(ItemCode)]
  # Note: I added the wgt line back in case you intended to keep it from the first version
  if ("wgt" %in% names(dt_joined)) dt_joined[, cost := cost * wgt]

  # ── 4) Aggregate spending by newid, year_mo, and ItemCode ────────────────────
  dt_agg <- dt_joined[, .(cost = sum(cost, na.rm = TRUE)),
                      by = .(newid, year_mo, ItemCode)]

  # ── 5) Pivot wide: newid + year_mo ~ ItemCode ────────────────────────────────
  dt_wide_item <- dcast(
    dt_agg,
    newid + year_mo ~ ItemCode,
    value.var     = "cost",
    fun.aggregate = sum,
    fill          = 0
  )

  # ── 6) Prefix ItemCode columns ───────────────────────────────────────────────
  item_cols <- setdiff(names(dt_wide_item), c("newid", "year_mo"))
  setnames(dt_wide_item, item_cols, paste0("item_", item_cols))

  # ── 7) Merge back demographics ───────────────────────────────────────────────
  # Exclude temporary calculation columns and the original ucc/cost columns
  demo_cols <- setdiff(names(dt), c("ucc", "cost", "qtr_temp", "yearquarter_calc", "interview_yearq"))
  dt_demo <- unique(dt[, .SD, .SDcols = demo_cols], by = c("newid", "year_mo"))
  dt_wide_item <- merge(dt_demo, dt_wide_item, by = c("newid", "year_mo"), all.y = TRUE)

  # ── 8) Save ──────────────────────────────────────────────────────────────────
  out_filename <- paste0("ItemCode_spending", year_val, "_light_WIDE.csv")
  out_path     <- paste0(year_folder, out_filename)

  fwrite(dt_wide_item, out_path)

  # Clean up memory before the next year
  rm(dt, dt_joined, dt_agg, dt_wide_item, dt_demo)
  message("Step 1b done: ", year_val, " → ", out_path)
}

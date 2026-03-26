################################################################################
# Step 7 — Final annualised budget shares, v3 (fmli only — production version)
#
# Purpose : Read the post-fusion weighted wide-category files produced by
#           step 6, compute annualised household-level expenditure totals and
#           budget shares for each spending category, and write one CSV per year.
#
#           FMLI (Interview): collapse to quarterly sums, then annualise with
#             factor = 4 / (#quarters observed).
#           FMLD (Diary): annualise with factor = 52 / (#weeks observed).
#           cat_Uncategorized_Other is excluded from totals and shares.
#           i.data is excluded from the household-year profile so the final
#           output has one unified row per household.
#
#           *** This is the PRODUCTION version of the budget shares. ***
#           *** Its output is consumed by step 8 (DCPI CPI) and step 9 (AIDS). ***
#
# Inputs  : post_hurdle_dir/{YYYY}/2_category_WGT_spending{YYYY}_light_WIDE_with_rent.csv
# Outputs : post_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv
#
# Called from : 00_master.R (RUN_STEP_7 = TRUE)
# Standalone  : define post_hurdle_dir and years_to_process before sourcing.
################################################################################

# ------------------------------------------------------------
# INPUT: dt_wide_cat
# Must contain: newid, year_mo (YYYYMM), i.data ("fmli"), and cat_* columns
# RULES:
#   - Exclude cat_Uncategorized_Other from totals and shares
#   - FMLI: collapse within quarter (mean), then annualize with factor = 4 / (#quarters observed)
#   - KEEP all other input columns by merging a household-year "profile" back in
# ------------------------------------------------------------

library(data.table)

# ------------------------------------------------------------------------------
# 0) Configuration
#    When sourced from 00_master.R these variables already exist in the session.
#    The stopifnot() call below catches the case where this script is run
#    standalone without them being set.
# ------------------------------------------------------------------------------

stopifnot(
  "post_hurdle_dir not found — source 00_master.R first, or define it manually." =
    exists("post_hurdle_dir")
)

# Years to process — can be overridden by 00_master.R before sourcing.
if (!exists("years_to_process")) {
  #years_to_process <- c("2013","2014","2015","2016","2017","2018","2019", "2020", "2021") # Add your years here
  years_to_process <- 1999:2021
}

# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------

for (year_val in years_to_process) {

  # Define the folder we created in previous steps
  year_folder <- paste0(post_hurdle_dir, year_val, "/")

  # Skip if the directory doesn't exist (means previous scripts didn't run)
  if (!dir.exists(year_folder)) {
    message(paste("Directory not found for year", year_val, "- skipping."))
    next
  }

  # ------------------------------------------------------------
  # INPUT & OUTPUT PATHS (Updated for Year-Specific Folders)
  # ------------------------------------------------------------
  input_path  <- paste0(year_folder, "2_category_WGT_spending", year_val, "_light_WIDE_with_rent.csv")
  output_path <- paste0(year_folder, "3_annualized_budget_shares_", year_val, ".csv")

  if (!file.exists(input_path)) {
    message(paste("Input file not found for year", year_val, "- skipping."))
    next
  }

  dt_wide_cat <- fread(input_path)

  # --- Ensure year and month exist ---
  if (!("year" %in% names(dt_wide_cat))) {
    if ("year_mo" %in% names(dt_wide_cat)) {
      dt_wide_cat[, year := substr(year_mo, 1, 4)]
    } else {
      stop("Need either 'year' or 'year_mo' to construct year.")
    }
  }

  dt_wide_cat[, month := as.integer(substr(year_mo, 5, 6))]
  dt_wide_cat[, qtr   := ceiling(month / 3)]
  dt_wide_cat[, yearq := paste0(year, "Q", qtr)]

  # --- Category columns (exclude Uncategorized) ---
  cat_cols_all <- grep("^cat_", names(dt_wide_cat), value = TRUE)
  cat_cols <- setdiff(cat_cols_all, "cat_Uncategorized_Other")
  if (length(cat_cols) == 0L) {
    message(paste("No usable category columns for", year_val))
    next
  }

  # --- Non-category columns to preserve ---
  # EXCLUDE i.data here so it doesn't get attached to the final, unified household row
  non_cat_cols <- setdiff(
    names(dt_wide_cat),
    c(cat_cols_all, "newid", "year", "month", "qtr", "yearq", "i.data") 
  )

  # Build a household-year profile using the LAST observed row
  setorder(dt_wide_cat, newid, year, year_mo)
  dt_profile <- dt_wide_cat[, .SD[.N], by = .(newid, year), .SDcols = non_cat_cols]

  # ============================================================
  # 1) FMLI (Interview): prevent triple counting
  # ============================================================
  dt_fmli <- dt_wide_cat[i.data == "fmli"]

  # (Keep your existing FMLI math here...)
  dt_fmli_q <- dt_fmli[, lapply(.SD, sum, na.rm = TRUE), by = .(newid, year, qtr), .SDcols = cat_cols]
  dt_fmli_year <- dt_fmli_q[, {
    q_obs <- .N
    scale <- 4 / q_obs
    lapply(.SD, function(x) sum(x, na.rm = TRUE) * scale)
  }, by = .(newid, year), .SDcols = cat_cols]

  # ============================================================
  # 2) FMLD (Diary): scale by 52 / weeks observed
  # ============================================================
  dt_fmld <- dt_wide_cat[i.data == "fmld"]

  # (Keep your existing FMLD math here...)
  dt_fmld_year <- dt_fmld[, {
    m_obs <- .N 
    scale <- 52 / m_obs
    lapply(.SD, function(x) sum(x, na.rm = TRUE) * scale)
  }, by = .(newid, year), .SDcols = cat_cols]

  # ============================================================
  # 3) Combine annualized totals
  # ============================================================
  # Stack the annualized FMLI and FMLD datasets, then sum them by newid + year
  dt_year <- rbindlist(list(dt_fmli_year, dt_fmld_year), use.names = TRUE, fill = TRUE)

  # This sum combines the annualized interview categories and the annualized diary categories
  # into a single, fused household budget!
  dt_year <- dt_year[, lapply(.SD, sum, na.rm = TRUE),
                     by = .(newid, year), .SDcols = cat_cols]


  # ============================================================
  # 4) Totals and shares
  # ============================================================
  dt_year[, total_exp := rowSums(.SD, na.rm = TRUE), .SDcols = cat_cols]
  dt_year[total_exp == 0, total_exp := NA_real_]

  # Remove households with no spending
  dt_year <- dt_year[total_exp > 0 & !is.na(total_exp)]

  share_cols <- paste0("share_", sub("^cat_", "", cat_cols))
  dt_year[, (share_cols) := lapply(.SD, function(x) x / total_exp), .SDcols = cat_cols]

  # ============================================================
  # 5) Merge profile back in
  # ============================================================
  dt_out <- merge(dt_profile, dt_year, by = c("newid", "year"), all.y = TRUE)
  dt_out[, share_sum := rowSums(.SD, na.rm = TRUE), .SDcols = share_cols]

  # Save
  fwrite(dt_out, output_path)

  message(paste("Done: Annualized budget shares for", year_val, "saved to:", output_path))

  # Clean up memory
  rm(dt_wide_cat, dt_fmli, dt_fmli_q, dt_fmli_year, dt_fmld, dt_fmld_year, dt_year, dt_out, dt_profile)
}

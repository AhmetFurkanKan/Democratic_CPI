################################################################################
# Step 1a — Wide UCC pivot (no crosswalk)
#
# Purpose : Reshape raw CEX long-format UCC spending files into a wide table
#           with one row per household × month and one column per UCC code.
#           No crosswalk is applied — UCCs are kept as-is.
#
# Inputs  : cex_raw_dir/ucc_spending{YYYY}_light.csv   (one file per year)
# Outputs : pre_hurdle_dir/{YYYY}/ucc_spending{YYYY}_light_WIDE.csv
#
# Called from : 00_master.R (RUN_STEP_1A = TRUE)
# Standalone  : define cex_raw_dir, pre_hurdle_dir, and years_to_process
#               before sourcing this script.
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
    exists("pre_hurdle_dir")
)

# Years to process — can be overridden by 00_master.R before sourcing.
# To run a different range standalone, set years_to_process before sourcing.
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

  # ── 1) Load raw spending file ────────────────────────────────────────────────
  input_path <- paste0(cex_raw_dir, "ucc_spending", year_val, "_light.csv")

  if (!file.exists(input_path)) {
    message("Skipping ", year_val, " — input file not found:\n  ", input_path)
    next
  }

  dt <- fread(input_path, colClasses = c(ucc = "character", cost = "numeric"))

  # ── 2) Identify all non-spending columns to use as row identifiers ───────────
  id_cols <- setdiff(names(dt), c("ucc", "cost"))

  # ── 3) Aggregate: sum cost within each (household, ucc) combination ──────────
  dt_agg <- dt[, .(cost = sum(cost, na.rm = TRUE)), by = c(id_cols, "ucc")]

  # ── 4) Pivot long → wide ─────────────────────────────────────────────────────
  dt_wide <- dcast(
    dt_agg,
    as.formula(paste(paste(id_cols, collapse = " + "), "~ ucc")),
    value.var = "cost",
    fill      = 0
  )

  # ── 5) Prefix UCC column names to avoid numeric-name issues ─────────────────
  ucc_cols <- setdiff(names(dt_wide), id_cols)
  setnames(dt_wide, ucc_cols, paste0("ucc_", ucc_cols))

  # ── 6) Save ──────────────────────────────────────────────────────────────────
  output_path <- paste0(year_folder, "ucc_spending", year_val, "_light_WIDE.csv")
  fwrite(dt_wide, output_path)

  message("Step 1a done: ", year_val, " → ", output_path)

  rm(dt, dt_agg, dt_wide)
}

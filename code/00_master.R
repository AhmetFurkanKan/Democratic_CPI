################################################################################
# 00_master.R
#
# Master orchestration script for the Distributional CPI and LA-AIDS thesis
# pipeline.
#
# PIPELINE OVERVIEW
# -----------------
# The full pipeline has two major phases that each end by running the DCPI
# CPI calculation engine (Jaravel 2024):
#
#   PRE-HURDLE PHASE  (Steps 1–3)
#     Reshape raw CEX spending files into wide category-level tables, then
#     compute preliminary annualised budget shares combining Interview (fmli)
#     and Diary (fmld) respondents.
#
#   HURDLE & FUSION PHASE  (Steps 4–5)
#     Estimate a two-part LASSO hurdle model to harmonise interview and diary
#     spending patterns.  Match each Interview household to a Diary donor via
#     k-NN and fuse their raw UCC spending into a single quarterly file.
#
#   POST-FUSION PHASE  (Steps 6–7)
#     Re-run the category reshaping and budget-share calculation on the fused
#     data.  The step-7 output is the *final* set of household budget shares.
#     It is also the input to the two separate CPI pipelines below.
#
#   DEMAND ANALYSIS  (Step 9)
#     Estimate a Linear Approximate Almost Ideal Demand System (LA/AIDS) using
#     pseudo-panel aggregation over region × month × income quintile cells.
#
# HOW TO USE
# ----------
#  1. Fill in every path in Section 1 below (only that section needs to
#     change across machines).
#  2. Flip the RUN_STEP_* flags in Section 2 to TRUE/FALSE to run only the
#     steps you need.
#  3. Source this file:  source("code/00_master.R")
#
# NOTE ON SEPARATE PIPELINES
# ---------------------------
# The CPI and DCPI calculations are run as SEPARATE processes:
#   0_CPI/0a.master.R   — run standalone to replicate the plutocratic CPI
#   8_DCPI/0a.master.R  — run standalone AFTER this 00_master.R to produce Distributional
#                         CPIs using the fused budget shares from post_hurdle_dir
# This master does NOT trigger either of those pipelines.
#
################################################################################


# ==============================================================================
# SECTION 1 — PATH CONFIGURATION
# Edit ONLY this section, all other sections read from these variables.
# ==============================================================================

# ── 1a. Root of the project (the folder that contains /code, /data, /output) ──
project_root <- "path/to/your/project/"           # <<< EDIT THIS

# ── 1b. Code directory ────────────────────────────────────────────────────────
code_dir <- paste0(project_root, "code/")

# ── 1c. Raw CEX light spending files ─────────────────────────────────────────
#   Expected files:  ucc_spending{YYYY}_light.csv  for each year
cex_raw_dir <- paste0(project_root, "data/CEX_light/")

# ── 1d. Reference / crosswalk files (shared across multiple steps) ────────────
#   xwalk_by_yearquarter.csv   — UCC → ItemCode crosswalk (quarterly)
#   XWALK_Furkan_itemcode_to_category.csv  — ItemCode → Category hierarchy
xwalk_dir  <- paste0(project_root, "data/crosswalks/")
xwalk_path     <- paste0(xwalk_dir, "xwalk_by_yearquarter.csv")
hierarchy_path <- paste0(xwalk_dir, "XWALK_Furkan_itemcode_to_category.csv")

# ── 1e. Pre-hurdle intermediate outputs (steps 1–3) ──────────────────────────
pre_hurdle_dir <- paste0(project_root, "intermediate/pre_hurdle/")
#   Subfolder structure created automatically:
#     pre_hurdle_dir/{YYYY}/ucc_spending{YYYY}_light_WIDE.csv          (step 1a)
#     pre_hurdle_dir/{YYYY}/ItemCode_spending{YYYY}_light_WIDE.csv     (step 1b)
#     pre_hurdle_dir/{YYYY}/ItemCode_WGT_spending{YYYY}_light_WIDE.csv (step 1c)
#     pre_hurdle_dir/{YYYY}/2_category_spending{YYYY}_light_WIDE_with_rent.csv    (step 2a)
#     pre_hurdle_dir/{YYYY}/2_category_WGT_spending{YYYY}_light_WIDE_with_rent.csv (step 2b)
#     pre_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv      (step 3)

# ── 1f. Hurdle model results (step 4) ─────────────────────────────────────────
hurdle_results_dir <- paste0(project_root, "intermediate/hurdle_results/")
#   Subfolder per year:
#     hurdle_results_dir/{YYYY}/matched_interview_diary_{YYYY}.csv
#     hurdle_results_dir/{YYYY}/stage1_participation_stats_{YYYY}.csv
#     hurdle_results_dir/{YYYY}/stage2_intensity_stats_{YYYY}.csv
#     hurdle_results_dir/{YYYY}/stage2_validation_plot_{YYYY}.png
#     hurdle_results_dir/{YYYY}/stage3_match_distance_{YYYY}.png
#     hurdle_results_dir/{YYYY}/stage3_donor_reuse_{YYYY}.png
#     hurdle_results_dir/{YYYY}/stage3_diary_balance_{YYYY}.csv

# ── 1g. Fused CEX spending files (step 5) ─────────────────────────────────────
fused_cex_dir <- paste0(project_root, "intermediate/fused_CEX/")
#   Files:  ucc_spending{YYYY}.csv  (quarterly, interview + diary donor rows)

# ── 1h. Post-fusion intermediate outputs (steps 6–7) ─────────────────────────
post_hurdle_dir <- paste0(project_root, "intermediate/post_hurdle/")
#   Subfolder per year:
#     post_hurdle_dir/{YYYY}/2_category_WGT_spending{YYYY}_light_WIDE_with_rent.csv (step 6)
#     post_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv                   (step 7)

# ── 1i. AIDS analysis outputs (step 9) ───────────────────────────────────────
regional_prices_path <- paste0(project_root,
                               "data/regional_prices/region_category_prices.csv")
aids_output_dir <- paste0(project_root, "output/aids_analysis/")


# ==============================================================================
# SECTION 2 — RUN-STEP FLAGS
# ==============================================================================
# Set each flag to TRUE to run that step, FALSE to skip it.
# You can safely skip steps whose outputs already exist on disk.
# ==============================================================================

RUN_STEP_1A  <- FALSE   # Wide UCC pivot (no xwalk) — exploratory / DCPI input
RUN_STEP_1B  <- FALSE   # Wide ItemCode pivot (unweighted)
RUN_STEP_1C  <- FALSE   # Wide ItemCode pivot (weighted)
RUN_STEP_2A  <- FALSE   # Wide Category with rent (unweighted) — exploratory
RUN_STEP_2B  <- TRUE    # Wide Category with rent (weighted) — feeds step 3
RUN_STEP_3   <- TRUE    # Preliminary budget shares (fmli + fmld, v1)
RUN_STEP_4   <- TRUE    # Two-part LASSO hurdle model + k-NN data fusion
RUN_STEP_5   <- TRUE    # Build fused quarterly UCC spending files
RUN_STEP_6   <- TRUE    # Post-fusion wide category reshape (equivalent of 2b)
RUN_STEP_7   <- TRUE    # Final budget shares (fmli only, v3) — main output
RUN_STEP_9   <- TRUE    # LA/AIDS demand system estimation


# ==============================================================================
# SECTION 3 — PACKAGES
# ==============================================================================

packages_required <- c(
  # Core data handling
  "data.table", "dplyr", "tidyr", "purrr",
  # Modelling
  "glmnet", "FNN", "systemfit",
  # Visualisation
  "ggplot2",
  # Utilities
  "stringr", "lubridate", "fixest"
)

install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    message("Installing package: ", pkg)
    install.packages(pkg, dependencies = TRUE)
  }
  library(pkg, character.only = TRUE)
}

invisible(lapply(packages_required, install_if_missing))


# ==============================================================================
# SECTION 4 — DIRECTORY INITIALISATION
# ==============================================================================

dirs_to_create <- c(
  pre_hurdle_dir,
  hurdle_results_dir,
  fused_cex_dir,
  post_hurdle_dir,
  aids_output_dir
)

for (d in dirs_to_create) {
  if (!dir.exists(d)) {
    dir.create(d, recursive = TRUE)
    message("Created directory: ", d)
  }
}


# ==============================================================================
# SECTION 5 — PRE-HURDLE DATA PREPARATION
# These steps reshape the raw CEX UCC-level spending files into wider formats
# and aggregate to the category level.  Steps 1a–1c and 2a produce exploratory
# representations that are not consumed by the main hurdle pipeline.
# ONLY step 2b output is required downstream (by step 3).
#
# Raw inputs:    cex_raw_dir/ucc_spending{YYYY}_light.csv
#                xwalk_path, hierarchy_path
# ==============================================================================

# Step 1a: Wide UCC pivot (no crosswalk)
# Output:  pre_hurdle_dir/{YYYY}/ucc_spending{YYYY}_light_WIDE.csv
# Note:    Useful for ad-hoc exploration. Not consumed by steps 2–9.
if (RUN_STEP_1A) {
  message("\n=== STEP 1a: Wide UCC pivot ===")
  source(paste0(code_dir, "step1_reshape_cex/1a_wide_ucc.R"))
}

# Step 1b: Wide ItemCode pivot (unweighted)
# Output:  pre_hurdle_dir/{YYYY}/ItemCode_spending{YYYY}_light_WIDE.csv
if (RUN_STEP_1B) {
  message("\n=== STEP 1b: Wide ItemCode pivot (unweighted) ===")
  source(paste0(code_dir, "step1_reshape_cex/1b_wide_itemcode.R"))
}

# Step 1c: Wide ItemCode pivot (weighted)
# Output:  pre_hurdle_dir/{YYYY}/ItemCode_WGT_spending{YYYY}_light_WIDE.csv
if (RUN_STEP_1C) {
  message("\n=== STEP 1c: Wide ItemCode pivot (weighted) ===")
  source(paste0(code_dir, "step1_reshape_cex/1c_wide_wgt_itemcode.R"))
}

# Step 2a: Wide Category with rent (unweighted)
# Output:  pre_hurdle_dir/{YYYY}/2_category_spending{YYYY}_light_WIDE_with_rent.csv
# Note:    Exploratory / validation only.  Not consumed downstream.
if (RUN_STEP_2A) {
  message("\n=== STEP 2a: Wide Category with rent (unweighted) ===")
  source(paste0(code_dir, "step2_category_aggregation/2a_wide_category.R"))
}

# Step 2b: Wide Category with rent (weighted)
# Output:  pre_hurdle_dir/{YYYY}/2_category_WGT_spending{YYYY}_light_WIDE_with_rent.csv
# *** This output is REQUIRED by step 3. ***
if (RUN_STEP_2B) {
  message("\n=== STEP 2b: Wide Category with rent (weighted) ===")
  source(paste0(code_dir, "step2_category_aggregation/2b_wide_wgt_category.R"))
}

# Step 3: Preliminary annualised budget shares (v1)
# Input:   pre_hurdle_dir/{YYYY}/2_category_WGT_spending{YYYY}_light_WIDE_with_rent.csv
# Output:  pre_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv
# Note:    Combines fmli (Interview) and fmld (Diary).
#          This is a PRELIMINARY version used only as input to the hurdle model
#          (step 4).
if (RUN_STEP_3) {
  message("\n=== STEP 3: Preliminary budget shares (v1, fmli + fmld) ===")
  source(paste0(code_dir, "step3_preliminary_shares/3_budget_shares_v1.R"))
}


# ==============================================================================
# SECTION 6 — HURDLE MODEL & DATA FUSION
# Step 4 reads the step-3 budget shares and estimates a two-part LASSO hurdle
# model (participation + intensity) for each spending category, then matches
# every Interview household to a Diary donor via k-NN.
#
# Step 5 uses that matched table to build a unified quarterly spending file
# combining Interview own-records with Diary donor records.
# Years covered: 1999–2021
# ==============================================================================

# Step 4: Two-part LASSO hurdle model + k-NN data fusion
# Input:   pre_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv
# Outputs: hurdle_results_dir/{YYYY}/matched_interview_diary_{YYYY}.csv
#          hurdle_results_dir/{YYYY}/stage1_participation_stats_{YYYY}.csv
#          hurdle_results_dir/{YYYY}/stage2_intensity_stats_{YYYY}.csv
#          hurdle_results_dir/{YYYY}/stage2_validation_plot_{YYYY}.png
#          hurdle_results_dir/{YYYY}/stage3_match_distance_{YYYY}.png
#          hurdle_results_dir/{YYYY}/stage3_donor_reuse_{YYYY}.png
#          hurdle_results_dir/{YYYY}/stage3_diary_balance_{YYYY}.csv
if (RUN_STEP_4) {
  message("\n=== STEP 4: Two-part hurdle model + k-NN matching ===")
  source(paste0(code_dir, "step4_hurdle_fusion/4_hurdle_matching.R"))
}

# Step 5: Build fused quarterly UCC spending files
# Input:   hurdle_results_dir/{YYYY}/matched_interview_diary_{YYYY}.csv
#          cex_raw_dir/ucc_spending{YYYY}_light.csv  (raw UCC costs)
# Output:  fused_cex_dir/ucc_spending{YYYY}.csv
if (RUN_STEP_5) {
  message("\n=== STEP 5: Build fused quarterly spending files ===")
  source(paste0(code_dir, "step4_hurdle_fusion/5_fuse_data.R"))
}


# ==============================================================================
# SECTION 7 — POST-FUSION PREPARATION
# Steps 6 and 7 mirror steps 2b and 3 exactly, but operate on the FUSED data
# from step 5 instead of the raw CEX files, and cover the full 1999–2021 range.
#
# Step 7 produces the final household-level budget shares used both by the DCPI
# CPI engine (step 8) and by the LA/AIDS demand system (step 9).
# ==============================================================================

# Step 6: Post-fusion wide category reshape
# Input:   fused_cex_dir/ucc_spending{YYYY}.csv
#          xwalk_path, hierarchy_path
# Output:  post_hurdle_dir/{YYYY}/2_category_WGT_spending{YYYY}_light_WIDE_with_rent.csv
# Note:    Functionally equivalent to step 2b but applied to fused spending.
if (RUN_STEP_6) {
  message("\n=== STEP 6: Post-fusion wide category reshape ===")
  source(paste0(code_dir, "step6_postfusion_prep/6_wide_wgt_category_fused.R"))
}

# Step 7: Final annualised budget shares (v3)
# Input:   post_hurdle_dir/{YYYY}/2_category_WGT_spending{YYYY}_light_WIDE_with_rent.csv
# Output:  post_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv
# Note:    Interview (fmli) households only.  This is the PRODUCTION version of
#          the budget shares.  Used by step 9 and by 8_DCPI/0a.master.R.
if (RUN_STEP_7) {
  message("\n=== STEP 7: Final budget shares (v3, fmli only) ===")
  source(paste0(code_dir, "step6_postfusion_prep/7_budget_shares_v3.R"))
}


# ==============================================================================
# SECTION 8 — LA/AIDS DEMAND SYSTEM
# Estimates a Linear Approximate Almost Ideal Demand System using pseudo-panel
# aggregation (Region × Month × Income quintile).
# Inputs:
#   post_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv   (step 7)
#   regional_prices_path   (FRED regional price series by category)
# Outputs:
#   aids_output_dir/full_elasticity_matrices_SUR.csv
#   aids_output_dir/own_price_summary_SUR.csv
#   aids_output_dir/micro_substitution_gap_SUR_LA_AIDS.png
# ==============================================================================

if (RUN_STEP_9) {
  message("\n=== STEP 9: LA/AIDS demand system estimation ===")
  source(paste0(code_dir, "step9_analysis/9_aids_substitution.R"))
}


# ==============================================================================
# DONE

message("\n================================================================")
message("PIPELINE COMPLETE")
message("================================================================")
message("Budget shares (final):  ", post_hurdle_dir)
message("AIDS output:            ", aids_output_dir)
message("")
message("Next: run 8_DCPI/0a.master.R separately to produce Distributional CPIs.")
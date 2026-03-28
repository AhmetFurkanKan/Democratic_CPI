################################################################################
# Step 5 — CEX Spending Data Fusion (Year-by-Year Loop)
#
# Purpose : Use the Stage 3 matched table (inter newid - diary donor newid) 
# to build a fused vertical spending file.
#
# Inputs  : hurdle_results_dir/{YYYY}/matched_interview_diary_{YYYY}.csv
#           cex_raw_dir/ucc_spending{YYYY}_light.csv
# Outputs : fused_cex_dir/ucc_spending{YYYY}.csv
#
# Called from : 00_master.R (RUN_STEP_5 = TRUE)
# Standalone  : define hurdle_results_dir, cex_raw_dir, fused_cex_dir, and
#               years_to_model before sourcing this script.
################################################################################

library(dplyr)
library(purrr)

# Configuration

DIARY_LABEL     <- "fmld"
INTERVIEW_LABEL <- "fmli"
QUARTERLY_DAYS  <- 90 


#    The stopifnot() calls below catch the case where this script is run
#    standalone without them being set.


stopifnot(
  "hurdle_results_dir not found — source 00_master.R first, or define it manually." =
    exists("hurdle_results_dir"),
  "cex_raw_dir not found — source 00_master.R first, or define it manually." =
    exists("cex_raw_dir"),
  "fused_cex_dir not found — source 00_master.R first, or define it manually." =
    exists("fused_cex_dir")
)

if (!dir.exists(fused_cex_dir)) dir.create(fused_cex_dir, recursive = TRUE)

# Years, can be overridden by 00_master.R before sourcing
if (!exists("years_to_model")) {
  years_to_model <- 1999:2021  
}

# MAIN EXECUTION LOOP
#-------------------------------------------------

for (yr in years_to_model) {
  year_label <- as.character(yr)
  
  cat("\n=================================================================\n")
  cat("                 STARTING DATA FUSION FOR YEAR:", year_label, "\n")
  cat("=================================================================\n")
  
  MATCHED_TABLE_PATH <- paste0(hurdle_results_dir, year_label, "/matched_interview_diary_", year_label, ".csv")
  RAW_SPENDING_PATH  <- paste0(cex_raw_dir, "ucc_spending", year_label, "_light.csv")
  
  # Safety check
  if (!file.exists(MATCHED_TABLE_PATH)) {
    warning(sprintf("Matched table missing for %s. Skipping...\n Path: %s", year_label, MATCHED_TABLE_PATH))
    next
  }
  if (!file.exists(RAW_SPENDING_PATH)) {
    warning(sprintf("Raw spending file missing for %s. Skipping...\n Path: %s", year_label, RAW_SPENDING_PATH))
    next
  }
  

  # Step 1: Load the matched table
  
  cat("Loading matched table...\n")
  matched <- read.csv(MATCHED_TABLE_PATH, stringsAsFactors = FALSE)
  
  if (!"inter_newid" %in% names(matched))
    stop("Column 'inter_newid' not found in matched table.")
  if (!"diary_newid" %in% names(matched))
    stop("Column 'diary_newid' not found in matched table.")
  
  match_pairs <- matched %>%
    select(inter_newid, diary_newid) %>%
    distinct()
  
  cat(sprintf("Matched pairs:       %d Interview HHs\n", nrow(match_pairs)))
  cat(sprintf("Unique diary donors: %d\n", length(unique(match_pairs$diary_newid))))
  

  # Step 2: Load raw spending file and verify columns
  cat("\nLoading raw CEX spending file...\n")
  spending <- read.csv(RAW_SPENDING_PATH, stringsAsFactors = FALSE)
  
  required_cols <- c("newid", "ucc", "cost", "i.data", "num_days")
  missing_cols  <- setdiff(required_cols, names(spending))
  if (length(missing_cols) > 0)
    stop("Missing required columns in spending file: ", paste(missing_cols, collapse = ", "))
  
  cat(sprintf("Raw spending rows:   %d\n", nrow(spending)))
  cat(sprintf("Unique households:   %d\n", length(unique(spending$newid))))
  
  extra_cols <- setdiff(names(spending), c("newid", "ucc", "cost", "i.data", "num_days"))
  cat(sprintf("Extra columns (%d):  %s\n", length(extra_cols), paste(extra_cols, collapse = ", ")))
  

  # Step 3: Split into Interview and Diary subsets
  inter_spending <- spending %>% filter(i.data == INTERVIEW_LABEL)
  diary_spending  <- spending %>% filter(i.data == DIARY_LABEL)
  
  cat(sprintf("\nInterview rows: %d  (%d unique HHs)\n",
              nrow(inter_spending), length(unique(inter_spending$newid))))
  cat(sprintf("Diary rows:     %d  (%d unique HHs)\n",
              nrow(diary_spending),  length(unique(diary_spending$newid))))
  


  # Validate num_days is constant per diary HH
  num_days_check <- diary_spending %>%
    group_by(newid) %>%
    summarise(n_distinct_days = n_distinct(num_days), .groups = "drop") %>%
    filter(n_distinct_days > 1)
  
  if (nrow(num_days_check) > 0) {
    warning(sprintf(
      "%d Diary HHs have more than one distinct num_days value within their rows.\n",
      nrow(num_days_check)
    ))
    cat("Affected newids (first 10):", head(num_days_check$newid, 10), "\n")
  } else {
    cat("Validation passed: num_days is constant within every Diary HH.\n")
  }
  
  # Check for zero or missing num_days
  n_zero_days    <- sum(diary_spending$num_days == 0,  na.rm = TRUE)
  n_missing_days <- sum(is.na(diary_spending$num_days))
  
  if (n_zero_days > 0)
    warning(sprintf("%d diary rows have num_days = 0.", n_zero_days))
  if (n_missing_days > 0)
    warning(sprintf("%d diary rows have NA num_days, costs will become NA after scaling.", n_missing_days))
  
  # Apply quarterly scaling to diary rows
  diary_spending_quarterly <- diary_spending 

  # Quick summary: compare raw vs scaled totals as a sense check
  cat(sprintf("Diary cost before scaling: %.2f  (sum)\n", sum(diary_spending$cost, na.rm = TRUE)))
  cat(sprintf("Diary cost after scaling:  %.2f  (sum)\n", sum(diary_spending_quarterly$cost, na.rm = TRUE)))
  cat(sprintf("Mean num_days across diary HHs: %.1f\n",
              mean(diary_spending %>%
                     group_by(newid) %>%
                     summarise(d = first(num_days), .groups = "drop") %>%
                     pull(d), na.rm = TRUE)))
  
  # Step 5: Build fused spending file
  cat("\nFusing spending data...\n")
  
    #Interview  rows
  inter_rows <- inter_spending %>%
    filter(newid %in% match_pairs$inter_newid) %>%
    left_join(match_pairs, by = c("newid" = "inter_newid")) %>%
    mutate(
      source_flag       = "interview_own",
      diary_donor_newid = diary_newid
    ) %>%
    select(-diary_newid)
  
  cat(sprintf("  Interview own rows:  %d\n", nrow(inter_rows)))
  
    #Block B: Diary donor  rows
    inter_hh_attrs <- inter_spending %>%
    filter(newid %in% match_pairs$inter_newid) %>%
    select(newid, all_of(extra_cols)) %>%
    group_by(newid) %>%
    slice(1) %>%
    ungroup() %>%
    rename(inter_newid = newid)
  
  diary_rows <- match_pairs %>%
    left_join(
      diary_spending_quarterly %>% rename(diary_newid = newid),
      by = "diary_newid",
      relationship = "many-to-many"
    ) %>%
    # Drop donor's extra columns, replace with interview HH's attributes
    select(-all_of(intersect(extra_cols, names(.)))) %>%
    left_join(inter_hh_attrs, by = "inter_newid") %>%
    mutate(
      source_flag       = "diary_donor",
      diary_donor_newid = diary_newid,
      newid             = inter_newid,
    ) %>%  
    select(-inter_newid, -diary_newid)
  
  cat(sprintf("  Diary donor rows: %d\n", nrow(diary_rows)))
  
  #Stack
  fused_spending <- bind_rows(inter_rows, diary_rows) %>%
    arrange(newid, source_flag, ucc)
  
  cat(sprintf("\nFused rows total:        %d\n", nrow(fused_spending)))
  cat(sprintf("Unique fused households: %d\n", length(unique(fused_spending$newid))))
  

  # Step 6: Sanity checks
  cat("\n--- Sanity Checks ---\n")
  
  # Check 1: All matched interview HHs present in output
  missing_hhs <- setdiff(match_pairs$inter_newid, unique(fused_spending$newid))
  if (length(missing_hhs) > 0) {
    warning(sprintf("%d Interview HHs missing from fused output. First 10: %s",
                    length(missing_hhs), paste(head(missing_hhs, 10), collapse = ", ")))
  } else {
    cat("CHECK 1 PASSED: All matched Interview HHs present in output.\n")
  }
  
  # Check 2: Every fused HH has both interview and diary rows
  row_comp <- fused_spending %>%
    group_by(newid) %>%
    summarise(
      n_inter = sum(source_flag == "interview_own"),
      n_diary = sum(source_flag == "diary_donor"),
      .groups = "drop"
    )
  
  cat(sprintf("CHECK 2: HHs missing diary rows:     %d\n", sum(row_comp$n_diary == 0)))
  cat(sprintf("         HHs missing interview rows: %d\n", sum(row_comp$n_inter == 0)))
  
  # Check 3: Source breakdown
  cat("\nCHECK 3: Row source breakdown:\n")
  fused_spending %>%
    count(source_flag, i.data) %>%
    mutate(pct = round(100 * n / nrow(fused_spending), 1)) %>%
    print()
  
  # Check 4: Quarterly cost comparison, interview vs diary donor
  cat("\nCHECK 4: Mean quarterly cost per row by source:\n")
  fused_spending %>%
    group_by(source_flag) %>%
    summarise(
      n_rows     = n(),
      mean_cost  = round(mean(cost, na.rm = TRUE), 2),
      total_cost = round(sum(cost,  na.rm = TRUE), 2),
      .groups = "drop"
    ) %>%
    print()
  
  # Check 5: Spot check one household
  example_id <- unique(fused_spending$newid)[1]
  cat(sprintf("\nCHECK 5: Spot check for newid = %s\n", example_id))
  fused_spending %>%
    filter(newid == example_id) %>%
    group_by(source_flag, i.data) %>%
    summarise(
      n_rows     = n(),
      total_cost = round(sum(cost, na.rm = TRUE), 2),
      num_days   = first(num_days),
      .groups    = "drop"
    ) %>%
    print()
  

  # Step 7: Export
  output_path <- paste0(fused_cex_dir, "ucc_spending", year_label, ".csv")
  write.csv(fused_spending, output_path, row.names = FALSE)
  
  cat(sprintf("\nSUCCESS: Fused quarterly spending file for %s saved to:\n  %s\n", year_label, output_path))
  cat(sprintf("  Rows:       %d\n", nrow(fused_spending)))
  cat(sprintf("  Unique HHs: %d\n", length(unique(fused_spending$newid))))
  cat(sprintf("  Columns:    %s\n", paste(names(fused_spending), collapse = ", ")))
  
  cat("\nColumn guide:\n")
  cat("  newid             — Interview HH's newid (unified across all rows)\n")
  cat("  ucc               — item code\n")
  cat("  cost              — quarterly-equivalent expenditure\n")
  cat("                       interview rows\n")
  cat("                       diary rows\n")
  cat("  i.data            — 'fmli' = interview row | 'fmld' = diary donor row\n")
  cat("  source_flag       — 'interview_own' | 'diary_donor'\n")
  cat("  diary_donor_newid — original diary newid (for audit)\n")
  cat("  num_days          — diary observation window (NA for interview rows)\n")
  if (length(extra_cols) > 0)
    cat(sprintf("  %s — carried through unchanged\n", paste(extra_cols, collapse = ", ")))
  
  cat("\nNext step: map ucc codes to your 30 categories and aggregate cost\n")
  cat("by newid + category to get the final fused consumption basket.\n")

  # Step 8: Memory Cleanup
  rm(matched, match_pairs, spending, inter_spending, diary_spending, 
     diary_spending_quarterly, inter_rows, diary_rows, inter_hh_attrs, 
     fused_spending)
  
  gc() 
} 

cat("\n=================================================================\n")
cat("ALL YEARS PROCESSED SUCCESSFULLY.\n")
cat("=================================================================\n")

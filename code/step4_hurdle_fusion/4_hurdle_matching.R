################################################################################
# Step 4 — Two-Part Hurdle Model + k-NN Data Fusion
#
# Structure:
#   Stage 1 — Participation models (LASSO logistic)
#   Stage 2 — Intensity models    (LASSO linear on log-share)
#   Stage 3 — Matching: one row per Interview HH, Diary HH is the donor
#              Donor selected by uniform sampling among k nearest neighbors.
#
# Output CSV columns:
#   inter_*                — Interview household's own data
#   diary_*                — matched Diary donor's data
#   match_distance         — distance to chosen donor
#   diary_donor_row_index  — which Diary row was selected (for audit)
#   diary_donor_reuse_flag — TRUE if this Diary donor was already used
#
# Inputs  : pre_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv
# Outputs : hurdle_results_dir/{YYYY}/matched_interview_diary_{YYYY}.csv
#           hurdle_results_dir/{YYYY}/stage1_participation_stats_{YYYY}.csv
#           hurdle_results_dir/{YYYY}/stage2_intensity_stats_{YYYY}.csv
#           hurdle_results_dir/{YYYY}/stage2_validation_plot_{YYYY}.png
#           hurdle_results_dir/{YYYY}/stage3_match_distance_{YYYY}.png
#           hurdle_results_dir/{YYYY}/stage3_donor_reuse_{YYYY}.png
#           hurdle_results_dir/{YYYY}/stage3_diary_balance_{YYYY}.csv
#
# Called from : 00_master.R (RUN_STEP_4 = TRUE)
# Standalone  : define pre_hurdle_dir, hurdle_results_dir, and years_to_model
#               before sourcing this script.
################################################################################

library(glmnet)   # LASSO regularization
library(dplyr)    # Data manipulation
library(tidyr)    # Data reshaping
library(ggplot2)  # Visualization
library(purrr)    # Functional programming
library(FNN)      # Fast Nearest Neighbor

################################################################################
# Configuration — adjust labels to match your actual data
################################################################################

categories <- c(
  "Alcoholic_beverages", "Apparel", "Dairy_and_related_products",
  "Education_and_communication_commodities", "Education_and_communication_services",
  "Electricity", "Food_at_employee_sites_and_schools",
  "Food_from_vending_machines_and_mobile_vendors", "Fruits_and_vegetables",
  "Fuel_oil_and_other_fuels", "Full_service_meals_and_snacks",
  "Household_furnishings_and_supplies", "Household_operations",
  "Limited_service_meals_and_snacks", "Medical_care_commodities",
  "Medical_care_services", "Motor_fuel",
  "Nonalcoholic_beverages_and_beverage_materials", "Other_food_at_home",
  "Other_food_away_from_home", "Other_goods", "Other_personal_services",
  "Recreation_commodities", "Recreation_services", "Rent_of_shelter",
  "Transportation_commodities_less_motor_fuel", "Transportation_services",
  "Utility_piped_gas_service", "Water_and_sewer_and_trash_collection_services"
)

numeric_cols     <- c("age_ref", "educ_ref", "income_quintile")
categorical_cols <- c("bls_urbn", "region", "ref_race")

DIARY_LABEL     <- "fmld"   # value of i.data for Diary records
INTERVIEW_LABEL <- "fmli"   # value of i.data for Interview records
K_CANDIDATES    <- 10       # nearest Diary neighbours to consider per Interview HH
SEED            <- 42       # reproducibility

base::cat("Number of categories:", length(categories), "\n")

################################################################################
# Helper: prepare_data
################################################################################

prepare_data <- function(data) {
  
  # Handle missing categorical values
  for (cat_var in categorical_cols) {
    if (cat_var %in% names(data)) {
      data[[cat_var]] <- ifelse(
        is.na(data[[cat_var]]) | data[[cat_var]] == "",
        "Unknown", as.character(data[[cat_var]])
      )
    }
  }
  
  # Apply 0.1% threshold and create hurdle variables
  for (cat_name in categories) {
    share_col       <- paste0("share_", cat_name)
    participate_col <- paste0("participate_", cat_name)
    logshare_col    <- paste0("logshare_", cat_name)
    
    if (share_col %in% names(data)) {
      data[[share_col]]       <- ifelse(data[[share_col]] < 0.001, 0, data[[share_col]])
      data[[participate_col]] <- as.numeric(!is.na(data[[share_col]]) & data[[share_col]] > 0)
      data[[logshare_col]]    <- ifelse(data[[participate_col]] == 1, log(data[[share_col]]), NA)
    }
  }
  
  available_predictors <- intersect(c(numeric_cols, categorical_cols), names(data))
  base::cat(sprintf(
    "\nData prepared: %d predictor variables available. Shares < 0.1%% treated as 0.\n",
    length(available_predictors)
  ))
  return(data)
}

################################################################################
# Helper: build_model_matrix
################################################################################

build_model_matrix <- function(data, include_weights = FALSE, reference_data = NULL) {
  
  for (var in categorical_cols) {
    if (var %in% names(data)) {
      levs <- if (!is.null(reference_data) && var %in% names(reference_data)) {
        sort(unique(as.character(reference_data[[var]])))
      } else {
        sort(unique(as.character(data[[var]])))
      }
      data[[var]] <- factor(as.character(data[[var]]), levels = levs)
    }
  }
  
  available_numeric     <- intersect(numeric_cols, names(data))
  available_categorical <- intersect(categorical_cols, names(data))
  
  formula_parts <- available_numeric
  if ("age_ref" %in% available_numeric) formula_parts <- c(formula_parts, "I(age_ref^2)")
  formula_parts <- c(formula_parts, available_categorical)
  
  formula_str <- paste("~", paste(formula_parts, collapse = " + "))
  formula_obj <- as.formula(formula_str)
  X           <- model.matrix(formula_obj, data = data)
  
  weights <- NULL
  if (include_weights && "popwt" %in% names(data)) {
    weights <- data[["popwt"]][complete.cases(data[, all.vars(formula_obj)])]
  }
  
  base::cat("\nUsing formula:", formula_str, "\n")
  base::cat(sprintf("Numeric predictors    (%d): %s\n",
                    length(available_numeric), paste(available_numeric, collapse = ", ")))
  base::cat(sprintf("Categorical predictors (%d): %s\n",
                    length(available_categorical), paste(available_categorical, collapse = ", ")))
  
  return(list(
    X              = X,
    weights        = weights,
    complete_cases = complete.cases(data[, all.vars(formula_obj)])
  ))
}

################################################################################
# Stage 1: Participation Models (LASSO logistic)
################################################################################

estimate_participation_models <- function(data, categories, cv_folds = 5, alpha = 1) {
  
  base::cat("\n=== STAGE 1: ESTIMATING PARTICIPATION MODELS ===\n")
  
  model_data   <- build_model_matrix(data, include_weights = TRUE, reference_data = data)
  X            <- model_data$X
  weights      <- model_data$weights
  complete_idx <- which(model_data$complete_cases)
  
  base::cat(sprintf("Model matrix: %d rows x %d columns\n", nrow(X), ncol(X)))
  
  participation_models <- list()
  participation_stats  <- data.frame()
  
  for (i in seq_along(categories)) {
    current_cat <- categories[i]
    base::cat(sprintf("\n[%d/%d] %s\n", i, length(categories), current_cat))
    
    p_col <- paste0("participate_", current_cat)
    if (!p_col %in% names(data)) next
    
    y <- data[[p_col]][complete_idx]
    if (length(unique(y)) < 2) {
      base::cat("  Skipped: only one class present\n")
      next
    }
    
    cv_fit    <- cv.glmnet(X, y, family = "binomial", alpha = alpha,
                           nfolds = cv_folds, weights = weights)
    final_fit <- glmnet(X, y, family = "binomial", alpha = alpha,
                        lambda = cv_fit$lambda.min, weights = weights)
    
    participation_models[[current_cat]] <- list(
      model      = final_fit,
      lambda_min = cv_fit$lambda.min
    )
    
    dev_explained <- 1 - cv_fit$cvm[cv_fit$lambda == cv_fit$lambda.min] / cv_fit$cvm[1]
    participation_stats <- rbind(participation_stats, data.frame(
      category           = current_cat,
      participation_rate = mean(y),
      cv_lambda_min      = cv_fit$lambda.min,
      num_nonzero_coefs  = sum(coef(final_fit) != 0) - 1,
      deviance_explained = dev_explained
    ))
  }
  
  return(list(models = participation_models, stats = participation_stats))
}

################################################################################
# Stage 2: Intensity Models (LASSO on log-share, participants only)
################################################################################

estimate_intensity_models <- function(data, categories, cv_folds = 5, alpha = 1) {
  
  base::cat("\n=== STAGE 2: ESTIMATING INTENSITY MODELS ===\n")
  
  intensity_models <- list()
  intensity_stats  <- data.frame()
  
  for (i in seq_along(categories)) {
    current_cat <- categories[i]
    base::cat(sprintf("\n[%d/%d] %s\n", i, length(categories), current_cat))
    
    p_col        <- paste0("participate_", current_cat)
    ls_col       <- paste0("logshare_", current_cat)
    participants <- !is.na(data[[p_col]]) & data[[p_col]] == 1 & !is.na(data[[ls_col]])
    
    if (sum(participants) < 50) {
      base::cat(sprintf("  Skipped: only %d participants (need >= 50)\n", sum(participants)))
      next
    }
    
    data_part  <- data[participants, ]
    model_data <- build_model_matrix(data_part, include_weights = TRUE, reference_data = data)
    
    X <- model_data$X
    y <- data_part[[ls_col]][model_data$complete_cases]
    
    cv_fit    <- cv.glmnet(X, y, alpha = alpha, nfolds = cv_folds,
                           weights = model_data$weights)
    final_fit <- glmnet(X, y, alpha = alpha, lambda = cv_fit$lambda.min,
                        weights = model_data$weights)
    
    intensity_models[[current_cat]] <- list(
      model      = final_fit,
      lambda_min = cv_fit$lambda.min
    )
    
    y_pred <- predict(final_fit, newx = X, s = cv_fit$lambda.min)
    r_sq   <- 1 - (sum((y - y_pred)^2) / sum((y - mean(y))^2))
    
    intensity_stats <- rbind(intensity_stats, data.frame(
      category          = current_cat,
      n_participants    = sum(participants),
      mean_logshare     = mean(y),
      cv_lambda_min     = cv_fit$lambda.min,
      num_nonzero_coefs = sum(coef(final_fit) != 0) - 1,
      r_squared         = r_sq
    ))
  }
  
  return(list(models = intensity_models, stats = intensity_stats))
}


################################################################################
# Predict shares  E[share] = P(participate) * E[share | participate] * Smearing
################################################################################

predict_shares <- function(data, part_res, int_res, categories) {
  
  base::cat("\n=== PREDICTING LATENT ANNUAL SHARES (WITH DUAN'S SMEARING) ===\n")
  
  model_data  <- build_model_matrix(data, reference_data = data)
  X           <- model_data$X
  pred_matrix <- matrix(0, nrow = nrow(X), ncol = length(categories))
  colnames(pred_matrix) <- categories
  
  for (current_cat in categories) {
    if (current_cat %in% names(part_res$models) &&
        current_cat %in% names(int_res$models)) {
      
      p_mod <- part_res$models[[current_cat]]
      i_mod <- int_res$models[[current_cat]]
      
      # 1. Predict Participation Probability
      p_hat <- predict(p_mod$model, newx = X, type = "response", s = p_mod$lambda_min)
      
      # 2. Predict Intensity (Log Share)
      log_y_hat <- predict(i_mod$model, newx = X, s = i_mod$lambda_min)
      
      # ------------------------------------------------------------------
      # NEW: Duan's Smearing Estimator to correct retransformation bias
      # ------------------------------------------------------------------
      ls_col <- paste0("logshare_", current_cat)
      p_col  <- paste0("participate_", current_cat)
      
      # Identify the actual participants used to train this category's intensity model
      participants <- !is.na(data[[p_col]]) & data[[p_col]] == 1 & !is.na(data[[ls_col]])
      
      if (sum(participants) > 0) {
        # Extract actual log-shares for participants
        actual_log_y <- data[[ls_col]][participants]
        
        # Predict log-shares specifically for these participants
        X_part <- X[participants, , drop = FALSE]
        pred_log_y_part <- predict(i_mod$model, newx = X_part, s = i_mod$lambda_min)
        
        # Calculate residuals: Actual - Predicted
        residuals <- actual_log_y - pred_log_y_part
        
        # Smearing Factor: Mean of exponentiated residuals
        smearing_factor <- mean(exp(residuals), na.rm = TRUE)
      } else {
        smearing_factor <- 1 # Fallback if no participants exist (rare)
      }
      
      # 3. Combine: P(participate) * exp(log-share) * smearing_factor
      pred_matrix[, current_cat] <- as.vector(p_hat * exp(log_y_hat) * smearing_factor)
      
      # Optional: Print the factor to monitor its impact
      # base::cat(sprintf("  %s smearing factor: %.4f\n", current_cat, smearing_factor))
    }
  }
  
  # Normalise rows to sum to 1
  row_sums  <- rowSums(pred_matrix)
  pred_norm <- pred_matrix
  pred_norm[row_sums > 0, ]                    <- pred_matrix[row_sums > 0, ] /
    row_sums[row_sums > 0]
  pred_norm[row_sums <= 0 | is.na(row_sums), ] <- NA
  
  return(list(
    predicted_shares = pred_norm,
    complete_cases   = model_data$complete_cases
  ))
}

################################################################################
# Validate predictions
################################################################################

validate_predictions <- function(data, predictions, categories) {
  
  base::cat("\n=== VALIDATION (WEIGHTED) ===\n")
  
  model_data <- build_model_matrix(data, reference_data = data)
  keep       <- model_data$complete_cases
  w          <- data$popwt[keep]
  pred       <- predictions$predicted_shares
  
  comparison <- data.frame(category = categories, actual = NA, predicted = NA)
  for (i in seq_along(categories)) {
    share_col               <- paste0("share_", categories[i])
    comparison$actual[i]    <- weighted.mean(data[[share_col]][keep], w = w, na.rm = TRUE)
    comparison$predicted[i] <- weighted.mean(pred[, i],              w = w, na.rm = TRUE)
  }
  
  base::cat(sprintf(
    "Correlation Actual vs Predicted: %.4f\n",
    stats::cor(comparison$actual, comparison$predicted, use = "complete.obs")
  ))
  return(comparison)
}

################################################################################
# Stage 3: Uniform k-NN Statistical Data Fusion
#
# Base table  : Interview households (larger sample — one output row each)
# Donor pool  : Diary households     (smaller sample)
#
# For each Interview HH:
#   1. Find K_CANDIDATES nearest Diary neighbours in
#      (demographics + predicted latent share) space
#   2. Sample ONE donor from those candidates using uniform sampling.
#
# Note on popwt:
#   Population weights were intentionally removed from the donor selection step.
#   Because diary popwt is inflated ~2x relative to interview popwt, using it
#   for sampling caused high-popwt (urban/wealthy) donors to over-dominate, 
#   which artificially collapsed food shares. We apply uniform sampling here 
#   and reserve popwt for computing post-fusion aggregate statistics.
################################################################################

perform_data_fusion <- function(data_with_preds, categories) {
  
  base::cat("\n=== STAGE 3: UNIFORM k-NN DATA FUSION ===\n")
  base::cat("Base table : Interview HHs (one output row each)\n")
  base::cat("Donor pool : Diary HHs     (sampled uniformly among k candidates)\n\n")
  
  # ------------------------------------------------------------------
  # 0. Guard: check required columns
  # ------------------------------------------------------------------
  if (!"i.data" %in% names(data_with_preds))
    stop("Column 'i.data' not found. Check DIARY_LABEL / INTERVIEW_LABEL.")
  if (!"popwt" %in% names(data_with_preds))
    stop("Column 'popwt' not found. Required for weighted donor sampling.")
  
  # ------------------------------------------------------------------
  # 1. Split surveys
  # ------------------------------------------------------------------
  inter_df <- data_with_preds[data_with_preds$i.data == INTERVIEW_LABEL, ]
  diary_df <- data_with_preds[data_with_preds$i.data == DIARY_LABEL,     ]
  
  rownames(inter_df) <- NULL
  rownames(diary_df) <- NULL
  
  base::cat(sprintf("Interview HHs (base):   %d\n", nrow(inter_df)))
  base::cat(sprintf("Diary HHs (donor pool): %d\n", nrow(diary_df)))
  
  if (nrow(diary_df) == 0 || nrow(inter_df) == 0)
    stop("One survey subset is empty. Check i.data labels.")
  
  # ------------------------------------------------------------------
  # 2. Bridge variables: demographics + Stage 2 predicted shares
  # ------------------------------------------------------------------
  pred_cols  <- intersect(paste0("pred_", categories), names(data_with_preds))
  demo_cols  <- intersect(
    c("age_ref", "educ_ref", "income_quintile", "bls_urbn", "region", "ref_race"),
    names(data_with_preds)
  )
  match_cols <- intersect(c(demo_cols, pred_cols), names(inter_df))
  match_cols <- intersect(match_cols, names(diary_df))
  
  base::cat(sprintf(
    "Bridge variables: %d total (%d demographic, %d predicted shares)\n",
    length(match_cols),
    length(intersect(demo_cols, match_cols)),
    length(intersect(pred_cols, match_cols))
  ))
  
  # ------------------------------------------------------------------
  # 3. Standardise on the combined pool
  # ------------------------------------------------------------------
  to_numeric_matrix <- function(df, cols) {
    mat <- df[, cols, drop = FALSE]
    mat <- as.data.frame(lapply(mat, function(x) as.numeric(as.factor(as.character(x)))))
    as.matrix(mat)
  }
  
  inter_mat    <- to_numeric_matrix(inter_df, match_cols)
  diary_mat    <- to_numeric_matrix(diary_df, match_cols)
  combined_mat <- rbind(inter_mat, diary_mat)
  
  col_means <- colMeans(combined_mat, na.rm = TRUE)
  col_sds   <- apply(combined_mat, 2, sd, na.rm = TRUE)
  col_sds[col_sds == 0] <- 1
  
  inter_scaled <- scale(inter_mat, center = col_means, scale = col_sds)
  diary_scaled <- scale(diary_mat, center = col_means, scale = col_sds)
  inter_scaled[is.na(inter_scaled)] <- 0
  diary_scaled[is.na(diary_scaled)] <- 0
  
  # ------------------------------------------------------------------
  # 4. k-NN search
  # ------------------------------------------------------------------
  k_actual <- min(K_CANDIDATES, nrow(diary_scaled))
  base::cat(sprintf(
    "\nFinding %d nearest Diary neighbours for each of %d Interview HHs...\n",
    k_actual, nrow(inter_scaled)
  ))
  
  knn_result <- FNN::get.knnx(
    data  = diary_scaled,   # donor pool
    query = inter_scaled,   # base table
    k     = k_actual
  )
  
  # ------------------------------------------------------------------
  # 5. Uniform donor sampling (among k nearest neighbors)
  # ------------------------------------------------------------------
  base::cat("Sampling donors uniformly from the k candidates...\n")
  set.seed(SEED)
  
  n_inter            <- nrow(inter_scaled)
  donor_row_in_diary <- integer(n_inter)
  match_distance     <- numeric(n_inter)
  
  for (i in seq_len(n_inter)) {
    candidate_rows    <- knn_result$nn.index[i, ]        # k Diary row indices
    candidate_dists   <- knn_result$nn.dist[i, ]         # their distances
    
    # Uniform sampling among k candidates — popwt removed from donor selection.
    # popwt should only be applied when computing post-fusion aggregate statistics,
    # not here, because diary popwt is inflated ~2x relative to interview popwt
    # due to the smaller diary sample size, which caused high-popwt (urban/wealthy)
    # donors to dominate and collapse food shares from ~15% to ~5%.
    chosen_position       <- sample(k_actual, size = 1)  # uniform, no popwt
    donor_row_in_diary[i] <- candidate_rows[chosen_position]
    match_distance[i]     <- candidate_dists[chosen_position]
  }
  
  # ------------------------------------------------------------------
  # 6. Assemble matched output table
  # ------------------------------------------------------------------
  base::cat("Assembling matched table...\n")
  
  inter_renamed        <- inter_df
  names(inter_renamed) <- paste0("inter_", names(inter_df))
  
  diary_donor          <- diary_df[donor_row_in_diary, ]
  rownames(diary_donor) <- NULL
  names(diary_donor)   <- paste0("diary_", names(diary_df))
  
  matched_table <- cbind(
    inter_renamed,
    diary_donor,
    data.frame(
      match_distance         = match_distance,
      diary_donor_row_index  = donor_row_in_diary,
      diary_donor_reuse_flag = duplicated(donor_row_in_diary)
    )
  )
  
  # ------------------------------------------------------------------
  # 7. Diagnostics
  # ------------------------------------------------------------------
  n_unique     <- length(unique(donor_row_in_diary))
  pct_unique   <- 100 * n_unique / nrow(diary_df)
  pct_reused   <- 100 * mean(duplicated(donor_row_in_diary))
  thresh_95    <- quantile(match_distance, 0.95)
  n_poor       <- sum(match_distance > thresh_95)
  
  base::cat("\n--- Match Quality Diagnostics ---\n")
  base::cat(sprintf("Mean match distance:            %.4f\n", mean(match_distance)))
  base::cat(sprintf("Median match distance:          %.4f\n", median(match_distance)))
  base::cat(sprintf("Max match distance:             %.4f\n", max(match_distance)))
  base::cat(sprintf("Unique Diary donors used:       %d / %d  (%.1f%%)\n",
                    n_unique, nrow(diary_df), pct_unique))
  base::cat(sprintf("Interview HHs sharing a donor: %.1f%%\n", pct_reused))
  base::cat(sprintf("Poor matches (> 95th pctile):  %d  (%.1f%%)\n",
                    n_poor, 100 * n_poor / nrow(matched_table)))
  
  # Demographic balance: used vs unused Diary donors
  used_flag <- seq_len(nrow(diary_df)) %in% unique(donor_row_in_diary)
  if (all(c("income_quintile", "age_ref", "popwt") %in% names(diary_df))) {
    base::cat("\n--- Diary Donor Balance (used vs unused) ---\n")
    bal <- diary_df %>%
      dplyr::mutate(used = used_flag) %>%
      dplyr::group_by(used) %>%
      dplyr::summarise(
        n           = dplyr::n(),
        mean_age    = mean(age_ref,         na.rm = TRUE),
        mean_income = mean(income_quintile,  na.rm = TRUE),
        mean_popwt  = mean(popwt,            na.rm = TRUE),
        .groups     = "drop"
      )
    print(bal)
    base::cat("If used/unused look similar, concentration is geometric (acceptable).\n")
    base::cat("If they differ substantially, consider increasing K_CANDIDATES.\n")
  }
  
  return(matched_table)
}

################################################################################
# Full Pipeline
################################################################################

run_hurdle_model_pipeline <- function(input_data) {
  
  prepared <- prepare_data(input_data)
  part_res <- estimate_participation_models(prepared, categories)
  int_res  <- estimate_intensity_models(prepared, categories)
  preds    <- predict_shares(prepared, part_res, int_res, categories)
  
  # Attach predicted shares — rows align with complete_cases
  complete_rows     <- which(preds$complete_cases)
  pred_df           <- as.data.frame(preds$predicted_shares)
  colnames(pred_df) <- paste0("pred_", categories)
  
  data_with_preds   <- cbind(prepared[complete_rows, ], pred_df)
  rownames(data_with_preds) <- NULL
  
  base::cat(sprintf(
    "\nData with predictions: %d rows, %d columns\n",
    nrow(data_with_preds), ncol(data_with_preds)
  ))
  
  matched_table <- perform_data_fusion(data_with_preds, categories)
  valid         <- validate_predictions(prepared, preds, categories)
  
  return(list(
    participation   = part_res,
    intensity       = int_res,
    predictions     = preds,
    validation      = valid,
    matched_table   = matched_table,
    data_with_preds = data_with_preds
  ))
}

################################################################################
# MAIN EXECUTION
################################################################################

# ------------------------------------------------------------------------------
# Configuration
#    When sourced from 00_master.R these variables already exist in the session.
#    The stopifnot() calls below catch the case where this script is run
#    standalone without them being set.
# ------------------------------------------------------------------------------

stopifnot(
  "pre_hurdle_dir not found — source 00_master.R first, or define it manually." =
    exists("pre_hurdle_dir"),
  "hurdle_results_dir not found — source 00_master.R first, or define it manually." =
    exists("hurdle_results_dir")
)

# Years to model — can be overridden by 00_master.R before sourcing.
if (!exists("years_to_model")) {
  years_to_model <- 1999:2021
}

for (yr in years_to_model) {
  current_year <- as.character(yr)
  
  # --- DYNAMIC DIRECTORY CREATION ---
  # Create a year-specific path: .../hurdle_results/2019/
  year_output_dir <- paste0(hurdle_results_dir, current_year, "/")
  
  # Check if this year's folder exists; if not, create it
  if (!dir.exists(year_output_dir)) {
    dir.create(year_output_dir, recursive = TRUE)
  }
  
  base::cat("\n=================================================================\n")
  base::cat("                 STARTING PROCESSING FOR YEAR:", current_year, "\n")
  base::cat("  Outputs saving to:", year_output_dir, "\n")
  base::cat("=================================================================\n")
  
  # --- NEW DYNAMIC FILE PATH ---
  # This builds: .../pre_hurdle/2019/3_annualized_budget_shares_2019.csv
  file_path <- paste0(pre_hurdle_dir, current_year, "/3_annualized_budget_shares_", current_year, ".csv")
  
  if (!file.exists(file_path)) {
    warning("File missing for year: ", current_year, ". Skipping to next year...")
    next
  }
  
  # 1. Load data for the current year
  base::cat("Loading data from:", file_path, "\n")
  data_yr <- read.csv(file_path)
  
  # 2. Run the full pipeline for this specific year
  results <- run_hurdle_model_pipeline(data_yr)
  
  # ------------------------------------------------------------------
  # 3. EXPORT OUTPUTS (Now using year_output_dir)
  # ------------------------------------------------------------------
  base::cat("\nSaving outputs for", current_year, "...\n")
  
  # Primary Matched Table
  matched_path <- paste0(year_output_dir, "matched_interview_diary_", current_year, ".csv")
  write.csv(results$matched_table, matched_path, row.names = FALSE)
  
  # Stats
  write.csv(results$participation$stats,
            paste0(year_output_dir, "stage1_participation_stats_", current_year, ".csv"), row.names = FALSE)
  write.csv(results$intensity$stats,
            paste0(year_output_dir, "stage2_intensity_stats_", current_year, ".csv"),     row.names = FALSE)
  
  # Validation Plot
  validation_plot <- ggplot(results$validation, aes(x = actual, y = predicted)) +
    geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "red") +
    geom_point(size = 3, alpha = 0.7, color = "steelblue") +
    geom_text(aes(label = category), check_overlap = TRUE, vjust = -1, size = 2.5) +
    labs(
      title    = paste("Stage 2 Validation:", current_year),
      subtitle = paste("Predictors:", length(c(numeric_cols, categorical_cols)), "variables"),
      x        = "Actual Mean Share (Weighted)",
      y        = "Predicted Mean Share"
    ) +
    theme_minimal()
  ggsave(paste0(year_output_dir, "stage2_validation_plot_", current_year, ".png"),
         validation_plot, width = 10, height = 8, dpi = 300)
  
  # Match distance distribution Plot
  distance_plot <- ggplot(
    data.frame(distance = results$matched_table$match_distance), aes(x = distance)) +
    geom_histogram(bins = 50, fill = "steelblue", alpha = 0.8) +
    geom_vline(xintercept = quantile(results$matched_table$match_distance, 0.95),
               linetype = "dashed", color = "red") +
    annotate("text",
             x     = quantile(results$matched_table$match_distance, 0.95),
             y     = Inf, vjust = 2, hjust = -0.1, color = "red",
             label = "95th percentile") +
    labs(
      title    = paste("Stage 3 Match Distance Distribution:", current_year),
      subtitle = paste0("k = ", K_CANDIDATES, " candidates | donor sampled uniformly"),
      x        = "Distance to Chosen Diary Donor",
      y        = "Number of Interview HHs"
    ) +
    theme_minimal()
  ggsave(paste0(year_output_dir, "stage3_match_distance_", current_year, ".png"),
         distance_plot, width = 8, height = 5, dpi = 300)
  
  # Donor reuse distribution Plot
  reuse_counts <- as.data.frame(table(results$matched_table$diary_donor_row_index))
  names(reuse_counts) <- c("diary_row", "times_used")
  
  reuse_plot <- ggplot(reuse_counts, aes(x = times_used)) +
    geom_histogram(bins = 40, fill = "darkorange", alpha = 0.8) +
    labs(
      title    = paste("Stage 3 Diary Donor Reuse Distribution:", current_year),
      subtitle = paste0("Unique donors used: ", length(unique(results$matched_table$diary_donor_row_index)),
                        " / ", sum(data_yr$i.data == DIARY_LABEL, na.rm = TRUE),
                        "  |  uniform sampling among k-NN"),
      x = "Times a Diary HH Was Selected as Donor",
      y = "Number of Diary HHs"
    ) +
    theme_minimal()
  ggsave(paste0(year_output_dir, "stage3_donor_reuse_", current_year, ".png"),
         reuse_plot, width = 8, height = 5, dpi = 300)
  
  # ------------------------------------------------------------------
  # 4. DIARY DONOR BALANCE SUMMARY (Used vs Unused)
  # ------------------------------------------------------------------
  diary_df  <- results$data_with_preds[results$data_with_preds$i.data == DIARY_LABEL, ]
  used_flag <- seq_len(nrow(diary_df)) %in% unique(results$matched_table$diary_donor_row_index)
  
  diary_balance_summary <- diary_df %>%
    dplyr::mutate(used = used_flag) %>%
    dplyr::group_by(used) %>%
    dplyr::summarise(
      n            = dplyr::n(),
      mean_age     = mean(age_ref,         na.rm = TRUE),
      mean_income  = mean(income_quintile, na.rm = TRUE),
      mean_popwt   = mean(popwt,           na.rm = TRUE),
      pct_urban    = mean(bls_urbn == "1", na.rm = TRUE),
      dplyr::across(dplyr::starts_with("pred_"), mean, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Print to the console for live monitoring
  base::cat("\n--- Diary Donor Balance for", current_year, "---\n")
  print(diary_balance_summary, width = Inf)
  
  # Save to CSV for the audit trail
  write.csv(diary_balance_summary,
            paste0(year_output_dir, "stage3_diary_balance_", current_year, ".csv"),
            row.names = FALSE)
  
  base::cat("\nSuccessfully finished processing", current_year, "\n\n")
}

base::cat("\nPIPELINE COMPLETE. All years processed and files saved to:\n", hurdle_results_dir, "\n")
base::cat("\nNext steps:\n")
base::cat("  1. Stack the yearly matched CSVs together.\n")
base::cat("  2. Join stacked CSV to raw 204-item data on HH ID (newid)\n")
base::cat("  3. Diary-type categories  → use diary raw expenditure\n")
base::cat("  4. Interview-type + shared → use interview raw expenditure\n")
base::cat("  5. Recompute budget shares from fused total expenditure\n")

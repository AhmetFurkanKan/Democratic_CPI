################################################################################
# Step 9 — LA/AIDS Demand System Estimation
# (original filename: 6_substitution_analysis_micro_LA_AIDS.R)
#
# Purpose : Estimate a Linear Approximate Almost Ideal Demand System (LA/AIDS)
#           using pseudo-panel aggregation (Region-Month-Quintile).
#
# Updates : 1. Integrates Stone's Price Index for real expenditure (X/P).
#           2. Adds demographic controls (fam_size, bls_urbn).
#           3. Calculates full NxN cross-price and expenditure elasticities.
#
# Inputs  : post_hurdle_dir/{YYYY}/3_annualized_budget_shares_{YYYY}.csv
#           regional_prices_path  (region_category_prices.csv)
# Outputs : aids_output_dir/full_elasticity_matrices_SUR.csv
#           aids_output_dir/own_price_summary_SUR.csv
#           aids_output_dir/micro_substitution_gap_SUR_LA_AIDS.png
#
# Called from : 00_master.R (RUN_STEP_9 = TRUE)
# Standalone  : define post_hurdle_dir, aids_output_dir, and
#               regional_prices_path before sourcing this script.
################################################################################

# ── 1. PACKAGES ───────────────────────────────────────────────────────────────
packages <- c("data.table", "dplyr", "fixest", "ggplot2", "stringr", "tidyr", "systemfit", "lubridate")
invisible(lapply(packages, function(p) {
  if (!requireNamespace(p, quietly = TRUE)) install.packages(p)
  library(p, character.only = TRUE)
}))

# ── 2. PATHS & SETTINGS ───────────────────────────────────────────────────────

# ------------------------------------------------------------------------------
# When sourced from 00_master.R these variables already exist in the session.
# The stopifnot() calls below catch the case where this script is run
# standalone without them being set.
# ------------------------------------------------------------------------------

stopifnot(
  "post_hurdle_dir not found — source 00_master.R first, or define it manually." =
    exists("post_hurdle_dir"),
  "aids_output_dir not found — source 00_master.R first, or define it manually." =
    exists("aids_output_dir"),
  "regional_prices_path not found — source 00_master.R first, or define it manually." =
    exists("regional_prices_path")
)

# NOTE: cex_years is specific to this analysis (constrained by regional price
# data availability) and is NOT controlled by the master's years_to_model.
cex_years <- 2010:2019

if (!dir.exists(aids_output_dir)) dir.create(aids_output_dir, recursive = TRUE)

# ── 3. LOAD & FORMAT REGIONAL FRED PRICES ─────────────────────────────────────
cat("Loading and formatting regional FRED prices...\n")

regional_prices <- fread(regional_prices_path)
regional_prices[, parsed_date := dmy(observation_date, quiet = TRUE)]
regional_prices[is.na(parsed_date), parsed_date := mdy(observation_date, quiet = TRUE)]
regional_prices[, year_mo := paste0(year(parsed_date), str_pad(month(parsed_date), 2, pad="0"))]
regional_prices[, cat_clean := str_replace_all(Category, "[^A-Za-z0-9]", "_")]
broad_cats_clean <- unique(regional_prices$cat_clean)
regional_prices[, lnp := log(price)]

prices_wide <- dcast(regional_prices, year_mo + Region ~ cat_clean, value.var = "lnp")
setnames(prices_wide, old = broad_cats_clean, new = paste0("lnp_", broad_cats_clean))

# ── 4. LOAD WIDE HOUSEHOLD MICRO-DATA ─────────────────────────────────────────
cat("\nLoading wide HH files...\n")
share_cols <- paste0("share_", broad_cats_clean)

hh_list <- lapply(cex_years, function(yr) {
  path <- sprintf("%s%d/3_annualized_budget_shares_%d.csv", post_hurdle_dir, yr, yr)
  if (!file.exists(path)) return(NULL)
  
  dt <- fread(path, colClasses = c(year_mo = "character"))
  # ADDED: fam_size and bls_urbn for demographic controls
  keep <- intersect(c("newid", "year_mo", "year", "income_quintile", "popwt",
                      "total_exp", "ln_total_exp", share_cols, "age_ref", 
                      "region", "fam_size", "bls_urbn"), names(dt))
  dt[, ..keep]
})

hh <- rbindlist(Filter(Negate(is.null), hh_list), fill = TRUE)
hh <- hh[income_quintile %in% 1:5 & total_exp > 0]
if (!"ln_total_exp" %in% names(hh)) hh[, ln_total_exp := log(total_exp)]

hh[, region_name := fcase(
  region == 1, "Northeast",
  region == 2, "Midwest",
  region == 3, "South",
  region == 4, "West"
)]

cat("\nAdjusting shares and expenditure to exclude shelter...\n")
shelter_cat_name <- "Rent_of_shelter" 
shelter_share_col <- paste0("share_", shelter_cat_name)

if (shelter_share_col %in% names(hh)) {
  shelter_share <- hh[[shelter_share_col]]
  valid_rows <- shelter_share < 1 & !is.na(shelter_share)
  hh <- hh[valid_rows]
  shelter_share <- shelter_share[valid_rows]
  
  hh[, total_exp := total_exp * (1 - shelter_share)]
  hh[, ln_total_exp := log(total_exp)]
  broad_cats_clean <- setdiff(broad_cats_clean, shelter_cat_name)
  
  for (cat in broad_cats_clean) {
    col_name <- paste0("share_", cat)
    hh[, (col_name) := get(col_name) / (1 - shelter_share)]
  }
  hh[, (shelter_share_col) := NULL]
}

# ── 5. MERGE PRICES & AGGREGATE TO PSEUDO-PANEL ───────────────────────────────
cat("\nMerging regional prices into household data...\n")
hh <- merge(hh, prices_wide, by.x = c("year_mo", "region_name"), by.y = c("year_mo", "Region"), all.x = TRUE)
hh <- hh[complete.cases(hh[, .SD, .SDcols = paste0("lnp_", broad_cats_clean)])]

cat("\nAggregating micro-data to Region x Year-Month x Quintile cells...\n")
# Ensure demographic controls are averaged during aggregation
agg_cols <- c(paste0("share_", broad_cats_clean), 
              paste0("lnp_", broad_cats_clean), 
              "ln_total_exp", "age_ref", "fam_size", "bls_urbn")
agg_cols <- intersect(agg_cols, names(hh))

hh_agg <- hh[, lapply(.SD, function(x) weighted.mean(x, w = popwt, na.rm = TRUE)), 
             by = .(year_mo, region_name, income_quintile), 
             .SDcols = agg_cols]

hh_agg_weights <- hh[, .(cell_popwt = sum(popwt, na.rm = TRUE)), 
                     by = .(year_mo, region_name, income_quintile)]
hh_agg <- merge(hh_agg, hh_agg_weights, by = c("year_mo", "region_name", "income_quintile"))

hh_agg[, month := substr(year_mo, 5, 6)]
hh_agg[, year := substr(year_mo, 1, 4)]

# ── 5.5 CALCULATE STONE'S PRICE INDEX & REAL EXPENDITURE ──────────────────────
cat("Calculating Stone's Price Index to deflate expenditure...\n")
hh_agg[, ln_P := 0]
for (cat in broad_cats_clean) {
  hh_agg[, ln_P := ln_P + (get(paste0("share_", cat)) * get(paste0("lnp_", cat)))]
}
hh_agg[, real_ln_total_exp := ln_total_exp - ln_P]

# ── 6. RUN SUR AIDS & CALCULATE FULL ELASTICITY MATRICES ──────────────────────
cat("\nRunning Restricted SUR demand system by quintile...\n")

kept_cats   <- broad_cats_clean[-length(broad_cats_clean)]
dropped_cat <- broad_cats_clean[length(broad_cats_clean)]
N_cats      <- length(broad_cats_clean)

safe_eq_names <- paste0("C", 1:length(kept_cats))
names(safe_eq_names) <- kept_cats

# Updated controls to use real expenditure and include region fixed effects
controls <- c("real_ln_total_exp", "age_ref", "fam_size", "bls_urbn") 
actual_controls <- intersect(controls, names(hh_agg))
actual_controls <- c(actual_controls, "factor(region_name)", "factor(month)", "factor(year)")

own_price_results <- list()
full_matrix_results <- list()

for (q in 0:5) {
  cat(sprintf("Estimating Quintile %d...\n", q))
  hh_q <- if (q == 0) copy(hh_agg) else hh_agg[income_quintile == q]
  
  for (cat_name in kept_cats) {
    rel_price_var <- paste0("rel_lnp_", cat_name)
    hh_q[[rel_price_var]] <- hh_q[[paste0("lnp_", cat_name)]] - hh_q[[paste0("lnp_", dropped_cat)]]
  }
  
  eq_system <- list()
  rhs_formula <- paste(c(paste0("rel_lnp_", kept_cats), actual_controls), collapse = " + ")
  for (cat_name in kept_cats) {
    eq_system[[ safe_eq_names[cat_name] ]] <- as.formula(paste(paste0("share_", cat_name), "~", rhs_formula))
  }
  
  restrictions <- c()
  for (i in 1:(length(kept_cats)-1)) {
    for (j in (i+1):length(kept_cats)) {
      res_str <- sprintf("%s_rel_lnp_%s = %s_rel_lnp_%s", 
                         safe_eq_names[kept_cats[i]], kept_cats[j], 
                         safe_eq_names[kept_cats[j]], kept_cats[i])
      restrictions <- c(restrictions, res_str)
    }
  }
  
  sur_model <- tryCatch({
    systemfit(eq_system, data = as.data.frame(hh_q), method = "SUR", restrict.matrix = restrictions)
  }, error = function(e) { return(NULL) })
  
  if (is.null(sur_model)) next
  
  # --- EXTRACT PARAMETERS (Gamma Matrix & Beta Vector) ---
  Gamma <- matrix(0, N_cats, N_cats, dimnames = list(broad_cats_clean, broad_cats_clean))
  Beta  <- numeric(N_cats); names(Beta) <- broad_cats_clean
  W     <- numeric(N_cats); names(W)    <- broad_cats_clean
  
  for(cat in broad_cats_clean) {
    W[cat] <- weighted.mean(hh_q[[paste0("share_", cat)]], w = hh_q$cell_popwt, na.rm = TRUE)
  }
  
  for(i in kept_cats) {
    eq_i <- safe_eq_names[i]
    Beta[i] <- coef(sur_model)[paste0(eq_i, "_real_ln_total_exp")]
    for(j in kept_cats) {
      Gamma[i, j] <- coef(sur_model)[paste0(eq_i, "_rel_lnp_", j)]
    }
    # Homogeneity: sum over j of gamma_ij = 0
    Gamma[i, dropped_cat] <- -sum(Gamma[i, kept_cats])
  }
  
  # Adding up: sum over i of Beta_i = 0
  Beta[dropped_cat] <- -sum(Beta[kept_cats])
  
  # Symmetry: gamma_ji = gamma_ij
  for(j in kept_cats) { Gamma[dropped_cat, j] <- Gamma[j, dropped_cat] }
  
  # Homogeneity for dropped cat
  Gamma[dropped_cat, dropped_cat] <- -sum(Gamma[dropped_cat, kept_cats])
  
  # --- CALCULATE FULL ELASTICITIES (WITH DELTA METHOD STANDARD ERRORS) ---
  for(i in broad_cats_clean) {
    # 1. Expenditure Elasticity
    e_exp <- 1 + (Beta[i] / W[i])
    
    se_e_exp <- NA_real_
    pval_exp <- NA_real_
    
    if (i %in% kept_cats) {
      eq_i <- safe_eq_names[i]
      coef_beta <- paste0(eq_i, "_real_ln_total_exp")
      var_beta <- vcov(sur_model)[coef_beta, coef_beta]
      
      se_e_exp <- sqrt(var_beta) / W[i]
      # Hypothesis test: is expenditure elasticity significantly different from 1? 
      # (This is mathematically identical to testing if Beta_i != 0)
      z_stat_exp <- Beta[i] / sqrt(var_beta) 
      pval_exp <- 2 * (1 - pnorm(abs(z_stat_exp)))
    }
    
    full_matrix_results[[paste(q, i, "Exp")]] <- data.table(
      quintile = q, category_i = i, category_j = "Expenditure", 
      elasticity_type = "Expenditure", elasticity_value = e_exp,
      std_error = se_e_exp, p_value = pval_exp
    )
    
    # 2. Price Elasticities
    for(j in broad_cats_clean) {
      etype <- if(i == j) "Own-Price" else "Cross-Price"
      kronecker_delta <- if(i == j) 1 else 0
      
      e_price <- -kronecker_delta + (Gamma[i, j] / W[i]) - Beta[i] * (W[j] / W[i])
      
      se_e_price <- NA_real_
      pval_price <- NA_real_
      
      if (i %in% kept_cats && j %in% kept_cats) {
        eq_i <- safe_eq_names[i]
        coef_gamma <- paste0(eq_i, "_rel_lnp_", j)
        coef_beta  <- paste0(eq_i, "_real_ln_total_exp")
        
        var_gamma <- vcov(sur_model)[coef_gamma, coef_gamma]
        var_beta  <- vcov(sur_model)[coef_beta, coef_beta]
        cov_gb    <- vcov(sur_model)[coef_gamma, coef_beta]
        
        # Delta method variance formula for uncompensated price elasticity
        var_e_price <- (1 / W[i]^2) * var_gamma + (W[j]^2 / W[i]^2) * var_beta - 2 * (W[j] / W[i]^2) * cov_gb
        
        if (!is.na(var_e_price) && var_e_price > 0) {
          se_e_price <- sqrt(var_e_price)
          # Hypothesis test: is price elasticity significantly different from 0?
          z_stat_price <- e_price / se_e_price
          pval_price <- 2 * (1 - pnorm(abs(z_stat_price)))
        }
      }
      
      full_matrix_results[[paste(q, i, j)]] <- data.table(
        quintile = q, category_i = i, category_j = j, 
        elasticity_type = etype, elasticity_value = e_price,
        std_error = se_e_price, p_value = pval_price
      )
    }
  }
  
  # --- PRESERVE DATA FOR YOUR PLOTTING SCRIPT ---
  for (cat_name in kept_cats) {
    eq_n <- safe_eq_names[cat_name]
    own_coef <- paste0(eq_n, "_rel_lnp_", cat_name)
    exp_coef <- paste0(eq_n, "_real_ln_total_exp")
    
    se_gamma <- sqrt(vcov(sur_model)[own_coef, own_coef])
    se_beta  <- sqrt(vcov(sur_model)[exp_coef, exp_coef])
    
    own_price_results[[paste(q, cat_name)]] <- data.table(
      quintile = q, category = cat_name, mean_share = W[cat_name],
      gamma_ii = Gamma[cat_name, cat_name], beta_i = Beta[cat_name],
      elasticity = -1 + (Gamma[cat_name, cat_name] / W[cat_name]) - Beta[cat_name],
      gamma_std_error = se_gamma, gamma_ci_low = Gamma[cat_name, cat_name] - 1.96 * se_gamma,
      gamma_ci_high = Gamma[cat_name, cat_name] + 1.96 * se_gamma,
      beta_std_error = se_beta, beta_ci_low = Beta[cat_name] - 1.96 * se_beta,  # <--- NEW
      beta_ci_high = Beta[cat_name] + 1.96 * se_beta,                           # <--- NEW
      n_obs = nrow(hh_q)
    )
  }
  # Add reference category for plotting
  own_price_results[[paste(q, dropped_cat)]] <- data.table(
    quintile = q, category = dropped_cat, mean_share = W[dropped_cat],
    gamma_ii = Gamma[dropped_cat, dropped_cat], beta_i = Beta[dropped_cat],
    elasticity = -1 + (Gamma[dropped_cat, dropped_cat] / W[dropped_cat]) - Beta[dropped_cat],
    gamma_std_error = NA_real_, gamma_ci_low = NA_real_, gamma_ci_high = NA_real_,
    beta_std_error = NA_real_, beta_ci_low = NA_real_, beta_ci_high = NA_real_, # <--- NEW
    n_obs = nrow(hh_q)
  )
}

# ── 7. EXPORT RESULTS ─────────────────────────────────────────────────────────
full_matrix_dt <- rbindlist(full_matrix_results)
plot_data_dt   <- rbindlist(own_price_results)

full_matrix_dt[, category_i := str_replace_all(category_i, "_", " ")]
full_matrix_dt[, category_j := str_replace_all(category_j, "_", " ")]
plot_data_dt[, category := str_replace_all(category, "_", " ")] 

fwrite(full_matrix_dt, paste0(aids_output_dir, "full_elasticity_matrices_SUR.csv"))
fwrite(plot_data_dt, paste0(aids_output_dir, "own_price_summary_SUR.csv"))
cat("Saved full matrices and own-price summary to CSV.\n")

# ── 8. PLOT THE SUBSTITUTION GAP (RAW COEFFICIENTS) ───────────────────────────
cat("\nGenerating plot...\n")
p <- ggplot(plot_data_dt, aes(x = factor(quintile, levels = 0:5), y = gamma_ii)) +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey50") +
  geom_errorbar(aes(ymin = gamma_ci_low, ymax = gamma_ci_high), width = 0.2, colour = "coral") +
  geom_point(size = 3, colour = "coral") +
  geom_line(data = plot_data_dt[quintile > 0], aes(group = 1), colour = "coral", linewidth = 0.5, alpha = 0.6) +
  facet_wrap(~category, scales = "free_y", ncol = 3) +
  scale_x_discrete(labels = c("All", paste0("Q", 1:5))) +  
  labs(
    title = "Own-Price Sensitivity of Budget Shares by Quintile",
    subtitle = "SUR LA/AIDS Model with Real Expenditure and Spatial Prices",
    x = "Income Group", y = "∂(share) / ∂ln(own_price)"
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.subtitle = element_text(size = 9, colour = "grey40"), strip.text = element_text(face = "bold"))

ggsave(paste0(aids_output_dir, "micro_substitution_gap_SUR_LA_AIDS.png"), plot = p, width = 10, height = 7, dpi = 300)
cat("DONE.\n")

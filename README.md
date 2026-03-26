# Democratic_CPI
R pipeline fusing US CEX data via ML hurdle models and k-NN matching to estimate LA/AIDS demand systems and construct a Distributional CPI.

# CEX Data Fusion, Distributional CPI, and Demand Analysis Pipeline

## Overview
This repository contains the complete R codebase for fusing the US Consumer Expenditure (CEX) Interview (`fmli`) and Diary (`fmld`) surveys using a machine learning hurdle model and k-Nearest Neighbors (k-NN) statistical matching. 

The fused dataset is then used to:
1. Calculate household-level annualized budget shares.
2. Construct a novel **Distributional Consumer Price Index (DCPI)** alongside a baseline official CPI replication, **built upon the CPI calculation methodology and pipeline developed by Xavier Jaravel.**
3. Estimate a Linear Approximate Almost Ideal Demand System (LA/AIDS) to compute price and expenditure elasticities.


## Data Sources
* **Raw CEX Data:** The raw Consumer Expenditure (CEX) Interview and Diary survey data can be accessed via the official [Bureau of Labor Statistics (BLS) website](https://www.bls.gov) or downloaded directly from [Xavier Jaravel's DCPI website](https://www.xavierjaravel.com/dcpi).
* **Processed BLS Series:** The full list of processed BLS series (including item price series, relative importance weights, and crosswalks) required for the 0_CPI and 8_DCPI calculation engines can also be found on Jaravel's DCPI website.

## Repository Structure
The project is organized to cleanly separate raw data, codebase, intermediate processing, and final outputs.

    project_root/
    │
    ├── README.md
    ├── data/
    │   ├── CEX_light/                  # Raw CEX spending files (e.g., ucc_spendingYYYY_light.csv)
    │   ├── xwalk_by_yearquarter.csv    # UCC to ItemCode crosswalk
    │   └── XWALK_Furkan_itemcode_to_category.csv # Hierarchy mapping
    │
    ├── code/
    │   ├── 00_master.R                 # Main orchestration script for Data Fusion & AIDS
    │   ├── step1_reshape_cex/          # Scripts 1a, 1b, 1c: Pre-hurdle wide reshaping
    │   ├── step2_category_aggregation/ # Scripts 2a, 2b: Category mapping and housing logic
    │   ├── step3_preliminary_shares/   # Script 3: Preliminary budget shares for ML model
    │   ├── step4_hurdle_fusion/        # Scripts 4, 5: LASSO Hurdle model and k-NN Data Fusion
    │   ├── step6_postfusion_prep/      # Scripts 6, 7: Post-fusion reshaping and final shares
    │   ├── step9_analysis/             # Script 9: LA/AIDS SUR estimation
    │   │
    │   ├── 0_CPI/                      # Official baseline CPI calculation engine (based on Jaravel)
    │   │   └── code/0a.master.R
    │   │
    │   └── 8_DCPI/                     # Distributional CPI calculation engine (based on Jaravel)
    │       └── code/0a.master.R
    │
    ├── intermediate/                   # Auto-generated directory for processing
    │   ├── pre_hurdle/                 # Intermediate wide tables
    │   ├── hurdle_results/             # Donor matching outputs and diagnostic plots
    │   ├── fused_CEX/                  # Final quarterly fused spending records
    │   └── post_hurdle/                # Final annualized household budget shares
    │
    └── output/                         # Auto-generated directory for final thesis results
        ├── aids_analysis/              # Elasticity matrices and substitution gap plots
        ├── 0_CPI/output/               # Baseline CPI series
        └── 8_DCPI/output/              # Distributional CPI series

## Software Requirements
This pipeline requires **R (version 4.0+)** and the following packages:
* **Data Wrangling:** `data.table`, `dplyr`, `tidyr`, `stringr`, `lubridate`, `purrr`
* **Machine Learning / Matching:** `glmnet` (LASSO), `FNN` (k-Nearest Neighbors)
* **Econometrics:** `systemfit` (SUR models), `fixest`
* **Visualization:** `ggplot2`

## How to Run the Pipeline (Quick Start)
The entire workflow is modular and automated. To reproduce the findings from raw data to final indices, execute the following three scripts in order. **You do not need to manually create any output folders; the scripts will generate them automatically.**

### Step 1: Data Fusion and Demand System (Main Pipeline)
1. Open `code/00_master.R`.
2. Edit the `project_root` path variable at the top of the script to match your local machine directory.
3. Run the script. 
*This will execute Steps 1-9, performing the LASSO hurdle modeling, uniform k-NN donor sampling, category aggregations, and the LA/AIDS SUR estimations.*

### Step 2: Calculate Baseline Official CPI
1. Open `code/0_CPI/code/0a.master.R`.
2. Edit the `project_root` path variable at the top to match your local directory.
3. Run the script.
*This bypasses the hurdle model to calculate the standard CPI using raw `CEX_light` data.*

### Step 3: Calculate Distributional CPI (DCPI)
1. Open `code/8_DCPI/code/0a.master.R`.
2. Edit the `project_root` path variable at the top to match your local directory.
3. Run the script.
*This reads the fused quarterly spending files from `intermediate/fused_CEX/` to calculate the novel Distributional CPI utilizing democratic budget shares.*

## Methodology Details
* **Hurdle Model (Step 4):** Evaluates consumption participation (LASSO logistic regression) and intensity (LASSO linear regression with Duan's smearing estimator for retransformation bias).
* **Data Fusion (Steps 4 & 5):** Uses predicted latent shares and demographics to map each Interview household to a Diary donor using uniform sampling among $k=10$ nearest neighbors.
* **Demand System (Step 9):** Estimates an LA/AIDS model using Seemingly Unrelated Regressions (SUR) across pseudo-panels (Region × Month × Quintile). Applies symmetry and homogeneity restrictions, and calculates full NxN uncompensated price elasticities.
* **Price Index Construction:** The baseline and distributional CPI computations leverage the robust estimation framework and coding architecture provided by Xavier Jaravel, adapted here to integrate seamlessly with the fused CEX outputs.

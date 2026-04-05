# =============================================================
# run_acquisition.R
# Purpose:  Daily acquisition pipeline orchestrator.
#           Runs all three acquisition scripts in sequence:
#           1. fetch_probabilities  — ESPN win probabilities
#           2. fetch_odds           — The Odds API moneylines
#           3. fetch_outcomes       — MLB Stats API results
# Usage:    Rscript scripts/run_acquisition.R
#           Rscript scripts/run_acquisition.R --debug
# Author:   Jonathan Erdmann
# =============================================================

library(here)

# -------------------------------------------------------------
# Parse arguments
# -------------------------------------------------------------

args   <- commandArgs(trailingOnly = TRUE)
iDebug <- "--debug" %in% args

# -------------------------------------------------------------
# Source all acquisition functions
# (sys.nframe() guard prevents auto-execution on source)
# -------------------------------------------------------------

source(here("R", "acquisition", "utils.R"))
source(here("R", "acquisition", "fetch_probabilities.R"))
source(here("R", "acquisition", "fetch_odds.R"))
source(here("R", "acquisition", "fetch_outcomes.R"))

# -------------------------------------------------------------
# Pipeline header
# -------------------------------------------------------------

cat("\n")
cat(rep("*", 55), "\n", sep = "")
cat("  DAILY ACQUISITION PIPELINE\n")
cat("  Date:", format(Sys.Date(), "%Y-%m-%d"), "\n")
cat("  Time:", format(Sys.time(), "%H:%M:%S %Z"), "\n")
if (iDebug) cat("  Mode: DEBUG — no database writes\n")
cat(rep("*", 55), "\n\n", sep = "")

# -------------------------------------------------------------
# Step 1 — Probabilities
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 1: ESPN Win Probabilities\n")
cat(rep("-", 55), "\n", sep = "")

tryCatch({
  fetch_and_store_probabilities(
    iDate  = Sys.Date(),
    iDebug = iDebug
  )
}, error = function(e) {
  cat("[ERROR] Probabilities failed:", conditionMessage(e), "\n")
})

cat("\n")

# -------------------------------------------------------------
# Step 2 — Odds
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 2: Odds API Moneylines\n")
cat(rep("-", 55), "\n", sep = "")

tryCatch({
  fetch_and_store_odds(
    iDate  = Sys.Date(),
    iDebug = iDebug
  )
}, error = function(e) {
  cat("[ERROR] Odds failed:", conditionMessage(e), "\n")
})

cat("\n")

# -------------------------------------------------------------
# Step 3 — Outcomes
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 3: MLB Outcomes\n")
cat(rep("-", 55), "\n", sep = "")

tryCatch({
  fetch_and_store_outcomes(iDebug = iDebug)
}, error = function(e) {
  cat("[ERROR] Outcomes failed:", conditionMessage(e), "\n")
})

# -------------------------------------------------------------
# Pipeline footer
# -------------------------------------------------------------

cat("\n")
cat(rep("*", 55), "\n", sep = "")
cat("  PIPELINE COMPLETE\n")
cat("  Time:", format(Sys.time(), "%H:%M:%S %Z"), "\n")
cat(rep("*", 55), "\n\n", sep = "")

# -------------------------------------------------------------
# Reconciliation check
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  RECONCILIATION CHECK\n")
cat(rep("-", 55), "\n", sep = "")

library(DBI)
library(RSQLite)
library(yaml)

con <- dbConnect(RSQLite::SQLite(), here("db", "betting.sqlite"))
check_orphaned_records(con)
dbDisconnect(con)


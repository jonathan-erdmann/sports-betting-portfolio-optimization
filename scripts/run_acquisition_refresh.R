# =============================================================
# run_acquisition_refresh.R
# Purpose:  Lightweight odds refresh pipeline for automated
#           cron execution. Runs Step 0 and Step 2 only.
#           No HIL prompts — safe for unattended execution.
#           Outcomes step also runs to capture completed games.
# Usage:    Rscript scripts/run_acquisition_refresh.R
# Author:   Jonathan Erdmann
# =============================================================

library(here)
library(DBI)
library(RSQLite)
library(httr)
library(jsonlite)
library(yaml)

# -------------------------------------------------------------
# Source acquisition functions
# -------------------------------------------------------------

source(here("R", "acquisition", "utils.R"))
source(here("R", "acquisition", "fetch_odds.R"))
source(here("R", "acquisition", "fetch_outcomes.R"))

# -------------------------------------------------------------
# Pipeline header
# -------------------------------------------------------------

cat("\n")
cat(rep("*", 55), "\n", sep = "")
cat("  ODDS REFRESH PIPELINE\n")
cat("  Date:", format(Sys.Date(), "%Y-%m-%d"), "\n")
cat("  Time:", format(Sys.time(), "%H:%M:%S %Z"), "\n")
cat(rep("*", 55), "\n\n", sep = "")

# -------------------------------------------------------------
# Step 0 — Register today's schedule
# (needed to ensure game_ids exist before storing odds)
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 0: Register MLB Schedule\n")
cat(rep("-", 55), "\n", sep = "")

con <- dbConnect(RSQLite::SQLite(),
                 here("db", "betting.sqlite"))
dbExecute(con, "PRAGMA foreign_keys = ON")

tryCatch({
  register_mlb_schedule(con, Sys.Date(), iDebug = FALSE)
}, error = function(e) {
  cat("[ERROR] Schedule registration failed:",
      conditionMessage(e), "\n")
})

dbDisconnect(con)
cat("\n")

# -------------------------------------------------------------
# Step 1 — Odds refresh
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 1: Odds API Moneylines\n")
cat(rep("-", 55), "\n", sep = "")

tryCatch({
  fetch_and_store_odds(
    iDate  = Sys.Date(),
    iDebug = FALSE
  )
}, error = function(e) {
  cat("[ERROR] Odds refresh failed:",
      conditionMessage(e), "\n")
})

cat("\n")

# -------------------------------------------------------------
# Step 2 — Outcomes
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 2: MLB Outcomes\n")
cat(rep("-", 55), "\n", sep = "")

tryCatch({
  fetch_and_store_outcomes(iDebug = FALSE)
}, error = function(e) {
  cat("[ERROR] Outcomes failed:",
      conditionMessage(e), "\n")
})

# -------------------------------------------------------------
# Reconciliation check
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  RECONCILIATION CHECK\n")
cat(rep("-", 55), "\n", sep = "")

con <- dbConnect(RSQLite::SQLite(),
                 here("db", "betting.sqlite"))
check_orphaned_records(con)
dbDisconnect(con)

# -------------------------------------------------------------
# Generate and push dashboard
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  DASHBOARD\n")
cat(rep("-", 55), "\n", sep = "")

tryCatch({
  source(here("R", "features", "remove_vig.R"))
  source(here("scripts", "generate_dashboard.R"))
}, error = function(e) {
  cat("[ERROR] Dashboard generation failed:",
      conditionMessage(e), "\n")
})

cat("\n")

# -------------------------------------------------------------
# Pipeline footer
# -------------------------------------------------------------

cat("\n")
cat(rep("*", 55), "\n", sep = "")
cat("  REFRESH COMPLETE\n")
cat("  Time:", format(Sys.time(), "%H:%M:%S %Z"), "\n")
cat(rep("*", 55), "\n\n", sep = "")

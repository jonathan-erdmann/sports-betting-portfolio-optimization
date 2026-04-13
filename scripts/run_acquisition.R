# =============================================================
# run_acquisition.R
# Purpose:  Daily acquisition pipeline orchestrator.
#           Runs all acquisition scripts in sequence:
#           0. register_mlb_schedule — canonical game registry
#           1. fetch_probabilities_espn — ESPN win probabilities
#           1b. fetch_probabilities_fangraphs — FanGraphs probabilities
#           2. fetch_odds — The Odds API moneylines
#           3. fetch_outcomes — MLB Stats API results
# Usage:    Rscript scripts/run_acquisition.R
#           Rscript scripts/run_acquisition.R --debug
# Author:   Jonathan Erdmann
# =============================================================

library(here)
library(DBI)
library(RSQLite)
library(httr)
library(jsonlite)
library(yaml)

# -------------------------------------------------------------
# Parse arguments
# -------------------------------------------------------------

args   <- commandArgs(trailingOnly = TRUE)
iDebug <- "--debug" %in% args

# -------------------------------------------------------------
# Source all acquisition functions
# -------------------------------------------------------------

source(here("R", "acquisition", "utils.R"))
source(here("R", "acquisition", "fetch_probabilities_espn.R"))
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
# Step 0 — Register today's games from MLB Stats API
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 0: Register MLB Schedule\n")
cat(rep("-", 55), "\n", sep = "")

config <- yaml::read_yaml(here("config", "config.yml"))
con    <- dbConnect(RSQLite::SQLite(),
                    here("db", "betting.sqlite"))
dbExecute(con, "PRAGMA foreign_keys = ON")

game_registry <- tryCatch({
  register_mlb_schedule(con, Sys.Date(), iDebug)
}, error = function(e) {
  cat("[ERROR] Schedule registration failed:",
      conditionMessage(e), "\n")
  NULL
})

dbDisconnect(con)
cat("\n")

# -------------------------------------------------------------
# -------------------------------------------------------------
# Step 0b — Register upcoming games (7-day lookahead)
#           and fetch opening odds for newly registered games
# -------------------------------------------------------------
cat(rep("-", 55), "\n", sep = "")
cat("  STEP 0b: Register Upcoming Schedule (7-day lookahead)\n")
cat(rep("-", 55), "\n", sep = "")
con <- dbConnect(RSQLite::SQLite(), here("db", "betting.sqlite"))
dbExecute(con, "PRAGMA foreign_keys = ON")
tryCatch({
  register_mlb_schedule_lookahead(
    iCon   = con,
    iDays  = 7,
    iDebug = iDebug
  )
}, error = function(e) {
  cat("[ERROR] Lookahead registration failed:",
      conditionMessage(e), "\n")
})
dbDisconnect(con)

# Fetch opening odds for upcoming games
cat("  Fetching opening odds for upcoming games...\n")
for (dd in as.character(Sys.Date() + seq_len(7))) {
  future_date <- as.Date(dd)
  tryCatch({
    future_con <- dbConnect(RSQLite::SQLite(),
                            here("db", "betting.sqlite"))
    future_registry <- tryCatch(
      dbGetQuery(future_con, "
        SELECT g.game_id, g.game_time,
               th.team_name AS home_name,
               ta.team_name AS away_name
        FROM games g
        JOIN teams th ON g.home_team_id = th.team_id
        JOIN teams ta ON g.away_team_id = ta.team_id
        WHERE g.game_date = ?
      ", params = list(dd)),
      error = function(e) NULL
    )
    dbDisconnect(future_con)
    if (!is.null(future_registry) && nrow(future_registry) > 0) {
      reg_list <- lapply(seq_len(nrow(future_registry)), function(ii) {
        list(
          game_id   = future_registry$game_id[ii],
          home_name = future_registry$home_name[ii],
          away_name = future_registry$away_name[ii],
          game_time = future_registry$game_time[ii]
        )
      })
      fetch_and_store_odds(
        iDate     = future_date,
        iRegistry = reg_list,
        iDebug    = FALSE
      )
      cat(sprintf("  Odds snapshot stored for %s\n", dd))
      Sys.sleep(1)
    }
  }, error = function(e) {
    cat(sprintf("  [WARN] Odds fetch failed for %s: %s\n",
                dd, conditionMessage(e)))
  })
}

# -------------------------------------------------------------
# Step 1 — Probabilities
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 1: ESPN Win Probabilities\n")
cat(rep("-", 55), "\n", sep = "")

tryCatch({
  fetch_and_store_probabilities(
    iDate     = Sys.Date(),
    iRegistry = game_registry,
    iDebug    = iDebug
  )
}, error = function(e) {
  cat("[ERROR] Probabilities failed:",
      conditionMessage(e), "\n")
})

cat("\n")

# -------------------------------------------------------------
# Step 1b — FanGraphs Probabilities
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 1b: FanGraphs Probabilities\n")
cat(rep("-", 55), "\n", sep = "")

source(here("R", "acquisition", "fetch_probabilities_fangraphs.R"))

tryCatch({
  fetch_and_store_fangraphs(
    iDate  = Sys.Date(),
    iDebug = iDebug
  )
}, error = function(e) {
  cat("[ERROR] FanGraphs failed:", conditionMessage(e), "\n")
})

cat("\n")

# -------------------------------------------------------------
# Step 1c — Elo Model Probabilities
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 1c: Elo Model Probabilities\n")
cat(rep("-", 55), "\n", sep = "")

source(here("R", "models", "elo_model.R"))

tryCatch({
  run_elo_model(
    iDate        = Sys.Date(),
    iDebug       = iDebug,
    iForceReinit = FALSE
  )
}, error = function(e) {
  cat("[ERROR] Elo model failed:", conditionMessage(e), "\n")
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
    iDate     = Sys.Date(),
    iRegistry = game_registry,
    iDebug    = iDebug
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
# Step 3b — Update Confidence Weights
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 3b: Update Confidence Weights\n")
cat(rep("-", 55), "\n", sep = "")

source(here("R", "features", "update_confidence_weights.R"))

tryCatch({
  update_confidence_weights(iDebug = iDebug)
}, error = function(e) {
  cat("[ERROR] Confidence weights failed:",
      conditionMessage(e), "\n")
})

cat("\n")

# -------------------------------------------------------------
# Step 4 — Daily Opportunities
# -------------------------------------------------------------

cat(rep("-", 55), "\n", sep = "")
cat("  STEP 4: Daily Opportunities\n")
cat(rep("-", 55), "\n", sep = "")

source(here("R", "features", "get_daily_opportunities.R"))

tryCatch({
  compute_daily_opportunities(
    iDate  = Sys.Date(),
    iDebug = iDebug
  )
}, error = function(e) {
  cat("[ERROR] Daily opportunities failed:",
      conditionMessage(e), "\n")
})

cat("\n")

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

# Record that manual run occurred today
record_manual_run()

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
cat("  PIPELINE COMPLETE\n")
cat("  Time:", format(Sys.time(), "%H:%M:%S %Z"), "\n")
cat(rep("*", 55), "\n\n", sep = "")

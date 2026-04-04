# =============================================================
# build_database.R
# Purpose:  One-time script to create the SQLite database
#           schema for the sports betting portfolio
#           optimization research platform.
# Run:      Once during project initialization only.
# Author:   Jonathan Erdmann
# Date:     2026-04-04
# =============================================================

library(DBI)
library(RSQLite)
library(here)

# -------------------------------------------------------------
# Connection
# -------------------------------------------------------------

db_path <- here("db", "betting.sqlite")

if (file.exists(db_path)) {
  stop(
    "Database already exists at: ", db_path, "\n",
    "Delete it manually if you want to rebuild from scratch."
  )
}

con <- dbConnect(RSQLite::SQLite(), db_path)
dbExecute(con, "PRAGMA foreign_keys = ON")
cat("Database created at:", db_path, "\n\n")

# -------------------------------------------------------------
# Helper: safe table creation
# -------------------------------------------------------------

create_table <- function(con, name, sql) {
  if (dbExistsTable(con, name)) {
    cat("SKIPPED (already exists):", name, "\n")
  } else {
    dbExecute(con, sql)
    cat("CREATED:", name, "\n")
  }
}

# -------------------------------------------------------------
# Seed Tables
# -------------------------------------------------------------

cat("--- Seed Tables ---\n")

create_table(con, "leagues", "
  CREATE TABLE leagues (
    league_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    league_name  TEXT    NOT NULL UNIQUE,
    abbreviation TEXT    NOT NULL UNIQUE
  )
")

create_table(con, "teams", "
  CREATE TABLE teams (
    team_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    league_id     INTEGER NOT NULL REFERENCES leagues(league_id),
    team_name     TEXT    NOT NULL,
    team_name_alt TEXT,
    abbreviation  TEXT    NOT NULL,
    active        INTEGER NOT NULL DEFAULT 1,
    UNIQUE(league_id, team_name)
  )
")

create_table(con, "bookmakers", "
  CREATE TABLE bookmakers (
    bookmaker_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    bookmaker_name TEXT    NOT NULL UNIQUE,
    bookmaker_key  TEXT    NOT NULL UNIQUE,
    base_url       TEXT,
    scrape_method  TEXT    NOT NULL DEFAULT 'api',
    active         INTEGER NOT NULL DEFAULT 1
  )
")

create_table(con, "probability_sources", "
  CREATE TABLE probability_sources (
    source_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name   TEXT    NOT NULL UNIQUE,
    source_url    TEXT,
    scrape_method TEXT    NOT NULL DEFAULT 'scrape',
    active        INTEGER NOT NULL DEFAULT 1,
    notes         TEXT
  )
")

# -------------------------------------------------------------
# Layer 2 — Normalization
# -------------------------------------------------------------

cat("\n--- Layer 2: Normalization ---\n")

create_table(con, "games", "
  CREATE TABLE games (
    game_id           TEXT    PRIMARY KEY,
    league_id         INTEGER NOT NULL REFERENCES leagues(league_id),
    game_date         TEXT    NOT NULL,
    game_time         TEXT,
    home_team_id      INTEGER NOT NULL REFERENCES teams(team_id),
    away_team_id      INTEGER NOT NULL REFERENCES teams(team_id),
    neutral_site      INTEGER NOT NULL DEFAULT 0,
    status            TEXT    NOT NULL DEFAULT 'scheduled',
    created_timestamp TEXT    NOT NULL
  )
")

# -------------------------------------------------------------
# Layer 1 — Raw Ingestion
# -------------------------------------------------------------

cat("\n--- Layer 1: Raw Ingestion ---\n")

create_table(con, "odds_snapshots", "
  CREATE TABLE odds_snapshots (
    snapshot_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id          TEXT    NOT NULL REFERENCES games(game_id),
    bookmaker_id     INTEGER NOT NULL REFERENCES bookmakers(bookmaker_id),
    scrape_timestamp TEXT    NOT NULL,
    game_time        TEXT,
    home_moneyline   INTEGER,
    away_moneyline   INTEGER
  )
")

create_table(con, "probability_snapshots", "
  CREATE TABLE probability_snapshots (
    snapshot_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id          TEXT    NOT NULL REFERENCES games(game_id),
    source_id        INTEGER NOT NULL REFERENCES probability_sources(source_id),
    team_id          INTEGER NOT NULL REFERENCES teams(team_id),
    scrape_timestamp TEXT    NOT NULL,
    win_probability  REAL    NOT NULL
  )
")

create_table(con, "outcomes", "
  CREATE TABLE outcomes (
    outcome_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id            TEXT    NOT NULL REFERENCES games(game_id),
    team_id            INTEGER NOT NULL REFERENCES teams(team_id),
    win                INTEGER NOT NULL,
    recorded_timestamp TEXT    NOT NULL,
    UNIQUE(game_id, team_id)
  )
")

# -------------------------------------------------------------
# Layer 3 — Feature Engineering
# -------------------------------------------------------------

cat("\n--- Layer 3: Feature Engineering ---\n")

create_table(con, "daily_opportunities", "
  CREATE TABLE daily_opportunities (
    opportunity_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id               TEXT    NOT NULL REFERENCES games(game_id),
    team_id               INTEGER NOT NULL REFERENCES teams(team_id),
    snapshot_timestamp    TEXT    NOT NULL,
    moneyline             INTEGER NOT NULL,
    implied_prob_raw      REAL    NOT NULL,
    implied_prob_fair     REAL    NOT NULL,
    source_id             INTEGER REFERENCES probability_sources(source_id),
    source_probability    REAL,
    confidence_weight     REAL,
    posterior_probability REAL,
    expected_value        REAL,
    kelly_full            REAL,
    kelly_fractional      REAL,
    time_to_game_hours    REAL
  )
")

create_table(con, "confidence_weights", "
  CREATE TABLE confidence_weights (
    weight_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id      INTEGER NOT NULL REFERENCES probability_sources(source_id),
    segment        TEXT    NOT NULL,
    weight_value   REAL    NOT NULL,
    brier_source   REAL,
    brier_market   REAL,
    effective_date TEXT    NOT NULL,
    sample_size    INTEGER NOT NULL DEFAULT 0
  )
")

create_table(con, "clv_records", "
  CREATE TABLE clv_records (
    clv_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id           TEXT    NOT NULL REFERENCES games(game_id),
    team_id           INTEGER NOT NULL REFERENCES teams(team_id),
    entry_moneyline   INTEGER NOT NULL,
    closing_moneyline INTEGER NOT NULL,
    entry_prob_fair   REAL    NOT NULL,
    closing_prob_fair REAL    NOT NULL,
    clv               REAL    NOT NULL,
    UNIQUE(game_id, team_id)
  )
")

# -------------------------------------------------------------
# Layer 4 — Strategy and Simulation
# -------------------------------------------------------------

cat("\n--- Layer 4: Strategy and Simulation ---\n")

create_table(con, "strategy_definitions", "
  CREATE TABLE strategy_definitions (
    strategy_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name      TEXT    NOT NULL UNIQUE,
    created_timestamp  TEXT    NOT NULL,
    ev_threshold       REAL    NOT NULL DEFAULT 0.0,
    confidence_weight  REAL    NOT NULL DEFAULT 0.3,
    kelly_fraction     REAL    NOT NULL DEFAULT 0.25,
    daily_exposure_cap REAL    NOT NULL DEFAULT 0.1,
    min_edge           REAL    NOT NULL DEFAULT 0.0,
    max_bets_per_day   INTEGER,
    notes              TEXT
  )
")

create_table(con, "simulation_runs", "
  CREATE TABLE simulation_runs (
    run_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id        INTEGER NOT NULL REFERENCES strategy_definitions(strategy_id),
    run_timestamp      TEXT    NOT NULL,
    start_date         TEXT    NOT NULL,
    end_date           TEXT    NOT NULL,
    starting_capital   REAL    NOT NULL DEFAULT 1000.0,
    terminal_capital   REAL,
    log_growth         REAL,
    geometric_return   REAL,
    total_bets         INTEGER,
    run_status         TEXT    NOT NULL DEFAULT 'pending',
    notes              TEXT
  )
")

create_table(con, "simulated_bets", "
  CREATE TABLE simulated_bets (
    bet_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         INTEGER NOT NULL REFERENCES simulation_runs(run_id),
    opportunity_id INTEGER NOT NULL REFERENCES daily_opportunities(opportunity_id),
    stake_fraction REAL    NOT NULL,
    stake_amount   REAL    NOT NULL,
    outcome        INTEGER,
    pnl            REAL,
    capital_after  REAL
  )
")

create_table(con, "capital_trajectory", "
  CREATE TABLE capital_trajectory (
    trajectory_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                INTEGER NOT NULL REFERENCES simulation_runs(run_id),
    game_date             TEXT    NOT NULL,
    capital_start         REAL    NOT NULL,
    capital_end           REAL    NOT NULL,
    daily_pnl             REAL    NOT NULL,
    log_return            REAL    NOT NULL,
    cumulative_log_return REAL    NOT NULL,
    UNIQUE(run_id, game_date)
  )
")

# -------------------------------------------------------------
# Layer 5 — Evaluation
# -------------------------------------------------------------

cat("\n--- Layer 5: Evaluation ---\n")

create_table(con, "calibration_records", "
  CREATE TABLE calibration_records (
    calibration_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id        INTEGER NOT NULL REFERENCES probability_sources(source_id),
    segment          TEXT    NOT NULL,
    prob_bucket_low  REAL    NOT NULL,
    prob_bucket_high REAL    NOT NULL,
    predicted_prob   REAL    NOT NULL,
    actual_win_rate  REAL    NOT NULL,
    sample_size      INTEGER NOT NULL DEFAULT 0,
    brier_score      REAL,
    evaluation_date  TEXT    NOT NULL
  )
")

create_table(con, "strategy_performance", "
  CREATE TABLE strategy_performance (
    performance_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id               INTEGER NOT NULL UNIQUE
                                 REFERENCES simulation_runs(run_id),
    total_bets           INTEGER NOT NULL DEFAULT 0,
    total_wagered        REAL    NOT NULL DEFAULT 0.0,
    total_pnl            REAL    NOT NULL DEFAULT 0.0,
    roi                  REAL,
    max_drawdown         REAL,
    log_growth           REAL,
    geometric_return     REAL,
    clv_mean             REAL,
    sharpe               REAL,
    brier_score          REAL,
    win_rate             REAL,
    evaluation_timestamp TEXT    NOT NULL
  )
")

# -------------------------------------------------------------
# Indexes
# -------------------------------------------------------------

cat("\n--- Indexes ---\n")

indexes <- list(
  "idx_odds_game"         = "odds_snapshots(game_id)",
  "idx_odds_timestamp"    = "odds_snapshots(scrape_timestamp)",
  "idx_prob_game"         = "probability_snapshots(game_id)",
  "idx_prob_team"         = "probability_snapshots(team_id)",
  "idx_prob_timestamp"    = "probability_snapshots(scrape_timestamp)",
  "idx_games_date"        = "games(game_date)",
  "idx_games_league"      = "games(league_id)",
  "idx_outcomes_game"     = "outcomes(game_id)",
  "idx_opps_game"         = "daily_opportunities(game_id)",
  "idx_opps_timestamp"    = "daily_opportunities(snapshot_timestamp)",
  "idx_opps_team"         = "daily_opportunities(team_id)",
  "idx_simbets_run"       = "simulated_bets(run_id)",
  "idx_trajectory_run"    = "capital_trajectory(run_id)",
  "idx_trajectory_date"   = "capital_trajectory(game_date)",
  "idx_clv_game"          = "clv_records(game_id)",
  "idx_calibration_source"= "calibration_records(source_id)"
)

for (idx_name in names(indexes)) {
  dbExecute(
    con,
    paste0(
      "CREATE INDEX IF NOT EXISTS ", idx_name,
      " ON ", indexes[[idx_name]]
    )
  )
  cat("CREATED INDEX:", idx_name, "\n")
}

# -------------------------------------------------------------
# Confirm and close
# -------------------------------------------------------------

cat("\n--- Summary ---\n")
tables <- dbListTables(con)
cat("Schema complete.", length(tables), "tables created:\n")
for (t in sort(tables)) cat(" -", t, "\n")

dbDisconnect(con)
cat("\nConnection closed. Database ready.\n")

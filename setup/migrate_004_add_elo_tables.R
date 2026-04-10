# =============================================================
# migrate_004_add_elo_tables.R
# Purpose:  Creates elo_ratings and elo_game_log tables to
#           support the pitcher-adjusted MLB Elo model.
#           Also creates supporting indexes.
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(here)

db_path <- here("db", "betting.sqlite")
con     <- dbConnect(RSQLite::SQLite(), db_path)
dbExecute(con, "PRAGMA foreign_keys = ON")

# -------------------------------------------------------------
# elo_ratings — current Elo rating per team per season
# One row per team per season; updated in place after each game
# -------------------------------------------------------------

if (dbExistsTable(con, "elo_ratings")) {
  cat("SKIPPED: elo_ratings already exists\n")
} else {
  dbExecute(con, "
    CREATE TABLE elo_ratings (
      rating_id      INTEGER PRIMARY KEY AUTOINCREMENT,
      team_id        INTEGER NOT NULL REFERENCES teams(team_id),
      rating         REAL    NOT NULL DEFAULT 1500,
      effective_date TEXT    NOT NULL,
      games_played   INTEGER NOT NULL DEFAULT 0,
      season         INTEGER NOT NULL
    )
  ")
  cat("CREATED: elo_ratings\n")
}

# -------------------------------------------------------------
# elo_game_log — one row per game processed by the Elo model
# Stores pre/post ratings, pitcher adjustments, and outcome
# -------------------------------------------------------------

if (dbExistsTable(con, "elo_game_log")) {
  cat("SKIPPED: elo_game_log already exists\n")
} else {
  dbExecute(con, "
    CREATE TABLE elo_game_log (
      log_id              INTEGER PRIMARY KEY AUTOINCREMENT,
      game_id             TEXT    NOT NULL,
      home_team_id        INTEGER NOT NULL,
      away_team_id        INTEGER NOT NULL,
      home_rating_pre     REAL    NOT NULL,
      away_rating_pre     REAL    NOT NULL,
      home_rating_post    REAL    NOT NULL,
      away_rating_post    REAL    NOT NULL,
      home_pitcher_adj    REAL    NOT NULL DEFAULT 0,
      away_pitcher_adj    REAL    NOT NULL DEFAULT 0,
      predicted_home_prob REAL    NOT NULL,
      actual_home_win     INTEGER,
      game_date           TEXT    NOT NULL,
      season              INTEGER NOT NULL
    )
  ")
  cat("CREATED: elo_game_log\n")
}

# -------------------------------------------------------------
# Indexes
# -------------------------------------------------------------

indexes <- list(
  "idx_elo_ratings_team_season" = "elo_ratings(team_id, season)",
  "idx_elo_log_game"            = "elo_game_log(game_id)",
  "idx_elo_log_season_date"     = "elo_game_log(season, game_date)",
  "idx_elo_log_home_team"       = "elo_game_log(home_team_id)",
  "idx_elo_log_away_team"       = "elo_game_log(away_team_id)"
)

for (idx_name in names(indexes)) {
  dbExecute(
    con,
    paste0("CREATE INDEX IF NOT EXISTS ", idx_name,
           " ON ", indexes[[idx_name]])
  )
  cat("CREATED INDEX:", idx_name, "\n")
}

# -------------------------------------------------------------
# Summary
# -------------------------------------------------------------

cat("\n=== Current tables ===\n")
tables <- sort(dbListTables(con))
for (tt in tables) cat(" -", tt, "\n")

dbDisconnect(con)
cat("\nMigration 004 complete\n")

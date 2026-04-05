# =============================================================
# migrate_001_add_conflicts_table.R
# Purpose:  Adds data_conflicts table for human-in-the-loop
#           conflict resolution audit trail
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(here)

db_path <- here("db", "betting.sqlite")
con     <- dbConnect(RSQLite::SQLite(), db_path)
dbExecute(con, "PRAGMA foreign_keys = ON")

if (dbExistsTable(con, "data_conflicts")) {
  cat("SKIPPED: data_conflicts table already exists\n")
} else {
  dbExecute(con, "
    CREATE TABLE data_conflicts (
      conflict_id        INTEGER PRIMARY KEY AUTOINCREMENT,
      detected_timestamp TEXT    NOT NULL,
      game_date          TEXT    NOT NULL,
      matchup            TEXT    NOT NULL,
      conflict_type      TEXT    NOT NULL,
      registry_times     TEXT    NOT NULL,
      source_times       TEXT    NOT NULL,
      proposed_mapping   TEXT,
      final_mapping      TEXT,
      resolution         TEXT    NOT NULL,
      resolved_by        TEXT    NOT NULL,
      notes              TEXT
    )
  ")
  cat("CREATED: data_conflicts\n")
}

dbDisconnect(con)
cat("Migration complete\n")

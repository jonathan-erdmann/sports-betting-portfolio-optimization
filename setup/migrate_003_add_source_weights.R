# =============================================================
# migrate_003_add_source_weights.R
# Purpose:  Adds weight_method and prior_weight fields to
#           probability_sources table to support per-source
#           confidence weight configuration
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(here)

db_path <- here("db", "betting.sqlite")
con     <- dbConnect(RSQLite::SQLite(), db_path)
dbExecute(con, "PRAGMA foreign_keys = ON")

cols <- dbGetQuery(con, "PRAGMA table_info(probability_sources)")$name

if ("weight_method" %in% cols) {
  cat("SKIPPED: weight fields already exist\n")
} else {
  dbExecute(con,
    "ALTER TABLE probability_sources
     ADD COLUMN weight_method TEXT NOT NULL DEFAULT 'brier'"
  )
  dbExecute(con,
    "ALTER TABLE probability_sources
     ADD COLUMN prior_weight REAL NOT NULL DEFAULT 0.20"
  )
  cat("CREATED: weight_method and prior_weight columns\n")
  
  # Set prior weights per source
  dbExecute(con, "
    UPDATE probability_sources
    SET weight_method = 'brier',
        prior_weight  = 0.20
    WHERE source_name IN ('ESPN Analytics', 'FanGraphs',
                          'Baseball Reference', 'MLB Stats API')
  ")
  cat("UPDATED: prior weights set to 0.20 for all sources\n")
}

cat("\n=== Current probability_sources ===\n")
print(dbGetQuery(con, "
  SELECT source_id, source_name, active,
         weight_method, prior_weight
  FROM probability_sources
  ORDER BY source_id
"))

dbDisconnect(con)
cat("\nMigration complete\n")

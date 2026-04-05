# =============================================================
# migrate_002_add_starters.R
# Purpose:  Adds home_starter and away_starter fields to
#           the games table for starting pitcher tracking
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(here)

db_path <- here("db", "betting.sqlite")
con     <- dbConnect(RSQLite::SQLite(), db_path)
dbExecute(con, "PRAGMA foreign_keys = ON")

# Check if columns already exist
cols <- dbGetQuery(con, "PRAGMA table_info(games)")$name

if ("home_starter" %in% cols) {
  cat("SKIPPED: starter columns already exist\n")
} else {
  dbExecute(con,
    "ALTER TABLE games ADD COLUMN home_starter TEXT"
  )
  dbExecute(con,
    "ALTER TABLE games ADD COLUMN away_starter TEXT"
  )
  cat("CREATED: home_starter and away_starter columns\n")
}

dbDisconnect(con)
cat("Migration complete\n")

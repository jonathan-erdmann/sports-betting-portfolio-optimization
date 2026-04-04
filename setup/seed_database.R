# =============================================================
# seed_database.R
# Purpose:  Populates reference tables in the SQLite database
#           with seed data. Fetches MLB team data from the
#           MLB Stats API (free, no key required).
# Run:      Once after build_database.R during project setup.
# Author:   Jonathan Erdmann
# Date:     2026-04-04
# =============================================================

library(DBI)
library(RSQLite)
library(httr)
library(jsonlite)
library(dplyr)
library(readr)
library(here)

# -------------------------------------------------------------
# Connection
# -------------------------------------------------------------

db_path <- here("db", "betting.sqlite")

if (!file.exists(db_path)) {
  stop(
    "Database not found at: ", db_path, "\n",
    "Run setup/build_database.R first."
  )
}

con <- dbConnect(RSQLite::SQLite(), db_path)
dbExecute(con, "PRAGMA foreign_keys = ON")
cat("Connected to database at:", db_path, "\n\n")

# -------------------------------------------------------------
# Helper: safe insert with duplicate check
# -------------------------------------------------------------

safe_insert <- function(con, table, data) {
  existing <- dbGetQuery(con, paste0("SELECT COUNT(*) as n FROM ", table))$n
  if (existing > 0) {
    cat("SKIPPED (already seeded):", table, "\n")
    return(invisible(NULL))
  }
  dbWriteTable(con, table, data, append = TRUE, row.names = FALSE)
  cat("SEEDED:", table, "(", nrow(data), "rows )\n")
}

# -------------------------------------------------------------
# Leagues
# -------------------------------------------------------------

cat("--- Seeding Leagues ---\n")

leagues <- data.frame(
  league_name  = "Major League Baseball",
  abbreviation = "MLB",
  stringsAsFactors = FALSE
)

safe_insert(con, "leagues", leagues)

# Retrieve league_id for FK references
mlb_league_id <- dbGetQuery(
  con, "SELECT league_id FROM leagues WHERE abbreviation = 'MLB'"
)$league_id

# -------------------------------------------------------------
# MLB Teams — fetched from MLB Stats API
# -------------------------------------------------------------

cat("\n--- Fetching MLB Teams from Stats API ---\n")

mlb_api_url <- "https://statsapi.mlb.com/api/v1/teams?sportId=1&activeStatus=Y"

response <- GET(mlb_api_url)

if (status_code(response) != 200) {
  stop("MLB Stats API request failed with status: ", status_code(response))
}

raw <- content(response, as = "text", encoding = "UTF-8")
parsed <- fromJSON(raw, flatten = TRUE)
api_teams <- parsed$teams

# Select and rename relevant fields
teams <- api_teams %>%
  filter(!is.na(abbreviation)) %>%
  select(
    team_id      = id,
    team_name    = name,
    abbreviation = abbreviation
  ) %>%
  mutate(
    league_id     = mlb_league_id,
    team_name_alt = NA_character_,
    active        = 1L
  ) %>%
  select(team_id, league_id, team_name, team_name_alt, 
         abbreviation, active)

cat("Retrieved", nrow(teams), "teams from MLB Stats API\n")

safe_insert(con, "teams", teams)

# Save to seeds CSV for reference
write_csv(teams, here("data", "seeds", "teams.csv"))
cat("Teams CSV written to data/seeds/teams.csv\n")

# -------------------------------------------------------------
# Bookmakers
# -------------------------------------------------------------

cat("\n--- Seeding Bookmakers ---\n")

bookmakers <- data.frame(
  bookmaker_name = c(
    "DraftKings",
    "FanDuel",
    "BetMGM",
    "Caesars Sportsbook",
    "PointsBet"
  ),
  bookmaker_key = c(
    "draftkings",
    "fanduel",
    "betmgm",
    "caesars",
    "pointsbet"
  ),
  base_url = c(
    "https://sportsbook.draftkings.com",
    "https://sportsbook.fanduel.com",
    "https://sports.betmgm.com",
    "https://sportsbook.caesars.com",
    "https://pointsbet.com"
  ),
  scrape_method = rep("scrape", 5),
  active        = rep(1L, 5),
  stringsAsFactors = FALSE
)

safe_insert(con, "bookmakers", bookmakers)

# Save to seeds CSV for reference
write_csv(bookmakers, here("data", "seeds", "bookmakers.csv"))
cat("Bookmakers CSV written to data/seeds/bookmakers.csv\n")

# -------------------------------------------------------------
# Probability Sources
# -------------------------------------------------------------

cat("\n--- Seeding Probability Sources ---\n")

probability_sources <- data.frame(
  source_name   = c(
    "FanGraphs",
    "Baseball Reference",
    "MLB Stats API"
  ),
  source_url    = c(
    "https://www.fangraphs.com/standings/playoff-odds",
    "https://www.baseball-reference.com/previews",
    "https://statsapi.mlb.com"
  ),
  scrape_method = c(
    "scrape",
    "scrape",
    "api"
  ),
  active        = rep(1L, 3),
  notes         = c(
    "Model-based win probabilities from depth chart projections",
    "Elo-based win probabilities",
    "Official MLB Stats API - schedule and game data"
  ),
  stringsAsFactors = FALSE
)

safe_insert(con, "probability_sources", probability_sources)

# Save to seeds CSV for reference
write_csv(
  probability_sources,
  here("data", "seeds", "probability_sources.csv")
)
cat("Probability sources CSV written to",
    "data/seeds/probability_sources.csv\n")

# -------------------------------------------------------------
# Confirm and close
# -------------------------------------------------------------

cat("\n--- Seed Summary ---\n")

tables <- c("leagues", "teams", "bookmakers", "probability_sources")
for (t in tables) {
  n <- dbGetQuery(con, paste0("SELECT COUNT(*) as n FROM ", t))$n
  cat(sprintf(" - %-25s %d rows\n", t, n))
}

dbDisconnect(con)
cat("\nConnection closed. Seeding complete.\n")
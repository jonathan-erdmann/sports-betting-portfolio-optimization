# =============================================================
# fetch_outcomes.R
# Purpose:  Fetches MLB game outcomes from the MLB Stats API
#           for all registered games with no recorded outcome.
#           Queries database for pending games automatically.
# Layer:    1 - Data Acquisition
# Author:   Jonathan Erdmann
# =============================================================

library(httr)
library(jsonlite)
library(DBI)
library(RSQLite)
library(yaml)
library(here)

# Source shared utilities
source(here("R", "acquisition", "utils.R"))

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------

config     <- yaml::read_yaml(here("config", "config.yml"))
mlb_base   <- config$mlb_stats_api$base_url
user_agent <- config$scraping$user_agent

# -------------------------------------------------------------
# Helper: fetch MLB schedule with scores for a given date
# -------------------------------------------------------------

fetch_mlb_schedule <- function(iDate = Sys.Date()) {
  
  url <- paste0(
    mlb_base, "/schedule",
    "?sportId=1",
    "&date=", format(iDate, "%Y-%m-%d"),
    "&hydrate=linescore"
  )
  
  response <- GET(url, add_headers("User-Agent" = user_agent))
  
  if (status_code(response) != 200) {
    stop("MLB Stats API request failed with status: ",
         status_code(response))
  }
  
  parsed <- fromJSON(
    content(response, as = "text", encoding = "UTF-8"),
    flatten = TRUE
  )
  
  if (parsed$totalGames == 0) {
    cat("No games found for date:",
        format(iDate, "%Y-%m-%d"), "\n")
    return(NULL)
  }
  
  games <- parsed$dates$games[[1]]
  
  # Filter to completed games only
  final <- games[games$status.abstractGameState == "Final", ]
  
  cat("Total games:", nrow(games), "\n")
  cat("Completed games:", nrow(final), "\n")
  
  if (nrow(final) == 0) return(NULL)
  
  final
  
}

# -------------------------------------------------------------
# Helper: get dates with pending outcomes from database
# -------------------------------------------------------------

get_pending_dates <- function(iCon) {
  
  result <- dbGetQuery(iCon, "
    SELECT DISTINCT g.game_date
    FROM games g
    LEFT JOIN outcomes o ON g.game_id = o.game_id
    WHERE o.outcome_id IS NULL
      AND g.status = 'scheduled'
      AND g.game_date <= DATE('now', 'localtime')
    ORDER BY g.game_date
  ")
  
  if (nrow(result) == 0) {
    cat("No pending games found in database\n")
    return(NULL)
  }
  
  as.Date(result$game_date)
  
}

# -------------------------------------------------------------
# Helper: update game status to final
# -------------------------------------------------------------

update_game_status <- function(iCon, iGameId,
                               iDebug = FALSE) {
  
  if (iDebug) {
    cat("  [DEBUG] Would update status to final:",
        iGameId, "\n")
    return(invisible(NULL))
  }
  
  dbExecute(iCon,
    "UPDATE games SET status = 'final'
     WHERE game_id = ?",
    params = list(iGameId)
  )
  
}

# -------------------------------------------------------------
# Main: fetch and store outcomes for all pending games
# -------------------------------------------------------------

fetch_and_store_outcomes <- function(iDebug = FALSE) {
  
  cat("=== Fetching MLB Outcomes ===\n")
  
  if (iDebug) {
    cat("[DEBUG MODE] No database writes will occur\n")
  }
  
  cat("\n")
  
  # Connect to database
  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))
  
  # Get dates with pending outcomes
  pending_dates <- get_pending_dates(con)
  if (is.null(pending_dates)) return(invisible(NULL))
  
  cat("Pending dates:",
      paste(pending_dates, collapse = ", "), "\n\n")
  
  total_outcomes <- 0
  total_games    <- 0
  
  for (ii in seq_along(pending_dates)) {
    
    iDate <- pending_dates[ii]
    cat("--- Date:", format(iDate, "%Y-%m-%d"), "---\n")
    
    # Fetch completed games from MLB Stats API
    games <- fetch_mlb_schedule(iDate)
    if (is.null(games)) {
      cat("\n")
      next
    }
    
    recorded_timestamp <- format(
      Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"
    )
    
    for (jj in seq_len(nrow(games))) {
      
      home_name   <- games$teams.home.team.name[jj]
      away_name   <- games$teams.away.team.name[jj]
      home_winner <- games$teams.home.isWinner[jj]
      away_winner <- games$teams.away.isWinner[jj]
      game_date   <- as.Date(games$officialDate[jj])
      
      cat("Processing [", jj, "/", nrow(games), "]:",
          away_name, "at", home_name, "\n")
      
      # Skip if winner data is missing
      if (is.na(home_winner) || is.na(away_winner)) {
        cat("  SKIPPED: winner data not yet available\n")
        next
      }
      
      # Resolve team IDs
      home_team_id <- get_team_id(con, home_name)
      away_team_id <- get_team_id(con, away_name)
      
      if (is.na(home_team_id) || is.na(away_team_id)) {
        cat("  SKIPPED: team not found in database\n")
        next
      }
      
      # Build canonical game_id
      home_abbr         <- get_team_abbr(con, home_team_id)
      away_abbr         <- get_team_abbr(con, away_team_id)
      canonical_game_id <- build_game_id(game_date,
                                         home_abbr, away_abbr)
      
      # Check if this game is pending in our database
      pending <- dbGetQuery(con, "
        SELECT g.game_id
        FROM games g
        LEFT JOIN outcomes o ON g.game_id = o.game_id
        WHERE g.game_id = ?
          AND o.outcome_id IS NULL
          AND g.status = 'scheduled'
      ", params = list(canonical_game_id))
      
      if (nrow(pending) == 0) {
        cat("  SKIPPED: not pending or already recorded\n")
        next
      }
      
      # Store outcome for each team
      teams_data <- list(
        list(team_id = home_team_id,
             win     = as.integer(home_winner),
             name    = home_name),
        list(team_id = away_team_id,
             win     = as.integer(away_winner),
             name    = away_name)
      )
      
      for (kk in seq_along(teams_data)) {
        
        team <- teams_data[[kk]]
        
        if (iDebug) {
          cat("  [DEBUG] Would store:", team$name,
              "| win:", team$win,
              "| game_id:", canonical_game_id,
              "| team_id:", team$team_id, "\n")
        } else {
          dbExecute(con,
            "INSERT INTO outcomes
             (game_id, team_id, win, recorded_timestamp)
             VALUES (?, ?, ?, ?)",
            params = list(
              canonical_game_id, team$team_id,
              team$win, recorded_timestamp
            )
          )
          total_outcomes <- total_outcomes + 1
        }
        
      }
      
      # Update game status to final
      update_game_status(con, canonical_game_id, iDebug)
      
      if (!iDebug) {
        winner_name <- if (home_winner) home_name else away_name
        cat("  Stored: Winner —", winner_name, "\n")
      }
      
      total_games <- total_games + 1
      
    }
    
    cat("\n")
    
  }
  
  cat("=== Complete ===\n")
  if (iDebug) {
    cat("[DEBUG MODE] No data was written to the database\n")
  } else {
    cat("Games processed:  ", total_games, "\n")
    cat("Outcomes stored:  ", total_outcomes, "\n")
  }
  
  invisible(NULL)
  
}

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (sys.nframe() == 0) {
  fetch_and_store_outcomes(iDebug = FALSE)
}

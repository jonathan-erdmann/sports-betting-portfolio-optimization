# =============================================================
# fetch_odds.R
# Purpose:  Fetches MLB moneylines from The Odds API for all
#           available games. Stores one snapshot per
#           bookmaker per game.
# Layer:    1 - Data Acquisition
# Author:   Jonathan Erdmann
# =============================================================

library(httr)
library(jsonlite)
library(dplyr)
library(DBI)
library(RSQLite)
library(yaml)
library(here)

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------

config     <- yaml::read_yaml(here("config", "config.yml"))
api_key    <- config$odds_api$key
base_url   <- config$odds_api$base_url
user_agent <- config$scraping$user_agent

# -------------------------------------------------------------
# Helper: fetch odds from The Odds API
# -------------------------------------------------------------

fetch_odds_api <- function(iSport      = "baseball_mlb",
                           iRegions    = "us",
                           iMarkets    = "h2h",
                           iOddsFormat = "american") {
  
  url <- paste0(base_url, "/sports/", iSport, "/odds/")
  
  response <- GET(
    url,
    add_headers("User-Agent" = user_agent),
    query = list(
      apiKey     = api_key,
      regions    = iRegions,
      markets    = iMarkets,
      oddsFormat = iOddsFormat,
      dateFormat = "iso"
    )
  )
  
  if (status_code(response) != 200) {
    stop("Odds API request failed with status: ",
         status_code(response), "\n",
         content(response, as = "text"))
  }
  
  cat("Requests used:     ",
      headers(response)[["x-requests-used"]], "\n")
  cat("Requests remaining:",
      headers(response)[["x-requests-remaining"]], "\n\n")
  
  fromJSON(
    content(response, as = "text", encoding = "UTF-8"),
    flatten = TRUE
  )
  
}

# -------------------------------------------------------------
# Helper: resolve team_id from database
# -------------------------------------------------------------

get_team_id <- function(iCon, iTeamName) {
  
  result <- dbGetQuery(
    iCon,
    "SELECT team_id FROM teams WHERE team_name = ?",
    params = list(iTeamName)
  )
  
  if (nrow(result) == 0) {
    warning("Team not found in database: ", iTeamName)
    return(NA_integer_)
  }
  
  result$team_id[1]
  
}

# -------------------------------------------------------------
# Helper: get team abbreviation from database
# -------------------------------------------------------------

get_team_abbr <- function(iCon, iTeamId) {
  
  dbGetQuery(
    iCon,
    "SELECT abbreviation FROM teams WHERE team_id = ?",
    params = list(iTeamId)
  )$abbreviation
  
}

# -------------------------------------------------------------
# Helper: resolve bookmaker_id from database
# -------------------------------------------------------------

get_bookmaker_id <- function(iCon, iBookmakerKey) {
  
  result <- dbGetQuery(
    iCon,
    "SELECT bookmaker_id FROM bookmakers
     WHERE bookmaker_key = ?",
    params = list(iBookmakerKey)
  )
  
  if (nrow(result) == 0) return(NA_integer_)
  result$bookmaker_id[1]
  
}

# -------------------------------------------------------------
# Helper: build canonical game_id
# -------------------------------------------------------------

build_game_id <- function(iGameDate, iHomeAbbr, iAwayAbbr) {
  paste0(
    "MLB_",
    gsub("-", "", as.character(iGameDate)),
    "_",
    toupper(iHomeAbbr),
    "_",
    toupper(iAwayAbbr)
  )
}

# -------------------------------------------------------------
# Helper: ensure game exists in games table
# -------------------------------------------------------------

ensure_game <- function(iCon, iGameId, iGameDate, iGameTime,
                        iHomeTeamId, iAwayTeamId,
                        iNeutralSite = 0,
                        iDebug       = FALSE) {
  
  existing <- dbGetQuery(
    iCon,
    "SELECT game_id FROM games WHERE game_id = ?",
    params = list(iGameId)
  )
  
  if (nrow(existing) == 0) {
    
    if (iDebug) {
      cat("  [DEBUG] Would register new game:", iGameId, "\n")
      return(invisible(NULL))
    }
    
    mlb_league_id <- dbGetQuery(
      iCon,
      "SELECT league_id FROM leagues
       WHERE abbreviation = 'MLB'"
    )$league_id
    
    dbExecute(iCon,
              "INSERT INTO games
       (game_id, league_id, game_date, game_time,
        home_team_id, away_team_id, neutral_site,
        status, created_timestamp)
       VALUES (?, ?, ?, ?, ?, ?, ?, 'scheduled', ?)",
              params = list(
                iGameId, mlb_league_id, as.character(iGameDate),
                iGameTime, iHomeTeamId, iAwayTeamId,
                iNeutralSite,
                format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
              )
    )
    cat("  Registered new game:", iGameId, "\n")
  }
  
}

# -------------------------------------------------------------
# Helper: ensure bookmaker exists in database
# -------------------------------------------------------------

ensure_bookmaker <- function(iCon, iKey, iTitle,
                             iDebug = FALSE) {
  
  existing <- dbGetQuery(
    iCon,
    "SELECT bookmaker_id FROM bookmakers
     WHERE bookmaker_key = ?",
    params = list(iKey)
  )
  
  if (nrow(existing) == 0) {
    
    if (iDebug) {
      cat("  [DEBUG] Would register bookmaker:", iTitle, "\n")
      return(NA_integer_)
    }
    
    dbExecute(iCon,
              "INSERT INTO bookmakers
       (bookmaker_name, bookmaker_key, scrape_method, active)
       VALUES (?, ?, 'api', 1)",
              params = list(iTitle, iKey)
    )
    cat("  Registered new bookmaker:", iTitle, "\n")
    
    return(dbGetQuery(
      iCon,
      "SELECT bookmaker_id FROM bookmakers
       WHERE bookmaker_key = ?",
      params = list(iKey)
    )$bookmaker_id)
    
  }
  
  existing$bookmaker_id[1]
  
}

# -------------------------------------------------------------
# Main: fetch and store odds
# -------------------------------------------------------------

fetch_and_store_odds <- function(iDate  = Sys.Date(),
                                 iDebug = FALSE) {
  
  cat("=== Fetching Odds API Moneylines ===\n")
  cat("Date:", as.character(iDate), "\n")
  
  if (iDebug) {
    cat("[DEBUG MODE] No database writes will occur\n")
  }
  
  cat("\n")
  
  # Connect to database
  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))
  
  # Fetch odds from API
  cat("Fetching from Odds API...\n")
  games_df <- fetch_odds_api()
  
  cat("Games returned:", nrow(games_df), "\n\n")
  
  if (nrow(games_df) == 0) {
    cat("No games returned from Odds API\n")
    return(invisible(NULL))
  }
  
  scrape_timestamp <- format(
    Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"
  )
  
  snapshots_stored <- 0
  games_processed  <- 0
  
  for (ii in seq_len(nrow(games_df))) {
    
    home_name  <- games_df$home_team[ii]
    away_name  <- games_df$away_team[ii]
    game_time  <- games_df$commence_time[ii]
    game_date  <- as.Date(substr(game_time, 1, 10))
    bookmakers <- games_df$bookmakers[[ii]]
    
    # Only process games on the target date
    if (game_date != iDate) next
    
    cat("Processing [", ii, "/", nrow(games_df), "]:",
        away_name, "at", home_name, "\n")
    
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
    canonical_game_id <- build_game_id(game_date, home_abbr, away_abbr)
    
    # Ensure game is registered
    ensure_game(
      con, canonical_game_id, game_date, game_time,
      home_team_id, away_team_id,
      iDebug = iDebug
    )
    
    # Process each bookmaker
    for (jj in seq_len(nrow(bookmakers))) {
      
      bm_key   <- bookmakers$key[jj]
      bm_title <- bookmakers$title[jj]
      outcomes <- bookmakers$markets[[jj]]$outcomes[[1]]
      
      if (is.null(outcomes) || nrow(outcomes) < 2) {
        cat("  SKIPPED bookmaker", bm_key,
            ": no outcomes available\n")
        next
      }
      
      # Resolve or register bookmaker
      bookmaker_id <- ensure_bookmaker(
        con, bm_key, bm_title, iDebug
      )
      
      if (is.na(bookmaker_id)) next
      
      # Extract home and away moneylines
      home_ml <- NA_integer_
      away_ml <- NA_integer_
      
      for (kk in seq_len(nrow(outcomes))) {
        if (outcomes$name[kk] == home_name) {
          home_ml <- as.integer(outcomes$price[kk])
        } else {
          away_ml <- as.integer(outcomes$price[kk])
        }
      }
      
      if (is.na(home_ml) || is.na(away_ml)) {
        cat("  SKIPPED bookmaker", bm_key,
            ": could not parse moneylines\n")
        next
      }
      
      if (iDebug) {
        cat("  [DEBUG] Would store:", bm_title,
            "| home:", home_ml,
            "| away:", away_ml,
            "| game_id:", canonical_game_id, "\n")
      } else {
        dbExecute(con,
                  "INSERT INTO odds_snapshots
           (game_id, bookmaker_id, scrape_timestamp,
            game_time, home_moneyline, away_moneyline)
           VALUES (?, ?, ?, ?, ?, ?)",
                  params = list(
                    canonical_game_id, bookmaker_id,
                    scrape_timestamp, game_time,
                    home_ml, away_ml
                  )
        )
        snapshots_stored <- snapshots_stored + 1
      }
      
    }
    
    if (iDebug) {
      cat("  [DEBUG] Bookmakers processed:",
          nrow(bookmakers), "\n")
    }
    
    games_processed <- games_processed + 1
    
  }
  
  cat("\n=== Complete ===\n")
  if (iDebug) {
    cat("[DEBUG MODE] No data was written to the database\n")
  } else {
    cat("Games processed:  ", games_processed, "\n")
    cat("Snapshots stored: ", snapshots_stored, "\n")
  }
  
  invisible(NULL)
  
}

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (!interactive()) {
  fetch_and_store_odds(
    iDate  = Sys.Date(),
    iDebug = FALSE
  )
}
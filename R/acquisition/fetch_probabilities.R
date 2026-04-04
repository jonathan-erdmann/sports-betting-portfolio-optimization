# =============================================================
# fetch_probabilities.R
# Purpose:  Fetches MLB win probabilities from ESPN's Matchup
#           Predictor for all preview games on a given date.
#           Uses ESPN Scoreboard API for game discovery and
#           ESPN game pages for probability extraction.
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
user_agent <- config$scraping$user_agent
sleep_secs <- config$scraping$sleep_seconds

# -------------------------------------------------------------
# Helper: fetch ESPN scoreboard for a given date
# -------------------------------------------------------------

get_espn_schedule <- function(iDate = Sys.Date()) {
  
  date_string <- gsub("-", "", as.character(iDate))
  url <- paste0(
    "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=",
    date_string
  )
  
  response <- GET(url, add_headers("User-Agent" = user_agent))
  
  if (status_code(response) != 200) {
    stop("ESPN Scoreboard API failed with status: ",
         status_code(response))
  }
  
  parsed <- fromJSON(
    content(response, as = "text", encoding = "UTF-8"),
    flatten = TRUE
  )
  events <- parsed[["events"]]
  
  if (is.null(events) || nrow(events) == 0) {
    cat("No games found for date:", as.character(iDate), "\n")
    return(NULL)
  }
  
  preview <- events[events[["status.type.state"]] == "pre", ]
  
  if (nrow(preview) == 0) {
    cat("No preview games found for date:",
        as.character(iDate), "\n")
    return(NULL)
  }
  
  cat("Found", nrow(preview), "preview games for",
      as.character(iDate), "\n")
  return(preview)
  
}

# -------------------------------------------------------------
# Helper: construct ESPN game page URL
# -------------------------------------------------------------

build_espn_game_url <- function(iGameId, iGameName) {
  
  teams     <- strsplit(iGameName, " at ")[[1]]
  away_slug <- tolower(tail(strsplit(trimws(teams[1]), " ")[[1]], 1))
  home_slug <- tolower(tail(strsplit(trimws(teams[2]), " ")[[1]], 1))
  url_slug  <- paste0(away_slug, "-", home_slug)
  
  paste0(
    "https://www.espn.com/mlb/game/_/gameId/",
    iGameId, "/", url_slug
  )
  
}

# -------------------------------------------------------------
# Helper: scrape probabilities from ESPN game page
# -------------------------------------------------------------

scrape_espn_probabilities <- function(iGameUrl) {
  
  response <- GET(iGameUrl, add_headers("User-Agent" = user_agent))
  
  if (status_code(response) != 200) {
    warning("ESPN game page failed for URL: ", iGameUrl)
    return(NULL)
  }
  
  html_text <- content(response, as = "text", encoding = "UTF-8")
  
  tmp_file <- tempfile(fileext = ".html")
  writeLines(html_text, tmp_file)
  on.exit(unlink(tmp_file))
  
  probs_raw <- system(
    paste0("grep -oP '\"percentage\":[0-9]+\\.?[0-9]*' ", tmp_file),
    intern = TRUE
  )
  
  teams_raw <- system(
    paste0("grep -oP '\"tmName\":\"[^\"]+\"' ", tmp_file),
    intern = TRUE
  )
  
  if (length(probs_raw) < 2 || length(teams_raw) < 2) {
    warning("Could not extract probabilities from: ", iGameUrl)
    return(NULL)
  }
  
  pcts       <- as.numeric(gsub(".*:", "", probs_raw[1:2]))
  team_names <- gsub('"tmName":"|"', "", teams_raw[1:2])
  
  data.frame(
    team_name       = team_names,
    win_probability = pcts / 100,
    stringsAsFactors = FALSE
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
                        iDebug = FALSE) {
  
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
      "SELECT league_id FROM leagues WHERE abbreviation = 'MLB'"
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
# Main: fetch and store probabilities
# -------------------------------------------------------------

fetch_and_store_probabilities <- function(iDate   = Sys.Date(),
                                          iDebug  = FALSE) {
  
  cat("=== Fetching ESPN Probabilities ===\n")
  cat("Date:", as.character(iDate), "\n")
  
  if (iDebug) {
    cat("[DEBUG MODE] No database writes will occur\n")
  }
  
  cat("\n")
  
  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))
  
  # Get ESPN Analytics source_id
  source_id <- dbGetQuery(
    con,
    "SELECT source_id FROM probability_sources
     WHERE source_name = 'ESPN Analytics'"
  )$source_id
  
  if (length(source_id) == 0 || is.na(source_id)) {
    stop("ESPN Analytics not found in probability_sources table.")
  }
  
  schedule <- get_espn_schedule(iDate)
  if (is.null(schedule)) return(invisible(NULL))
  
  scrape_timestamp <- format(
    Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"
  )
  
  results      <- list()
  games_stored <- 0
  
  for (ii in seq_len(nrow(schedule))) {
    
    game_id_espn <- schedule$id[ii]
    game_name    <- schedule$name[ii]
    game_time    <- schedule$date[ii]
    
    cat("Processing [", ii, "/", nrow(schedule), "]:",
        game_name, "\n")
    
    # Build URL and scrape probabilities
    game_url <- build_espn_game_url(game_id_espn, game_name)
    probs    <- scrape_espn_probabilities(game_url)
    
    if (is.null(probs)) {
      cat("  SKIPPED: could not extract probabilities\n")
      next
    }
    
    # Parse team names from game name
    teams     <- strsplit(game_name, " at ")[[1]]
    away_name <- trimws(teams[1])
    home_name <- trimws(teams[2])
    
    # Resolve team IDs
    home_team_id <- get_team_id(con, home_name)
    away_team_id <- get_team_id(con, away_name)
    
    if (is.na(home_team_id) || is.na(away_team_id)) {
      cat("  SKIPPED: team not found in database\n")
      next
    }
    
    # Get abbreviations and build canonical game_id
    home_abbr         <- get_team_abbr(con, home_team_id)
    away_abbr         <- get_team_abbr(con, away_team_id)
    canonical_game_id <- build_game_id(iDate, home_abbr, away_abbr)
    
    # Ensure game is registered
    ensure_game(
      con, canonical_game_id, iDate, game_time,
      home_team_id, away_team_id,
      iDebug = iDebug
    )
    
    # Store or preview probability for each team
    for (jj in seq_len(nrow(probs))) {
      
      team_name <- probs$team_name[jj]
      prob      <- probs$win_probability[jj]
      
      home_last <- tail(strsplit(home_name, " ")[[1]], 1)
      if (grepl(home_last, team_name, ignore.case = TRUE)) {
        team_id <- home_team_id
      } else {
        team_id <- away_team_id
      }
      
      if (iDebug) {
        cat("  [DEBUG] Would store:",
            team_name, "->",
            round(prob * 100, 1), "%",
            "| game_id:", canonical_game_id,
            "| team_id:", team_id,
            "| source_id:", source_id, "\n"
        )
      } else {
        dbExecute(con,
                  "INSERT INTO probability_snapshots
           (game_id, source_id, team_id,
            scrape_timestamp, win_probability)
           VALUES (?, ?, ?, ?, ?)",
                  params = list(
                    canonical_game_id, source_id, team_id,
                    scrape_timestamp, prob
                  )
        )
      }
      
    }
    
    if (!iDebug) {
      cat("  Stored:",
          probs$team_name[1],
          round(probs$win_probability[1] * 100, 1), "%  |  ",
          probs$team_name[2],
          round(probs$win_probability[2] * 100, 1), "%\n"
      )
    }
    
    results[[ii]] <- probs
    games_stored  <- games_stored + 1
    
    Sys.sleep(sleep_secs)
    
  }
  
  cat("\n=== Complete ===\n")
  if (iDebug) {
    cat("[DEBUG MODE] No data was written to the database\n")
  }
  cat("Games processed:", games_stored, "/", nrow(schedule), "\n")
  
  invisible(results)
  
}

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (!interactive()) {
  
  # Set iDebug = FALSE to test without writing to database
  # Set iDebug = FALSE for a live run
  fetch_and_store_probabilities(
    iDate  = Sys.Date(),
    iDebug = FALSE
  )
  
}

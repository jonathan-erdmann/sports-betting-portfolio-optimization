# =============================================================
# fetch_fangraphs.R
# Purpose:  Fetches MLB win probabilities from FanGraphs
#           API for all scheduled games on a given date.
#           Also updates starting pitcher information in
#           the games table.
# Layer:    1 - Data Acquisition
# Author:   Jonathan Erdmann
# =============================================================

library(httr)
library(jsonlite)
library(DBI)
library(RSQLite)
library(yaml)
library(here)

source(here("R", "acquisition", "utils.R"))

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------

config     <- yaml::read_yaml(here("config", "config.yml"))
user_agent <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
fg_headers <- add_headers(
  "User-Agent"      = user_agent,
  "Referer"         = "https://www.fangraphs.com/standings/playoff-odds",
  "Accept"          = "application/json, text/plain, */*",
  "Accept-Language" = "en-US,en;q=0.9",
  "Origin"          = "https://www.fangraphs.com"
)

# -------------------------------------------------------------
# Helper: fetch FanGraphs game data for a given date
# -------------------------------------------------------------

fetch_fangraphs_games <- function(iDate = Sys.Date()) {
  
  url <- paste0(
    "https://www.fangraphs.com/api/scores/most-recent?gamedate=",
    format(iDate, "%Y-%m-%d")
  )
  
  response <- GET(url, fg_headers)
  
  if (status_code(response) != 200) {
    stop("FanGraphs API failed with status: ",
         status_code(response))
  }
  
  parsed <- fromJSON(
    content(response, as = "text", encoding = "UTF-8"),
    flatten = TRUE
  )
  
  if (nrow(parsed) == 0) {
    cat("No games found for:", format(iDate, "%Y-%m-%d"), "\n")
    return(NULL)
  }
  
  # Filter to scheduled games only
  scheduled <- parsed[parsed$schedule.gamestate == "S", ]
  
  cat("FanGraphs games found:", nrow(parsed), "\n")
  cat("Scheduled games:", nrow(scheduled), "\n")
  
  scheduled
  
}

# -------------------------------------------------------------
# Helper: resolve game_id from MLBGameId via games table
# -------------------------------------------------------------

get_game_id_from_mlb_pk <- function(iCon, iMlbGameId) {
  
  # First check if we have this MLBGameId stored
  # We match via game_time since we store game times from MLB API
  result <- dbGetQuery(iCon, "
    SELECT g.game_id, g.home_team_id, g.away_team_id
    FROM games g
    WHERE g.game_id IN (
      SELECT game_id FROM games
      WHERE game_date >= DATE('now', 'localtime', '-1 day')
    )
  ")
  
  if (nrow(result) == 0) return(NULL)
  
  # Match via MLB Stats API lookup
  url <- paste0(
    "https://statsapi.mlb.com/api/v1/game/",
    iMlbGameId, "/feed/live?fields=gameData,game,pk"
  )
  
  response <- tryCatch(
    GET(url),
    error = function(e) NULL
  )
  
  if (is.null(response) || status_code(response) != 200) {
    return(NULL)
  }
  
  raw    <- content(response, as = "text", encoding = "UTF-8")
  parsed <- tryCatch(
    fromJSON(raw, flatten = TRUE),
    error = function(e) NULL
  )
  
  if (is.null(parsed)) return(NULL)
  
  # Get team IDs from MLB Stats API
  home_id <- tryCatch(
    parsed$gameData$teams$home$id,
    error = function(e) NULL
  )
  away_id <- tryCatch(
    parsed$gameData$teams$away$id,
    error = function(e) NULL
  )
  
  if (is.null(home_id) || is.null(away_id)) return(NULL)
  
  # Match to our games table by team IDs
  match <- dbGetQuery(iCon, "
    SELECT game_id FROM games
    WHERE home_team_id = ?
      AND away_team_id = ?
      AND game_date >= DATE('now', 'localtime', '-1 day')
    ORDER BY game_date DESC
    LIMIT 1
  ", params = list(home_id, away_id))
  
  if (nrow(match) == 0) return(NULL)
  match$game_id[1]
  
}

# -------------------------------------------------------------
# Main: fetch and store FanGraphs probabilities
# -------------------------------------------------------------

fetch_and_store_fangraphs <- function(iDate  = Sys.Date(),
                                      iDebug = FALSE) {
  
  cat("=== Fetching FanGraphs Probabilities ===\n")
  cat("Date:", format(iDate, "%Y-%m-%d"), "\n\n")
  
  if (iDebug) {
    cat("[DEBUG MODE] No database writes will occur\n\n")
  }
  
  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))
  
  # Get FanGraphs source_id
  source_id <- dbGetQuery(con, "
    SELECT source_id FROM probability_sources
    WHERE source_name = 'FanGraphs'
  ")$source_id
  
  if (length(source_id) == 0 || is.na(source_id)) {
    stop("FanGraphs not found in probability_sources table.")
  }
  
  # Fetch games from FanGraphs
  fg_games <- tryCatch(
    fetch_fangraphs_games(iDate),
    error = function(e) {
      cat("[ERROR] FanGraphs fetch failed:", 
          conditionMessage(e), "\n")
      NULL
    }
  )
  
  if (is.null(fg_games) || nrow(fg_games) == 0) {
    return(invisible(NULL))
  }
  
  scrape_timestamp <- format(
    Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"
  )
  
  stored    <- 0
  skipped   <- 0
  
  for (ii in seq_len(nrow(fg_games))) {
    
    mlb_game_id  <- fg_games$schedule.MLBGameId[ii]
    home_abbr_fg <- fg_games$schedule.HomeTeamAbbName[ii]
    away_abbr_fg <- fg_games$schedule.AwayTeamAbbName[ii]
    home_prob    <- fg_games$schedule.HomeGameOdds[ii]
    away_prob    <- fg_games$schedule.AwayGameOdds[ii]
    home_starter <- fg_games$schedule.HomeStarter[ii]
    away_starter <- fg_games$schedule.AwayStarter[ii]
    
    cat("Processing [", ii, "/", nrow(fg_games), "]:",
        fg_games$schedule.AwayTeamName[ii], "at",
        fg_games$schedule.HomeTeamName[ii], "\n")
    
    # Map FanGraphs abbreviations to our database abbreviations
    fg_to_our <- c(
      "ARI" = "AZ",  "CHW" = "CWS", "KCR" = "KC",
      "SDP" = "SD",  "SFG" = "SF",  "TBR" = "TB",
      "WSN" = "WSH"
    )
    
    home_abbr <- ifelse(home_abbr_fg %in% names(fg_to_our),
                        fg_to_our[home_abbr_fg], home_abbr_fg)
    away_abbr <- ifelse(away_abbr_fg %in% names(fg_to_our),
                        fg_to_our[away_abbr_fg], away_abbr_fg)
    
    # Use DH field for doubleheader game number
    dh_number <- fg_games$schedule.DH[ii]
    
    # For doubleheaders use game number suffix
    if (dh_number > 0) {
      home_team_lookup <- dbGetQuery(con, "
        SELECT team_id FROM teams WHERE abbreviation = ?
      ", params = list(home_abbr))
      away_team_lookup <- dbGetQuery(con, "
        SELECT team_id FROM teams WHERE abbreviation = ?
      ", params = list(away_abbr))
      
      if (nrow(home_team_lookup) > 0 && nrow(away_team_lookup) > 0) {
        canonical_game_id <- build_game_id(
          iDate, home_abbr, away_abbr, dh_number
        )
        game_lookup <- dbGetQuery(con, "
          SELECT g.game_id, g.home_team_id, g.away_team_id
          FROM games g
          WHERE g.game_id = ?
        ", params = list(canonical_game_id))
      } else {
        game_lookup <- data.frame()
      }
    } else {
      game_lookup <- dbGetQuery(con, "
        SELECT g.game_id, g.home_team_id, g.away_team_id
        FROM games g
        JOIN teams th ON g.home_team_id = th.team_id
        JOIN teams ta ON g.away_team_id = ta.team_id
        WHERE g.game_date     = ?
          AND th.abbreviation = ?
          AND ta.abbreviation = ?
        LIMIT 1
      ", params = list(as.character(iDate), home_abbr, away_abbr))
    }
    
    if (nrow(game_lookup) == 0) {
      cat("  SKIPPED: game not found in database\n")
      skipped <- skipped + 1
      next
    }
    
    canonical_game_id <- game_lookup$game_id[1]
    home_team_id      <- game_lookup$home_team_id[1]
    away_team_id      <- game_lookup$away_team_id[1]
    
    if (iDebug) {
      cat("  [DEBUG] Would store:",
          fg_games$schedule.HomeTeamName[ii], "->",
          round(home_prob * 100, 1), "%",
          "| game_id:", canonical_game_id, "\n")
      cat("  [DEBUG] Would store:",
          fg_games$schedule.AwayTeamName[ii], "->",
          round(away_prob * 100, 1), "%",
          "| game_id:", canonical_game_id, "\n")
      cat("  [DEBUG] Would update starters:",
          home_starter, "/", away_starter, "\n")
    } else {
      
      # Store home probability
      dbExecute(con, "
        INSERT INTO probability_snapshots
        (game_id, source_id, team_id,
         scrape_timestamp, win_probability)
        VALUES (?, ?, ?, ?, ?)
      ", params = list(
        canonical_game_id, source_id, home_team_id,
        scrape_timestamp, home_prob
      ))
      
      # Store away probability
      dbExecute(con, "
        INSERT INTO probability_snapshots
        (game_id, source_id, team_id,
         scrape_timestamp, win_probability)
        VALUES (?, ?, ?, ?, ?)
      ", params = list(
        canonical_game_id, source_id, away_team_id,
        scrape_timestamp, away_prob
      ))
      
      # Update starting pitchers in games table
      dbExecute(con, "
        UPDATE games
        SET home_starter = ?,
            away_starter = ?
        WHERE game_id = ?
      ", params = list(
        home_starter, away_starter, canonical_game_id
      ))
      
      cat("  Stored:",
          fg_games$schedule.HomeTeamName[ii],
          round(home_prob * 100, 1), "%  |  ",
          fg_games$schedule.AwayTeamName[ii],
          round(away_prob * 100, 1), "%\n")
      cat("  Starters:", away_starter, "vs", home_starter, "\n")
      
      stored <- stored + 1
      
    }
    
  }
  
  cat("\n=== Complete ===\n")
  if (iDebug) {
    cat("[DEBUG MODE] No data written to database\n")
  } else {
    cat("Games stored:  ", stored, "\n")
    cat("Games skipped: ", skipped, "\n")
  }
  
  invisible(NULL)
  
}

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (sys.nframe() == 0) {
  fetch_and_store_fangraphs(
    iDate  = Sys.Date() + 1,
    iDebug = TRUE
  )
}

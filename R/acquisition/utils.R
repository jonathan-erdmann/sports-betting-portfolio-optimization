# =============================================================
# utils.R
# Purpose:  Shared helper functions for all acquisition scripts
# Layer:    1 - Data Acquisition
# Author:   Jonathan Erdmann
# =============================================================

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

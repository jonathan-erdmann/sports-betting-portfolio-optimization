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
  
  # Clean team name before lookup
  iTeamName <- clean_team_name(iTeamName)
  
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
# Helper: clean team name
# Fixes issues like "Athletics Athletics" from ESPN
# -------------------------------------------------------------

clean_team_name <- function(iName) {
  
  iName <- trimws(iName)
  words <- strsplit(iName, " ")[[1]]
  nn    <- length(words)
  
  # Check for exact word repetition in first/second half
  if (nn %% 2 == 0) {
    half <- nn %/% 2
    if (identical(words[1:half], words[(half + 1):nn])) {
      return(paste(words[1:half], collapse = " "))
    }
  }
  
  iName
  
}

# -------------------------------------------------------------
# Helper: build canonical game_id
# Supports doubleheader disambiguation via iGameNumber
# -------------------------------------------------------------

build_game_id <- function(iGameDate, iHomeAbbr, iAwayAbbr,
                          iGameNumber = 1) {
  
  base <- paste0(
    "MLB_",
    gsub("-", "", as.character(iGameDate)),
    "_",
    toupper(iHomeAbbr),
    "_",
    toupper(iAwayAbbr)
  )
  
  if (iGameNumber > 1) {
    paste0(base, "_G", iGameNumber)
  } else {
    base
  }
  
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
# Helper: reconciliation check
# Logs any orphaned snapshots after acquisition runs
# -------------------------------------------------------------

check_orphaned_records <- function(iCon) {
  
  orphaned_probs <- dbGetQuery(iCon, "
    SELECT ps.snapshot_id, ps.game_id, ps.scrape_timestamp
    FROM probability_snapshots ps
    LEFT JOIN games g ON ps.game_id = g.game_id
    WHERE g.game_id IS NULL
  ")
  
  orphaned_odds <- dbGetQuery(iCon, "
    SELECT os.snapshot_id, os.game_id, os.scrape_timestamp
    FROM odds_snapshots os
    LEFT JOIN games g ON os.game_id = g.game_id
    WHERE g.game_id IS NULL
  ")
  
  orphaned_outcomes <- dbGetQuery(iCon, "
    SELECT o.outcome_id, o.game_id, o.recorded_timestamp
    FROM outcomes o
    LEFT JOIN games g ON o.game_id = g.game_id
    WHERE g.game_id IS NULL
  ")
  
  total <- nrow(orphaned_probs) +
           nrow(orphaned_odds)  +
           nrow(orphaned_outcomes)
  
  if (total == 0) {
    cat("  [OK] No orphaned records found\n")
  } else {
    if (nrow(orphaned_probs) > 0) {
      cat("  [WARN] Orphaned probability snapshots:",
          nrow(orphaned_probs), "\n")
      print(orphaned_probs)
    }
    if (nrow(orphaned_odds) > 0) {
      cat("  [WARN] Orphaned odds snapshots:",
          nrow(orphaned_odds), "\n")
      print(orphaned_odds)
    }
    if (nrow(orphaned_outcomes) > 0) {
      cat("  [WARN] Orphaned outcomes:",
          nrow(orphaned_outcomes), "\n")
      print(orphaned_outcomes)
    }
  }
  
  invisible(total)
  
}

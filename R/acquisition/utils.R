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

# -------------------------------------------------------------
# Helper: register today's games from MLB Stats API
# This should be called before fetching odds or probabilities
# to ensure canonical game IDs are established correctly
# including doubleheader disambiguation
# -------------------------------------------------------------

register_mlb_schedule <- function(iCon, iDate = Sys.Date(),
                                  iDebug = FALSE) {
  
  url <- paste0(
    "https://statsapi.mlb.com/api/v1/schedule",
    "?sportId=1",
    "&date=", format(iDate, "%Y-%m-%d")
  )
  
  response <- GET(url, add_headers(
    "User-Agent" = "Mozilla/5.0"
  ))
  
  if (status_code(response) != 200) {
    stop("MLB Stats API schedule request failed: ",
         status_code(response))
  }
  
  parsed <- fromJSON(
    content(response, as = "text", encoding = "UTF-8"),
    flatten = TRUE
  )
  
  if (parsed$totalGames == 0) {
    cat("No games scheduled for:", format(iDate, "%Y-%m-%d"), "\n")
    return(invisible(NULL))
  }
  
  games         <- parsed$dates$games[[1]]
  games_reg     <- 0
  game_registry <- list()
  
  for (ii in seq_len(nrow(games))) {
    
    home_name   <- games$teams.home.team.name[ii]
    away_name   <- games$teams.away.team.name[ii]
    game_time   <- games$gameDate[ii]
    game_date   <- as.Date(games$officialDate[ii])
    game_number <- games$gameNumber[ii]
    
    # Resolve team IDs
    home_team_id <- get_team_id(iCon, home_name)
    away_team_id <- get_team_id(iCon, away_name)
    
    if (is.na(home_team_id) || is.na(away_team_id)) {
      warning("Team not found — skipping: ",
              away_name, " at ", home_name)
      next
    }
    
    # Get abbreviations and build canonical game_id
    home_abbr         <- get_team_abbr(iCon, home_team_id)
    away_abbr         <- get_team_abbr(iCon, away_team_id)
    canonical_game_id <- build_game_id(game_date, home_abbr,
                                       away_abbr, game_number)
    
    # Register game
    ensure_game(
      iCon, canonical_game_id, game_date, game_time,
      home_team_id, away_team_id,
      iDebug = iDebug
    )
    
    # Store in registry for matching by other scripts
    game_registry[[ii]] <- list(
      game_id      = canonical_game_id,
      home_name    = home_name,
      away_name    = away_name,
      game_time    = game_time,
      game_date    = game_date,
      game_number  = game_number,
      home_team_id = home_team_id,
      away_team_id = away_team_id,
      home_abbr    = home_abbr,
      away_abbr    = away_abbr
    )
    
    games_reg <- games_reg + 1
    
  }
  
  cat("Registered", games_reg, "games for",
      format(iDate, "%Y-%m-%d"), "\n")
  
  invisible(game_registry)
  
}

# -------------------------------------------------------------
# Helper: match a game to the registry by time proximity
# Used to link ESPN and Odds API records to canonical game IDs
# -------------------------------------------------------------

match_game_to_registry <- function(iRegistry,
                                   iHomeName,
                                   iAwayName,
                                   iGameTime = NULL) {
  
  iHomeName <- clean_team_name(iHomeName)
  iAwayName <- clean_team_name(iAwayName)
  
  # Find candidates by team name match
  candidates <- Filter(function(ig) {
    ig$home_name == iHomeName && ig$away_name == iAwayName
  }, iRegistry)
  
  if (length(candidates) == 0) return(NULL)
  
  # Single match — return directly
  if (length(candidates) == 1) return(candidates[[1]])
  
  # Multiple matches — use time proximity
  if (!is.null(iGameTime)) {
    
    parse_time <- function(iT) {
      iT <- gsub("Z$", "+00:00", iT)
      as.POSIXct(iT, format = "%Y-%m-%dT%H:%M:%S+00:00",
                 tz = "UTC")
    }
    
    target_time <- parse_time(iGameTime)
    
    diffs <- sapply(candidates, function(ig) {
      cand_time <- parse_time(ig$game_time)
      abs(as.numeric(difftime(target_time, cand_time,
                              units = "mins")))
    })
    
    return(candidates[[which.min(diffs)]])
    
  }
  
  # No time provided — return first match
  candidates[[1]]
  
}

# -------------------------------------------------------------
# Helper: detect doubleheader time conflict
# Returns TRUE if gap between game times is suspiciously small
# -------------------------------------------------------------

detect_time_conflict <- function(iRegistry, iHomeName, iAwayName,
                                 iThresholdMins = 120) {
  
  iHomeName <- clean_team_name(iHomeName)
  iAwayName <- clean_team_name(iAwayName)
  
  candidates <- Filter(function(ig) {
    ig$home_name == iHomeName && ig$away_name == iAwayName
  }, iRegistry)
  
  if (length(candidates) < 2) return(FALSE)
  
  parse_time <- function(iT) {
    iT <- gsub("Z$", "+00:00", iT)
    as.POSIXct(iT, format = "%Y-%m-%dT%H:%M:%S+00:00",
               tz = "UTC")
  }
  
  times <- sapply(candidates, function(ig) {
    as.numeric(parse_time(ig$game_time))
  })
  times <- sort(times)
  
  min_gap_mins <- min(diff(times)) / 60
  min_gap_mins < iThresholdMins
  
}

# -------------------------------------------------------------
# Helper: log conflict to database
# -------------------------------------------------------------

log_conflict <- function(iCon, iGameDate, iMatchup,
                         iConflictType, iRegistryTimes,
                         iSourceTimes, iProposedMapping,
                         iFinalMapping, iResolution,
                         iResolvedBy, iNotes = NULL) {
  
  dbExecute(iCon, "
    INSERT INTO data_conflicts
    (detected_timestamp, game_date, matchup, conflict_type,
     registry_times, source_times, proposed_mapping,
     final_mapping, resolution, resolved_by, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    as.character(iGameDate),
    as.character(iMatchup),
    as.character(iConflictType),
    as.character(iRegistryTimes),
    as.character(iSourceTimes),
    if (is.null(iProposedMapping)) NA_character_ else
      as.character(iProposedMapping),
    if (is.null(iFinalMapping)) NA_character_ else
      as.character(iFinalMapping),
    as.character(iResolution),
    as.character(iResolvedBy),
    if (is.null(iNotes)) NA_character_ else
      as.character(iNotes)
  ))
  
}

# -------------------------------------------------------------
# Helper: human-in-the-loop doubleheader conflict resolution
# Returns a named list mapping source entries to game_ids
# or NULL if skipped
# -------------------------------------------------------------

resolve_doubleheader_conflict <- function(iCon,
                                          iRegistryCandidates,
                                          iSourceEntries,
                                          iGameDate,
                                          iMatchup) {
  
  # Sort registry candidates by game time
  parse_time <- function(iT) {
    iT <- gsub("Z$", "+00:00", iT)
    as.POSIXct(iT, format = "%Y-%m-%dT%H:%M:%S+00:00",
               tz = "UTC")
  }
  
  reg_times <- sapply(iRegistryCandidates,
                      function(ig) parse_time(ig$game_time))
  reg_order <- order(reg_times)
  reg_sorted <- iRegistryCandidates[reg_order]
  
  # Sort source entries by time
  src_times <- sapply(iSourceEntries,
                      function(is) parse_time(is$game_time))
  src_order <- order(src_times)
  src_sorted <- iSourceEntries[src_order]
  
  # Build proposed mapping (positional)
  proposed <- list()
  for (ii in seq_along(src_sorted)) {
    if (ii <= length(reg_sorted)) {
      proposed[[ii]] <- list(
        source = src_sorted[[ii]],
        target = reg_sorted[[ii]]
      )
    }
  }
  
  # Format times for display
  reg_times_str <- paste(sapply(reg_sorted, function(ig) {
    paste0("  [", which(reg_order == which(
      sapply(iRegistryCandidates,
             function(x) x$game_id == ig$game_id)
    )), "] ", ig$game_id, " — ", ig$game_time)
  }), collapse = "\n")
  
  src_times_str <- paste(sapply(seq_along(src_sorted),
                                function(ii) {
    paste0("  [", LETTERS[ii], "] ", src_sorted[[ii]]$game_time)
  }), collapse = "\n")
  
  prop_str <- paste(sapply(seq_along(proposed), function(ii) {
    paste0("  ", LETTERS[ii], " (", 
           proposed[[ii]]$source$game_time, ") → ",
           proposed[[ii]]$target$game_id)
  }), collapse = "\n")
  
  # Display conflict
  cat("\n")
  cat(rep("!", 55), "\n", sep = "")
  cat("  DOUBLEHEADER TIME CONFLICT DETECTED\n")
  cat("  Matchup:", iMatchup, "\n")
  cat("  Date:   ", as.character(iGameDate), "\n")
  cat(rep("!", 55), "\n", sep = "")
  cat("\n  Registry games (MLB Stats API):\n")
  cat(reg_times_str, "\n")
  cat("\n  Source entries:\n")
  cat(src_times_str, "\n")
  cat("\n  Proposed matching (positional by time):\n")
  cat(prop_str, "\n")
  cat("\n  Options:\n")
  cat("    Y — Accept proposed matching\n")
  cat("    M — Manual override\n")
  cat("    S — Skip this doubleheader\n")
  cat("\n  Enter choice [Y/M/S]: ")
  
  # Check if running interactively
  if (!isatty(stdin())) {
    cat("S (auto-skipped — non-interactive session)\n")
    log_conflict(
      iCon, iGameDate, iMatchup,
      "doubleheader_time_conflict",
      paste(sapply(reg_sorted, function(ig) ig$game_time),
            collapse = ", "),
      paste(sapply(src_sorted, function(is) is$game_time),
            collapse = ", "),
      prop_str, NA,
      "auto_skipped", "system",
      "Non-interactive session"
    )
    return(NULL)
  }
  
  tty    <- file("/dev/tty", open = "r", raw = TRUE)
  choice <- toupper(trimws(readLines(tty, n = 1)))
  close(tty)
  
  if (choice == "Y") {
    
    # Accept proposed mapping
    final_str <- prop_str
    log_conflict(
      iCon, iGameDate, iMatchup,
      "doubleheader_time_conflict",
      paste(sapply(reg_sorted, function(ig) ig$game_time),
            collapse = ", "),
      paste(sapply(src_sorted, function(is) is$game_time),
            collapse = ", "),
      prop_str, final_str,
      "accepted", "user", NULL
    )
    cat("  Accepted.\n\n")
    return(proposed)
    
  } else if (choice == "M") {
    
    # Manual override
    cat("\n  Enter mapping (e.g. A1,B2) or S to skip: ")
    tty         <- file("/dev/tty", open = "r", raw = TRUE)
    mapping_str <- toupper(trimws(readLines(tty, n = 1)))
    close(tty)
    
    if (mapping_str == "S") {
      log_conflict(
        iCon, iGameDate, iMatchup,
        "doubleheader_time_conflict",
        paste(sapply(reg_sorted, function(ig) ig$game_time),
              collapse = ", "),
        paste(sapply(src_sorted, function(is) is$game_time),
              collapse = ", "),
        prop_str, NA,
        "skipped", "user", "User skipped at manual override"
      )
      cat("  Skipped.\n\n")
      return(NULL)
    }
    
    # Parse mapping string e.g. "A1,B2"
    pairs    <- strsplit(mapping_str, ",")[[1]]
    manual   <- list()
    
    for (pp in pairs) {
      pp      <- trimws(pp)
      src_idx <- which(LETTERS == substr(pp, 1, 1))
      reg_idx <- as.integer(substr(pp, 2, nchar(pp)))
      
      if (length(src_idx) == 0 || is.na(reg_idx) ||
          src_idx > length(src_sorted) ||
          reg_idx > length(reg_sorted)) {
        cat("  [WARN] Invalid mapping:", pp, "— skipping\n")
        next
      }
      
      manual[[length(manual) + 1]] <- list(
        source = src_sorted[[src_idx]],
        target = reg_sorted[[reg_idx]]
      )
    }
    
    final_str <- paste(sapply(seq_along(manual), function(ii) {
      paste0(LETTERS[ii], " → ",
             manual[[ii]]$target$game_id)
    }), collapse = ", ")
    
    log_conflict(
      iCon, iGameDate, iMatchup,
      "doubleheader_time_conflict",
      paste(sapply(reg_sorted, function(ig) ig$game_time),
            collapse = ", "),
      paste(sapply(src_sorted, function(is) is$game_time),
            collapse = ", "),
      prop_str, final_str,
      "manual", "user", mapping_str
    )
    cat("  Manual mapping applied.\n\n")
    return(manual)
    
  } else {
    
    # Skip
    log_conflict(
      iCon, iGameDate, iMatchup,
      "doubleheader_time_conflict",
      paste(sapply(reg_sorted, function(ig) ig$game_time),
            collapse = ", "),
      paste(sapply(src_sorted, function(is) is$game_time),
            collapse = ", "),
      prop_str, NA,
      "skipped", "user", NULL
    )
    cat("  Skipped.\n\n")
    return(NULL)
    
  }
  
}

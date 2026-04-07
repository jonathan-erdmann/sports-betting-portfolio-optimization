# =============================================================
# remove_vig.R
# Purpose:  Computes fair market probabilities from raw
#           moneylines using multiplicative vig removal.
#           Operates on odds_snapshots table and returns
#           enriched records ready for EV calculation.
# Layer:    3 - Feature Engineering
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(yaml)
library(here)

# -------------------------------------------------------------
# Helper: convert American moneyline to implied probability
# -------------------------------------------------------------

ml_to_implied <- function(iMoneyline) {
  ifelse(
    iMoneyline < 0,
    (-iMoneyline) / (-iMoneyline + 100),
    100 / (iMoneyline + 100)
  )
}

# -------------------------------------------------------------
# Helper: remove vig from a pair of implied probabilities
# Uses multiplicative normalization
# Returns fair probabilities summing to 1.0
# -------------------------------------------------------------

remove_vig_multiplicative <- function(iImpliedHome,
                                      iImpliedAway) {
  overround  <- iImpliedHome + iImpliedAway
  fair_home  <- iImpliedHome / overround
  fair_away  <- iImpliedAway / overround
  list(
    fair_home  = fair_home,
    fair_away  = fair_away,
    overround  = overround,
    vig        = overround - 1
  )
}

# -------------------------------------------------------------
# Helper: compute consensus fair probability across bookmakers
# Takes the mean fair probability across all bookmakers
# for the most recent snapshot before game time
# -------------------------------------------------------------

get_consensus_fair_prob <- function(iCon, iGameId,
                                    iClosingWindowHours = 2) {
  
  snapshots <- dbGetQuery(iCon, "
    SELECT os.snapshot_id,
           os.home_moneyline,
           os.away_moneyline,
           os.scrape_timestamp,
           os.game_time,
           b.bookmaker_name,
           b.bookmaker_key
    FROM odds_snapshots os
    JOIN bookmakers b ON os.bookmaker_id = b.bookmaker_id
    WHERE os.game_id = ?
    ORDER BY os.scrape_timestamp DESC
  ", params = list(iGameId))
  
  if (nrow(snapshots) == 0) return(NULL)
  
  # Filter to closing window if game_time is available
  if (!is.na(snapshots$game_time[1])) {
    
    game_time <- as.POSIXct(
      snapshots$game_time[1],
      format = "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    )
    
    scrape_times <- as.POSIXct(
      snapshots$scrape_timestamp,
      format = "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    )
    
    hours_before <- as.numeric(difftime(
      game_time, scrape_times, units = "hours"
    ))
    
    # Always restrict to pre-game snapshots first
    pre_game <- hours_before >= 0
    if (any(pre_game)) {
      snapshots <- snapshots[pre_game, ]
    } else {
      # No pre-game snapshots available
      return(NULL)
    }
    
    # Apply closing window if snapshots exist within it
    hours_before <- hours_before[pre_game]
    in_window    <- hours_before <= iClosingWindowHours
    if (any(in_window)) {
      snapshots <- snapshots[in_window, ]
    }
    # If no snapshots in closing window, use all pre-game
    # snapshots (morning line is better than in-game line)
    
  }
  
  # Use most recent snapshot per bookmaker
  snapshots <- snapshots[!duplicated(
    snapshots$bookmaker_key
  ), ]
  
  # Compute fair probabilities per bookmaker
  snapshots$implied_home <- ml_to_implied(
    snapshots$home_moneyline
  )
  snapshots$implied_away <- ml_to_implied(
    snapshots$away_moneyline
  )
  snapshots$overround <- snapshots$implied_home +
    snapshots$implied_away
  snapshots$fair_home <- snapshots$implied_home /
    snapshots$overround
  snapshots$fair_away <- snapshots$implied_away /
    snapshots$overround
  snapshots$vig       <- snapshots$overround - 1
  
  # Consensus across bookmakers
  list(
    game_id          = iGameId,
    n_bookmakers     = nrow(snapshots),
    consensus_home   = mean(snapshots$fair_home),
    consensus_away   = mean(snapshots$fair_away),
    mean_vig         = mean(snapshots$vig),
    min_vig          = min(snapshots$vig),
    max_vig          = max(snapshots$vig),
    best_home_ml     = max(snapshots$home_moneyline),
    best_away_ml     = max(snapshots$away_moneyline),
    snapshots        = snapshots
  )
  
}

# -------------------------------------------------------------
# Main: compute fair probabilities for all games on a date
# -------------------------------------------------------------

compute_fair_probabilities <- function(iDate  = Sys.Date(),
                                       iDebug = FALSE) {
  
  cat("=== Computing Fair Market Probabilities ===\n")
  cat("Date:", as.character(iDate), "\n\n")
  
  config <- yaml::read_yaml(here("config", "config.yml"))
  closing_window <- config$feature_engineering$closing_window_hours
  
  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))
  
  # Get games for this date that have odds
  games <- dbGetQuery(con, "
    SELECT DISTINCT g.game_id,
           th.team_name AS home_team,
           ta.team_name AS away_team,
           g.game_time, g.status
    FROM games g
    JOIN teams th ON g.home_team_id = th.team_id
    JOIN teams ta ON g.away_team_id = ta.team_id
    JOIN odds_snapshots os ON g.game_id = os.game_id
    WHERE g.game_date = ?
    ORDER BY g.game_time
  ", params = list(as.character(iDate)))
  
  if (nrow(games) == 0) {
    cat("No games with odds found for:", as.character(iDate), "\n")
    return(invisible(NULL))
  }
  
  cat(sprintf("%-30s  %-6s  %-6s  %-5s  %-4s  %s\n",
              "Game", "Home%", "Away%", "Vig%", "Bkm", "Best Lines"))
  cat(rep("-", 75), "\n", sep = "")
  
  results <- list()
  
  for (ii in seq_len(nrow(games))) {
    
    game_id   <- games$game_id[ii]
    home_team <- games$home_team[ii]
    away_team <- games$away_team[ii]
    
    result <- get_consensus_fair_prob(
      con, game_id, closing_window
    )
    
    if (is.null(result)) next
    
    display_name <- paste0(
      substr(away_team, 1, 12), "@",
      substr(home_team, 1, 12)
    )
    
    cat(sprintf("%-30s  %5.1f%%  %5.1f%%  %4.1f%%  %3d  H:%+d A:%+d\n",
                display_name,
                result$consensus_home * 100,
                result$consensus_away * 100,
                result$mean_vig * 100,
                result$n_bookmakers,
                result$best_home_ml,
                result$best_away_ml
    ))
    
    results[[game_id]] <- result
    
  }
  
  cat("\n")
  invisible(results)
  
}

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (sys.nframe() == 0) {
  compute_fair_probabilities(
    iDate  = Sys.Date(),
    iDebug = FALSE
  )
}
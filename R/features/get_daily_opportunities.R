# =============================================================
# get_daily_opportunities.R
# Purpose:  Computes daily betting opportunities by combining
#           vig removal, Bayesian posterior probabilities,
#           EV calculation, and Kelly criterion sizing.
#           Writes results to daily_opportunities table.
# Layer:    3 - Feature Engineering
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(yaml)
library(here)

source(here("R", "features", "remove_vig.R"))
source(here("R", "features", "get_posterior_probability.R"))

# -------------------------------------------------------------
# Helper: convert American moneyline to decimal odds (net)
# -------------------------------------------------------------

ml_to_decimal <- function(iMoneyline) {
  ifelse(
    iMoneyline > 0,
    iMoneyline / 100,
    100 / (-iMoneyline)
  )
}

# -------------------------------------------------------------
# Helper: compute EV given posterior probability and net odds
# EV = p * b - (1 - p)
# where b is net profit per unit staked
# -------------------------------------------------------------

compute_ev <- function(iPosterior, iNetOdds) {
  iPosterior * iNetOdds - (1 - iPosterior)
}

# -------------------------------------------------------------
# Helper: compute full Kelly fraction
# f* = (p * b - (1 - p)) / b = EV / b
# -------------------------------------------------------------

compute_kelly <- function(iPosterior, iNetOdds) {
  ev <- compute_ev(iPosterior, iNetOdds)
  ifelse(ev <= 0, 0, ev / iNetOdds)
}

# -------------------------------------------------------------
# Helper: get best available moneyline per game/team
# Returns bookmaker offering highest EV for that side
# -------------------------------------------------------------

get_best_line <- function(iCon, iGameId, iTeamId,
                          iSide, iClosingWindowHours) {
  
  # Get most recent snapshot per bookmaker within closing window
  snapshots <- dbGetQuery(iCon, "
    SELECT os.snapshot_id,
           os.home_moneyline,
           os.away_moneyline,
           os.scrape_timestamp,
           os.game_time,
           os.bookmaker_id,
           b.bookmaker_name
    FROM odds_snapshots os
    JOIN bookmakers b ON os.bookmaker_id = b.bookmaker_id
    WHERE os.game_id = ?
    ORDER BY os.scrape_timestamp DESC
  ", params = list(iGameId))
  
  if (nrow(snapshots) == 0) return(NULL)
  
  # Filter to closing window
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
    in_window <- hours_before >= 0 &
      hours_before <= iClosingWindowHours
    if (any(in_window)) snapshots <- snapshots[in_window, ]
  }
  
  # Most recent snapshot per bookmaker
  snapshots <- snapshots[!duplicated(snapshots$bookmaker_id), ]
  
  # Select relevant moneyline column
  if (iSide == "home") {
    snapshots$moneyline <- snapshots$home_moneyline
  } else {
    snapshots$moneyline <- snapshots$away_moneyline
  }
  
  # Best line = highest moneyline value (most favorable)
  best_idx <- which.max(snapshots$moneyline)
  
  list(
    moneyline      = snapshots$moneyline[best_idx],
    bookmaker_id   = snapshots$bookmaker_id[best_idx],
    bookmaker_name = snapshots$bookmaker_name[best_idx]
  )
  
}

# -------------------------------------------------------------
# Helper: apply exposure caps to portfolio
# Scales down bets to respect per-game and daily caps
# -------------------------------------------------------------

apply_exposure_caps <- function(iOpportunities, iConfig) {
  
  daily_cap    <- iConfig$feature_engineering$daily_exposure_cap
  game_cap     <- iConfig$feature_engineering$per_game_exposure_cap
  same_game_cap <- iConfig$feature_engineering$max_same_game_exposure
  
  if (nrow(iOpportunities) == 0) return(iOpportunities)
  
  # Step 1 — Apply per-game cap
  # For each game, check total exposure across all sides
  game_ids <- unique(iOpportunities$game_id)
  
  for (gg in game_ids) {
    
    game_rows <- which(iOpportunities$game_id == gg)
    game_exposure <- sum(iOpportunities$kelly_fractional[game_rows])
    
    if (game_exposure > game_cap) {
      scale <- game_cap / game_exposure
      iOpportunities$kelly_fractional[game_rows] <-
        iOpportunities$kelly_fractional[game_rows] * scale
      iOpportunities$scaled[game_rows] <- TRUE
    }
    
    # Same-game cap when both sides are present
    if (length(game_rows) > 1) {
      same_game_exposure <- sum(
        iOpportunities$kelly_fractional[game_rows]
      )
      if (same_game_exposure > same_game_cap) {
        scale <- same_game_cap / same_game_exposure
        iOpportunities$kelly_fractional[game_rows] <-
          iOpportunities$kelly_fractional[game_rows] * scale
        iOpportunities$scaled[game_rows] <- TRUE
      }
    }
    
  }
  
  # Step 2 — Apply daily total cap
  total_exposure <- sum(iOpportunities$kelly_fractional)
  
  if (total_exposure > daily_cap) {
    scale <- daily_cap / total_exposure
    iOpportunities$kelly_fractional <-
      iOpportunities$kelly_fractional * scale
    iOpportunities$scaled <- TRUE
  }
  
  iOpportunities
  
}

# -------------------------------------------------------------
# Main: compute and store daily opportunities
# -------------------------------------------------------------

compute_daily_opportunities <- function(iDate  = Sys.Date(),
                                        iDebug = FALSE) {
  
  cat("=== Computing Daily Opportunities ===\n")
  cat("Date:", as.character(iDate), "\n\n")
  
  config         <- yaml::read_yaml(here("config", "config.yml"))
  closing_window <- config$feature_engineering$closing_window_hours
  min_edge       <- config$feature_engineering$min_edge
  kelly_fraction <- config$feature_engineering$kelly_fraction
  
  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))
  
  # Get games for this date with both odds and probabilities
  games <- dbGetQuery(con, "
    SELECT DISTINCT g.game_id,
           g.home_team_id, g.away_team_id,
           g.game_time, g.game_date,
           th.team_name AS home_team,
           ta.team_name AS away_team
    FROM games g
    JOIN teams th ON g.home_team_id = th.team_id
    JOIN teams ta ON g.away_team_id = ta.team_id
    JOIN odds_snapshots os        ON g.game_id = os.game_id
    JOIN probability_snapshots ps ON g.game_id = ps.game_id
    WHERE g.game_date = ?
      AND g.status    = 'scheduled'
    ORDER BY g.game_time
  ", params = list(as.character(iDate)))
  
  if (nrow(games) == 0) {
    cat("No eligible games found for:", as.character(iDate), "\n")
    return(invisible(NULL))
  }
  
  cat("Eligible games:", nrow(games), "\n\n")
  
  # Get active probability sources with snapshots today
  sources <- dbGetQuery(con, "
    SELECT DISTINCT ps.source_id, src.source_name
    FROM probability_snapshots ps
    JOIN probability_sources src ON ps.source_id = src.source_id
    JOIN games g ON ps.game_id = g.game_id
    WHERE g.game_date = ?
      AND src.active  = 1
  ", params = list(as.character(iDate)))
  
  if (nrow(sources) == 0) {
    cat("No probability sources with data for:",
        as.character(iDate), "\n")
    return(invisible(NULL))
  }
  
  # Get confidence weights per source
  weights <- list()
  for (ss in seq_len(nrow(sources))) {
    ww <- get_confidence_weight(
      con, sources$source_id[ss], config
    )
    weights[[sources$source_id[ss]]] <- ww$weight
  }
  
  scrape_timestamp <- format(
    Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"
  )
  
  opportunities <- data.frame()
  
  for (ii in seq_len(nrow(games))) {
    
    game_id      <- games$game_id[ii]
    home_team_id <- games$home_team_id[ii]
    away_team_id <- games$away_team_id[ii]
    home_team    <- games$home_team[ii]
    away_team    <- games$away_team[ii]
    game_time    <- games$game_time[ii]
    
    # Compute time to game
    game_posix <- as.POSIXct(
      game_time,
      format = "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    )
    time_to_game_hours <- as.numeric(difftime(
      game_posix, Sys.time(), units = "hours"
    ))
    
    # Skip games that have already started or have no time
    if (is.na(time_to_game_hours) || time_to_game_hours < 0) next
    
    # Get market fair probabilities
    market <- get_consensus_fair_prob(
      con, game_id, closing_window
    )
    if (is.null(market)) next
    
    # Process each side (home and away)
    sides <- list(
      list(team_id = home_team_id,
           team    = home_team,
           side    = "home",
           mkt_prob = market$consensus_home),
      list(team_id = away_team_id,
           team    = away_team,
           side    = "away",
           mkt_prob = market$consensus_away)
    )
    
    for (sd in sides) {
      
      # Get best available line for this side
      best_line <- get_best_line(
        con, game_id, sd$team_id,
        sd$side, closing_window
      )
      if (is.null(best_line)) next
      
      moneyline <- best_line$moneyline
      net_odds  <- ml_to_decimal(moneyline)
      
      # Get source probabilities and compute posterior
      # Use best available source (highest weight with data)
      best_source_id   <- NA_integer_
      best_source_prob <- NA_real_
      best_weight      <- 0
      
      for (ss in seq_len(nrow(sources))) {
        
        source_id <- sources$source_id[ss]
        
        src_prob <- dbGetQuery(con, "
          SELECT win_probability
          FROM probability_snapshots
          WHERE game_id   = ?
            AND team_id   = ?
            AND source_id = ?
          ORDER BY scrape_timestamp DESC
          LIMIT 1
        ", params = list(game_id, sd$team_id, source_id))
        
        if (nrow(src_prob) == 0) next
        
        ww <- weights[[source_id]]
        if (ww > best_weight || is.na(best_source_prob)) {
          best_source_id   <- source_id
          best_source_prob <- src_prob$win_probability[1]
          best_weight      <- ww
        }
        
      }
      
      if (is.na(best_source_prob)) next
      
      # Compute posterior probability
      posterior <- get_posterior(
        best_source_prob, sd$mkt_prob, best_weight
      )
      
      # Compute EV and Kelly
      ev            <- compute_ev(posterior, net_odds)
      kelly_full    <- compute_kelly(posterior, net_odds)
      kelly_frac    <- kelly_full * kelly_fraction
      
      # Skip if below minimum edge threshold
      if (ev < min_edge) next
      
      # Build opportunity record
      opp <- data.frame(
        game_id               = game_id,
        team_id               = sd$team_id,
        snapshot_timestamp    = scrape_timestamp,
        moneyline             = as.integer(moneyline),
        implied_prob_raw      = ml_to_implied(moneyline),
        implied_prob_fair     = sd$mkt_prob,
        source_id             = best_source_id,
        source_probability    = best_source_prob,
        confidence_weight     = best_weight,
        posterior_probability = posterior,
        expected_value        = ev,
        kelly_full            = kelly_full,
        kelly_fractional      = kelly_frac,
        time_to_game_hours    = time_to_game_hours,
        scaled                = FALSE,
        stringsAsFactors      = FALSE
      )
      
      opportunities <- rbind(opportunities, opp)
      
    }
    
  }
  
  if (nrow(opportunities) == 0) {
    cat("No positive EV opportunities found",
        "(min edge:", min_edge * 100, "%)\n")
    return(invisible(NULL))
  }
  
  # Apply exposure caps
  opportunities <- apply_exposure_caps(opportunities, config)
  
  # Sort by EV descending
  opportunities <- opportunities[
    order(-opportunities$expected_value), ]
  
  # Print summary
  cat(sprintf("%-25s  %-6s  %-6s  %-6s  %-6s  %-6s  %s\n",
              "Game/Team", "ML", "Mkt%", "Post%", "EV%",
              "Kelly%", "Book"))
  cat(rep("-", 80), "\n", sep = "")
  
  for (ii in seq_len(nrow(opportunities))) {
    
    opp <- opportunities[ii, ]
    
    # Get team and bookmaker names for display
    team_name <- dbGetQuery(con, "
      SELECT team_name FROM teams WHERE team_id = ?
    ", params = list(opp$team_id))$team_name
    
    bm_name <- dbGetQuery(con, "
      SELECT bookmaker_name FROM bookmakers
      WHERE bookmaker_id = (
        SELECT bookmaker_id FROM odds_snapshots
        WHERE game_id = ? AND home_moneyline = ?
           OR game_id = ? AND away_moneyline = ?
        LIMIT 1
      )
    ", params = list(opp$game_id, opp$moneyline,
                     opp$game_id, opp$moneyline))
    
    bm_display <- if (nrow(bm_name) > 0) bm_name$bookmaker_name[1] else "?"
    scaled_flag <- if (opp$scaled) "*" else ""
    
    cat(sprintf("%-25s  %+5d  %5.1f%%  %5.1f%%  %5.1f%%  %5.2f%%%s  %s\n",
                substr(team_name, 1, 25),
                opp$moneyline,
                opp$implied_prob_fair * 100,
                opp$posterior_probability * 100,
                opp$expected_value * 100,
                opp$kelly_fractional * 100,
                scaled_flag,
                bm_display
    ))
    
  }
  
  cat(rep("-", 80), "\n", sep = "")
  cat(sprintf(
    "Total exposure: %.2f%%  |  Opportunities: %d  |  * = scaled\n",
    sum(opportunities$kelly_fractional) * 100,
    nrow(opportunities)
  ))
  
  # Write to database
  if (!iDebug) {
    
    # Remove existing opportunities for this date
    existing_games <- paste0("'",
                             unique(opportunities$game_id), "'",
                             collapse = ", ")
    
    dbExecute(con, paste0("
      DELETE FROM daily_opportunities
      WHERE game_id IN (", existing_games, ")
        AND DATE(snapshot_timestamp) = '",
                          as.character(iDate), "'
    "))
    
    # Insert new opportunities
    for (ii in seq_len(nrow(opportunities))) {
      opp <- opportunities[ii, ]
      dbExecute(con, "
        INSERT INTO daily_opportunities
        (game_id, team_id, snapshot_timestamp,
         moneyline, implied_prob_raw, implied_prob_fair,
         source_id, source_probability, confidence_weight,
         posterior_probability, expected_value,
         kelly_full, kelly_fractional, time_to_game_hours)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ", params = list(
        opp$game_id, opp$team_id, opp$snapshot_timestamp,
        opp$moneyline, opp$implied_prob_raw,
        opp$implied_prob_fair, opp$source_id,
        opp$source_probability, opp$confidence_weight,
        opp$posterior_probability, opp$expected_value,
        opp$kelly_full, opp$kelly_fractional,
        opp$time_to_game_hours
      ))
    }
    
    cat("\nWritten to daily_opportunities table\n")
    
  } else {
    cat("\n[DEBUG MODE] No data written to database\n")
  }
  
  invisible(opportunities)
  
}

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (sys.nframe() == 0) {
  compute_daily_opportunities(
    iDate  = Sys.Date(),
    iDebug = FALSE
  )
}
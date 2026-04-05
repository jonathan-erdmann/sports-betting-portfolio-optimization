# =============================================================
# get_posterior_probability.R
# Purpose:  Computes Bayesian posterior win probabilities by
#           blending source model probabilities with market
#           fair probabilities using a confidence weight.
#           Updates confidence weights via Brier scores when
#           sufficient outcome data is available.
# Layer:    3 - Feature Engineering
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(yaml)
library(here)

source(here("R", "features", "remove_vig.R"))

# -------------------------------------------------------------
# Helper: compute Brier score for a source
# Brier = mean((p_predicted - outcome)^2)
# Lower is better
# -------------------------------------------------------------

compute_brier_score <- function(iCon, iSourceId,
                                iDecay = 0.95) {
  
  # Join probability snapshots to outcomes
  records <- dbGetQuery(iCon, "
    SELECT ps.win_probability AS predicted,
           o.win              AS outcome,
           o.recorded_timestamp
    FROM probability_snapshots ps
    JOIN outcomes o ON ps.game_id  = o.game_id
                   AND ps.team_id  = o.team_id
    WHERE ps.source_id = ?
    ORDER BY o.recorded_timestamp ASC
  ", params = list(iSourceId))
  
  if (nrow(records) == 0) return(list(brier_score = NA_real_, n_obs = 0))
  
  nn <- nrow(records)
  
  # Exponential decay weights — most recent games weighted most
  weights <- iDecay ^ (nn - seq_len(nn))
  weights <- weights / sum(weights)
  
  # Weighted Brier score
  brier <- sum(weights * (records$predicted - records$outcome)^2)
  
  list(
    brier_score = brier,
    n_obs       = nn
  )
  
}

# -------------------------------------------------------------
# Helper: compute market Brier score
# Uses consensus fair probability as the market prediction
# -------------------------------------------------------------

compute_market_brier_score <- function(iCon, iDecay = 0.95) {
  
  # Get consensus fair probability per game/team from odds
  records <- dbGetQuery(iCon, "
    SELECT os.game_id,
           CASE
             WHEN t.team_id = g.home_team_id THEN 'home'
             ELSE 'away'
           END AS side,
           os.home_moneyline,
           os.away_moneyline,
           o.win,
           o.recorded_timestamp,
           t.team_id
    FROM odds_snapshots os
    JOIN games g  ON os.game_id    = g.game_id
    JOIN outcomes o ON os.game_id  = o.game_id
    JOIN teams t    ON o.team_id   = t.team_id
    WHERE os.snapshot_id IN (
      SELECT MAX(snapshot_id)
      FROM odds_snapshots
      GROUP BY game_id, bookmaker_id
    )
    ORDER BY o.recorded_timestamp ASC
  ")
  
  if (nrow(records) == 0) return(NA_real_)
  
  # Compute fair probability per row
  records$implied_home <- records$home_moneyline / 
    ifelse(records$home_moneyline < 0,
           records$home_moneyline - 100,
           records$home_moneyline + 100) * -1
  
  # Use vectorized ml_to_implied
  records$implied_home <- ml_to_implied(records$home_moneyline)
  records$implied_away <- ml_to_implied(records$away_moneyline)
  records$overround    <- records$implied_home +
    records$implied_away
  records$fair_home    <- records$implied_home / records$overround
  records$fair_away    <- records$implied_away / records$overround
  
  records$predicted <- ifelse(
    records$side == "home",
    records$fair_home,
    records$fair_away
  )
  
  # Average across bookmakers per game/team
  agg <- aggregate(
    cbind(predicted, win) ~ game_id + team_id + recorded_timestamp,
    data = records,
    FUN  = mean
  )
  agg <- agg[order(agg$recorded_timestamp), ]
  
  nn      <- nrow(agg)
  weights <- iDecay ^ (nn - seq_len(nn))
  weights <- weights / sum(weights)
  
  brier <- sum(weights * (agg$predicted - agg$win)^2)
  
  list(
    brier_score = brier,
    n_obs       = nn
  )
  
}

# -------------------------------------------------------------
# Helper: get or compute confidence weight for a source
# Uses empirical Brier scores if sufficient data exists
# Falls back to prior weight otherwise
# -------------------------------------------------------------

get_confidence_weight <- function(iCon, iSourceId,
                                  iConfig) {
  
  prior_weight <- iConfig$bayesian$prior_weight
  min_weight   <- iConfig$bayesian$min_weight
  max_weight   <- iConfig$bayesian$max_weight
  decay        <- iConfig$bayesian$brier_decay
  min_sample   <- iConfig$bayesian$min_sample_brier
  
  # Check if we have enough data for empirical weight
  source_brier <- compute_brier_score(iCon, iSourceId, decay)
  
  if (is.na(source_brier$brier_score) ||
      source_brier$n_obs < min_sample) {
    cat(sprintf(
      "  Using prior weight %.2f (only %d observations, need %d)\n",
      prior_weight,
      ifelse(is.na(source_brier$n_obs), 0, source_brier$n_obs),
      min_sample
    ))
    return(list(
      weight       = prior_weight,
      method       = "prior",
      n_obs        = ifelse(is.na(source_brier$n_obs), 0,
                            source_brier$n_obs),
      brier_source = NA_real_,
      brier_market = NA_real_
    ))
  }
  
  # Compute market Brier score for comparison
  market_brier <- compute_market_brier_score(iCon, decay)
  
  if (is.na(market_brier$brier_score)) {
    return(list(
      weight       = prior_weight,
      method       = "prior",
      n_obs        = source_brier$n_obs,
      brier_source = source_brier$brier_score,
      brier_market = NA_real_
    ))
  }
  
  # Empirical weight: how much better is our source vs market?
  # w = clip(1 - BS_source / BS_market, min, max)
  raw_weight <- 1 - (source_brier$brier_score /
                       market_brier$brier_score)
  weight     <- max(min_weight,
                    min(max_weight, raw_weight))
  
  cat(sprintf(
    "  Empirical weight: %.3f (source Brier: %.4f, market Brier: %.4f)\n",
    weight,
    source_brier$brier_score,
    market_brier$brier_score
  ))
  
  list(
    weight       = weight,
    method       = "empirical",
    n_obs        = source_brier$n_obs,
    brier_source = source_brier$brier_score,
    brier_market = market_brier$brier_score
  )
  
}

# -------------------------------------------------------------
# Helper: compute posterior probability for one game/team
# -------------------------------------------------------------

get_posterior <- function(iSourceProb, iMarketFairProb,
                          iWeight) {
  iWeight * iSourceProb + (1 - iWeight) * iMarketFairProb
}

# -------------------------------------------------------------
# Main: compute posterior probabilities for all games on date
# -------------------------------------------------------------

compute_posterior_probabilities <- function(iDate  = Sys.Date(),
                                            iDebug = FALSE) {
  
  cat("=== Computing Posterior Probabilities ===\n")
  cat("Date:", as.character(iDate), "\n\n")
  
  config <- yaml::read_yaml(here("config", "config.yml"))
  
  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))
  
  # Get active probability sources
  sources <- dbGetQuery(con, "
    SELECT source_id, source_name
    FROM probability_sources
    WHERE active = 1
  ")
  
  # Get games for this date with both odds and probabilities
  games <- dbGetQuery(con, "
    SELECT DISTINCT g.game_id,
           g.home_team_id, g.away_team_id,
           g.game_time,
           th.team_name AS home_team,
           ta.team_name AS away_team
    FROM games g
    JOIN teams th ON g.home_team_id = th.team_id
    JOIN teams ta ON g.away_team_id = ta.team_id
    JOIN odds_snapshots os        ON g.game_id = os.game_id
    JOIN probability_snapshots ps ON g.game_id = ps.game_id
    WHERE g.game_date = ?
    ORDER BY g.game_time
  ", params = list(as.character(iDate)))
  
  if (nrow(games) == 0) {
    cat("No games with both odds and probabilities for:",
        as.character(iDate), "\n")
    return(invisible(NULL))
  }
  
  cat("Games with odds and probabilities:", nrow(games), "\n\n")
  
  # Get market fair probabilities for all games
  closing_window <- config$feature_engineering$closing_window_hours
  
  results <- list()
  
  for (ss in seq_len(nrow(sources))) {
    
    source_id   <- sources$source_id[ss]
    source_name <- sources$source_name[ss]
    
    cat("--- Source:", source_name, "---\n")
    
    # Get confidence weight for this source
    weight_info <- get_confidence_weight(con, source_id, config)
    ww          <- weight_info$weight
    
    cat(sprintf("  %-30s  %-8s  %-8s  %-8s  %s\n",
                "Game", "Mkt%", "Src%", "Post%", "Edge"))
    cat("  ", rep("-", 65), "\n", sep = "")
    
    for (ii in seq_len(nrow(games))) {
      
      game_id      <- games$game_id[ii]
      home_team_id <- games$home_team_id[ii]
      away_team_id <- games$away_team_id[ii]
      home_team    <- games$home_team[ii]
      away_team    <- games$away_team[ii]
      
      # Get market fair probability
      market <- get_consensus_fair_prob(
        con, game_id, closing_window
      )
      if (is.null(market)) next
      
      # Get source probabilities for this game
      src_probs <- dbGetQuery(con, "
        SELECT team_id, win_probability
        FROM probability_snapshots
        WHERE game_id   = ?
          AND source_id = ?
      ", params = list(game_id, source_id))
      
      if (nrow(src_probs) == 0) next
      
      # Match source probs to home/away
      src_home <- src_probs$win_probability[
        src_probs$team_id == home_team_id]
      src_away <- src_probs$win_probability[
        src_probs$team_id == away_team_id]
      
      if (length(src_home) == 0 || length(src_away) == 0) next
      
      # Use most recent source probability
      src_home <- tail(src_home, 1)
      src_away <- tail(src_away, 1)
      
      # Compute posterior
      post_home <- get_posterior(src_home,
                                 market$consensus_home, ww)
      post_away <- get_posterior(src_away,
                                 market$consensus_away, ww)
      
      # Edge = posterior - market fair
      edge_home <- post_home - market$consensus_home
      edge_away <- post_away - market$consensus_away
      
      display <- paste0(
        substr(away_team, 1, 10), "@",
        substr(home_team, 1, 10)
      )
      
      cat(sprintf(
        "  %-30s  %5.1f%%  %5.1f%%  %5.1f%%  %+.1f%%\n",
        display,
        market$consensus_home * 100,
        src_home * 100,
        post_home * 100,
        edge_home * 100
      ))
      
      results[[paste(game_id, source_id)]] <- list(
        game_id          = game_id,
        source_id        = source_id,
        home_team_id     = home_team_id,
        away_team_id     = away_team_id,
        market_home      = market$consensus_home,
        market_away      = market$consensus_away,
        source_home      = src_home,
        source_away      = src_away,
        posterior_home   = post_home,
        posterior_away   = post_away,
        edge_home        = edge_home,
        edge_away        = edge_away,
        weight           = ww,
        n_bookmakers     = market$n_bookmakers,
        mean_vig         = market$mean_vig
      )
      
    }
    
    cat("\n")
    
  }
  
  invisible(results)
  
}

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (sys.nframe() == 0) {
  compute_posterior_probabilities(
    iDate  = Sys.Date(),
    iDebug = FALSE
  )
}
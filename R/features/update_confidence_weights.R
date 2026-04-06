# =============================================================
# update_confidence_weights.R
# Purpose:  Computes and stores confidence weights for each
#           active probability source based on empirical
#           Brier scores relative to the market.
#           Falls back to prior weights when insufficient
#           outcome data is available.
# Layer:    3 - Feature Engineering
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(yaml)
library(here)

source(here("R", "features", "remove_vig.R"))

# -------------------------------------------------------------
# Helper: compute exponentially weighted Brier score
# for a given source against recorded outcomes
# -------------------------------------------------------------

compute_source_brier <- function(iCon, iSourceId,
                                 iDecay = 0.95) {
  
  records <- dbGetQuery(iCon, "
    SELECT ps.win_probability AS predicted,
           o.win              AS outcome,
           o.recorded_timestamp
    FROM probability_snapshots ps
    JOIN outcomes o ON ps.game_id = o.game_id
                   AND ps.team_id = o.team_id
    WHERE ps.source_id = ?
    ORDER BY o.recorded_timestamp ASC
  ", params = list(iSourceId))
  
  if (nrow(records) == 0) {
    return(list(brier_score = NA_real_, n_obs = 0L))
  }
  
  nn      <- nrow(records)
  weights <- iDecay ^ (nn - seq_len(nn))
  weights <- weights / sum(weights)
  
  brier <- sum(weights *
               (records$predicted - records$outcome)^2)
  
  list(brier_score = brier, n_obs = nn)
  
}

# -------------------------------------------------------------
# Helper: compute exponentially weighted market Brier score
# Uses consensus fair probability as market prediction
# -------------------------------------------------------------

compute_market_brier <- function(iCon, iDecay = 0.95) {
  
  # Get consensus fair probability per game/team from outcomes
  records <- dbGetQuery(iCon, "
    SELECT g.game_id,
           o.team_id,
           o.win,
           o.recorded_timestamp,
           g.home_team_id,
           g.away_team_id,
           AVG(CASE
             WHEN o.team_id = g.home_team_id THEN
               CASE WHEN os.home_moneyline < 0
                 THEN CAST(-os.home_moneyline AS REAL) /
                      (-os.home_moneyline + 100)
                 ELSE 100.0 / (os.home_moneyline + 100)
               END /
               (CASE WHEN os.home_moneyline < 0
                 THEN CAST(-os.home_moneyline AS REAL) /
                      (-os.home_moneyline + 100)
                 ELSE 100.0 / (os.home_moneyline + 100)
               END +
               CASE WHEN os.away_moneyline < 0
                 THEN CAST(-os.away_moneyline AS REAL) /
                      (-os.away_moneyline + 100)
                 ELSE 100.0 / (os.away_moneyline + 100)
               END)
             ELSE
               CASE WHEN os.away_moneyline < 0
                 THEN CAST(-os.away_moneyline AS REAL) /
                      (-os.away_moneyline + 100)
                 ELSE 100.0 / (os.away_moneyline + 100)
               END /
               (CASE WHEN os.home_moneyline < 0
                 THEN CAST(-os.home_moneyline AS REAL) /
                      (-os.home_moneyline + 100)
                 ELSE 100.0 / (os.home_moneyline + 100)
               END +
               CASE WHEN os.away_moneyline < 0
                 THEN CAST(-os.away_moneyline AS REAL) /
                      (-os.away_moneyline + 100)
                 ELSE 100.0 / (os.away_moneyline + 100)
               END)
           END) AS predicted
    FROM outcomes o
    JOIN games g         ON o.game_id  = g.game_id
    JOIN odds_snapshots os ON o.game_id = os.game_id
    GROUP BY g.game_id, o.team_id
    ORDER BY o.recorded_timestamp ASC
  ")
  
  if (nrow(records) == 0) {
    return(list(brier_score = NA_real_, n_obs = 0L))
  }
  
  nn      <- nrow(records)
  weights <- iDecay ^ (nn - seq_len(nn))
  weights <- weights / sum(weights)
  
  brier <- sum(weights * (records$predicted - records$win)^2)
  
  list(brier_score = brier, n_obs = nn)
  
}

# -------------------------------------------------------------
# Helper: compute raw confidence weight from Brier scores
# w = clip(1 - BS_source / BS_market, 0, prior_weight * 2)
# -------------------------------------------------------------

compute_raw_weight <- function(iBrierSource, iBrierMarket,
                               iPriorWeight) {
  
  if (is.na(iBrierSource) || is.na(iBrierMarket) ||
      iBrierMarket == 0) {
    return(iPriorWeight)
  }
  
  raw <- 1 - (iBrierSource / iBrierMarket)
  
  # Clip to reasonable range
  # Upper bound: 2x prior (don't over-trust any single source)
  # Lower bound: 0 (never negative weight)
  max(0, min(iPriorWeight * 2, raw))
  
}

# -------------------------------------------------------------
# Helper: normalize weights across all active sources
# Ensures total source weight never exceeds max_total_weight
# -------------------------------------------------------------

normalize_weights <- function(iRawWeights,
                              iMaxTotalWeight = 0.50) {
  
  total <- sum(iRawWeights)
  
  if (total <= iMaxTotalWeight) return(iRawWeights)
  
  iRawWeights * (iMaxTotalWeight / total)
  
}

# -------------------------------------------------------------
# Main: update confidence weights for all active sources
# -------------------------------------------------------------

update_confidence_weights <- function(iDebug = FALSE) {
  
  cat("=== Updating Confidence Weights ===\n\n")
  
  if (iDebug) {
    cat("[DEBUG MODE] No database writes will occur\n\n")
  }
  
  config         <- yaml::read_yaml(here("config", "config.yml"))
  decay          <- config$bayesian$brier_decay
  min_sample     <- config$bayesian$min_sample_brier
  max_total_wt   <- config$bayesian$max_weight
  effective_date <- as.character(Sys.Date())
  
  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))
  
  # Get active sources
  sources <- dbGetQuery(con, "
    SELECT source_id, source_name,
           weight_method, prior_weight
    FROM probability_sources
    WHERE active = 1
    ORDER BY source_id
  ")
  
  if (nrow(sources) == 0) {
    cat("No active probability sources found\n")
    return(invisible(NULL))
  }
  
  cat("Active sources:", nrow(sources), "\n\n")
  
  # Compute market Brier score once
  market_brier <- compute_market_brier(con, decay)
  cat(sprintf("Market Brier score: %.6f (n=%d)\n\n",
              ifelse(is.na(market_brier$brier_score), 0,
                     market_brier$brier_score),
              market_brier$n_obs))
  
  raw_weights <- numeric(nrow(sources))
  brier_scores <- list()
  
  for (ii in seq_len(nrow(sources))) {
    
    source_id   <- sources$source_id[ii]
    source_name <- sources$source_name[ii]
    method      <- sources$weight_method[ii]
    prior_wt    <- sources$prior_weight[ii]
    
    cat(sprintf("--- %s ---\n", source_name))
    
    if (method == "brier") {
      
      src_brier <- compute_source_brier(con, source_id, decay)
      brier_scores[[ii]] <- src_brier
      
      if (src_brier$n_obs < min_sample) {
        
        # Insufficient data — use prior weight
        raw_weights[ii] <- prior_wt
        cat(sprintf(
          "  Using prior weight: %.3f\n",
          prior_wt
        ))
        cat(sprintf(
          "  (only %d observations, need %d)\n\n",
          src_brier$n_obs, min_sample
        ))
        
      } else {
        
        # Sufficient data — compute empirical weight
        raw_wt <- compute_raw_weight(
          src_brier$brier_score,
          market_brier$brier_score,
          prior_wt
        )
        raw_weights[ii] <- raw_wt
        cat(sprintf(
          "  Source Brier: %.6f\n",
          src_brier$brier_score
        ))
        cat(sprintf(
          "  Market Brier: %.6f\n",
          market_brier$brier_score
        ))
        cat(sprintf(
          "  Raw weight:   %.4f\n\n",
          raw_wt
        ))
        
      }
      
    } else {
      
      # Fixed prior weight method
      raw_weights[ii] <- prior_wt
      brier_scores[[ii]] <- list(brier_score = NA_real_,
                                  n_obs = 0L)
      cat(sprintf(
        "  Fixed prior weight: %.3f\n\n", prior_wt
      ))
      
    }
    
  }
  
  # Normalize weights across all sources
  normalized_weights <- normalize_weights(raw_weights,
                                          max_total_wt)
  
  cat("=== Weight Summary ===\n")
  cat(sprintf("%-20s  %-8s  %-8s\n",
              "Source", "Raw", "Normalized"))
  cat(rep("-", 42), "\n", sep = "")
  
  for (ii in seq_len(nrow(sources))) {
    cat(sprintf("%-20s  %.4f    %.4f\n",
                sources$source_name[ii],
                raw_weights[ii],
                normalized_weights[ii]))
  }
  
  cat(sprintf("\nTotal source weight: %.4f\n",
              sum(normalized_weights)))
  cat(sprintf("Market weight:       %.4f\n\n",
              1 - sum(normalized_weights)))
  
  # Write to confidence_weights table
  if (!iDebug) {
    
    # Remove existing weights for today before inserting fresh ones
    dbExecute(con, "
    DELETE FROM confidence_weights
    WHERE effective_date = ?
    ", params = list(effective_date))
    
    for (ii in seq_len(nrow(sources))) {
      
      src_brier <- brier_scores[[ii]]
      
      dbExecute(con, "
        INSERT INTO confidence_weights
        (source_id, segment, weight_value,
         brier_source, brier_market,
         effective_date, sample_size)
        VALUES (?, 'MLB', ?, ?, ?, ?, ?)
      ", params = list(
        sources$source_id[ii],
        normalized_weights[ii],
        src_brier$brier_score,
        market_brier$brier_score,
        effective_date,
        src_brier$n_obs
      ))
      
    }
    
    cat("Weights written to confidence_weights table\n")
    
  } else {
    cat("[DEBUG MODE] No data written to database\n")
  }
  
  # Return named weight vector for use by other functions
  weight_vector <- setNames(normalized_weights,
                            sources$source_name)
  invisible(weight_vector)
  
}

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (sys.nframe() == 0) {
  update_confidence_weights(iDebug = FALSE)
}

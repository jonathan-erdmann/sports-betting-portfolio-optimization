# =============================================================
# get_posterior_probability.R
# Purpose:  Computes Bayesian posterior win probabilities by
#           blending multiple source model probabilities with
#           market fair probabilities, weighted by empirical
#           confidence weights derived from Brier scoring.
#           Uses true weighted blend across all active sources:
#             p = Σ(wₛ * pₛ) + w_market * p_market
#           where Σwₛ <= 0.50 (market always >= 50% weight).
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

  if (nrow(records) == 0) {
    return(list(brier_score = NA_real_, n_obs = 0))
  }

  nn      <- nrow(records)
  weights <- iDecay ^ (nn - seq_len(nn))
  weights <- weights / sum(weights)
  brier   <- sum(weights * (records$predicted - records$outcome)^2)

  list(brier_score = brier, n_obs = nn)
}

# -------------------------------------------------------------
# Helper: compute market Brier score
# Uses consensus fair probability as the market prediction
# -------------------------------------------------------------

compute_market_brier_score <- function(iCon, iDecay = 0.95) {

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
    JOIN games g    ON os.game_id   = g.game_id
    JOIN outcomes o ON os.game_id   = o.game_id
    JOIN teams t    ON o.team_id    = t.team_id
    WHERE os.snapshot_id IN (
      SELECT MAX(snapshot_id)
      FROM odds_snapshots
      GROUP BY game_id, bookmaker_id
    )
    ORDER BY o.recorded_timestamp ASC
  ")

  if (nrow(records) == 0) return(NA_real_)

  records$implied_home <- ml_to_implied(records$home_moneyline)
  records$implied_away <- ml_to_implied(records$away_moneyline)
  records$overround    <- records$implied_home + records$implied_away
  records$fair_home    <- records$implied_home / records$overround
  records$fair_away    <- records$implied_away / records$overround

  records$predicted <- ifelse(
    records$side == "home",
    records$fair_home,
    records$fair_away
  )

  agg <- aggregate(
    cbind(predicted, win) ~ game_id + team_id + recorded_timestamp,
    data = records,
    FUN  = mean
  )
  agg <- agg[order(agg$recorded_timestamp), ]

  nn      <- nrow(agg)
  weights <- iDecay ^ (nn - seq_len(nn))
  weights <- weights / sum(weights)
  brier   <- sum(weights * (agg$predicted - agg$win)^2)

  list(brier_score = brier, n_obs = nn)
}

# -------------------------------------------------------------
# Helper: get or compute confidence weight for a source
# Uses empirical Brier scores if sufficient data exists,
# falls back to prior weight otherwise.
# Called by compute_posterior_probabilities for diagnostics.
# -------------------------------------------------------------

get_confidence_weight <- function(iCon, iSourceId, iConfig) {

  prior_weight <- iConfig$bayesian$prior_weight
  min_weight   <- iConfig$bayesian$min_weight
  max_weight   <- iConfig$bayesian$max_weight
  decay        <- iConfig$bayesian$brier_decay
  min_sample   <- iConfig$bayesian$min_sample_brier

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

  raw_weight <- 1 - (source_brier$brier_score /
                       market_brier$brier_score)
  weight     <- max(min_weight, min(max_weight, raw_weight))

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

# =============================================================
# MULTI-SOURCE BLEND HELPERS
# =============================================================

# -------------------------------------------------------------
# Helper: get normalized confidence weights for active sources
#
# Looks up the most recent confidence_weights record per source.
# Falls back to probability_sources.prior_weight when no record
# exists. Normalizes so Σwₛ <= 0.50 (market always >= 50%).
#
# Returns named numeric vector: source_id (char) -> weight
# -------------------------------------------------------------

get_source_weights <- function(iCon, iActiveSourceIds) {

  if (length(iActiveSourceIds) == 0) return(numeric(0))

  raw_weights <- setNames(
    numeric(length(iActiveSourceIds)),
    as.character(iActiveSourceIds)
  )

  for (ii in seq_along(iActiveSourceIds)) {

    src_id <- iActiveSourceIds[ii]
    key    <- as.character(src_id)

    # Try most recent confidence_weights record (segment = MLB)
    cw <- dbGetQuery(iCon, "
      SELECT weight_value
      FROM confidence_weights
      WHERE source_id = ?
        AND segment   = 'MLB'
      ORDER BY effective_date DESC
      LIMIT 1
    ", params = list(src_id))

    if (nrow(cw) > 0 && !is.na(cw$weight_value[1])) {
      raw_weights[key] <- cw$weight_value[1]
    } else {
      # Fallback: prior_weight from probability_sources
      pw <- dbGetQuery(iCon, "
        SELECT prior_weight, source_name
        FROM probability_sources
        WHERE source_id = ?
      ", params = list(src_id))

      prior <- if (nrow(pw) > 0 && !is.na(pw$prior_weight[1]))
        pw$prior_weight[1] else 0.20

      raw_weights[key] <- prior

      cat(sprintf(
        "  Using prior weight %.2f for %s (no confidence_weight record)\n",
        prior,
        if (nrow(pw) > 0) pw$source_name[1] else key
      ))
    }
  }

  # Normalize so sum of source weights <= 0.50
  total <- sum(raw_weights)
  if (total > 0.50) {
    raw_weights <- raw_weights * (0.50 / total)
  }

  raw_weights
}

# -------------------------------------------------------------
# Helper: get pre-game win probability for one source
#
# Fetches the latest probability_snapshot that was recorded
# before game_time. If game_time is NULL/NA, uses the most
# recent snapshot regardless of timing.
#
# Returns single win_probability or NA_real_ if no snapshot.
# -------------------------------------------------------------

get_source_probability <- function(iCon, iGameId, iTeamId,
                                    iSourceId, iGameTime) {

  if (is.na(iGameTime) || is.null(iGameTime)) {
    snap <- dbGetQuery(iCon, "
      SELECT win_probability
      FROM probability_snapshots
      WHERE game_id   = ?
        AND team_id   = ?
        AND source_id = ?
      ORDER BY scrape_timestamp DESC
      LIMIT 1
    ", params = list(iGameId, iTeamId, iSourceId))
  } else {
    snap <- dbGetQuery(iCon, "
      SELECT ps.win_probability
      FROM probability_snapshots ps
      JOIN games g ON ps.game_id = g.game_id
      WHERE ps.game_id   = ?
        AND ps.team_id   = ?
        AND ps.source_id = ?
        AND ps.scrape_timestamp < g.game_time
      ORDER BY ps.scrape_timestamp DESC
      LIMIT 1
    ", params = list(iGameId, iTeamId, iSourceId))
  }

  if (nrow(snap) == 0) return(NA_real_)
  snap$win_probability[1]
}

# -------------------------------------------------------------
# Helper: compute weighted blend of source and market probs
#
# iSourceProbs:   named numeric vector (source_id -> probability)
# iSourceWeights: named numeric vector (source_id -> weight)
# iMarketFairProb: market fair probability (scalar)
#
# Excludes sources with NA probability.
# Re-normalizes remaining weights to respect the <= 0.50 cap.
# Returns blended posterior probability.
# -------------------------------------------------------------

blend_probabilities <- function(iSourceProbs, iSourceWeights,
                                 iMarketFairProb) {

  # Intersect names — only blend sources present in both
  common <- intersect(names(iSourceProbs), names(iSourceWeights))
  if (length(common) == 0) return(iMarketFairProb)

  # Keep only sources with non-NA probabilities
  valid <- common[!is.na(iSourceProbs[common])]
  if (length(valid) == 0) return(iMarketFairProb)

  # Re-normalize available source weights to <= 0.50
  avail_weights <- iSourceWeights[valid]
  total_w       <- sum(avail_weights)

  if (total_w > 0.50) {
    avail_weights <- avail_weights * (0.50 / total_w)
  }

  source_contrib <- sum(avail_weights * iSourceProbs[valid])
  market_weight  <- 1 - sum(avail_weights)

  source_contrib + market_weight * iMarketFairProb
}

# -------------------------------------------------------------
# Helper: compute posterior probability for one game/team
#
# Blends all active sources with pre-game snapshots using
# normalized confidence weights. Falls back to iMarketFairProb
# if no sources have snapshots for this game.
#
# iDebug = TRUE prints the full blend breakdown per call.
# Returns single numeric probability in [0, 1].
# -------------------------------------------------------------

get_posterior <- function(iCon, iGameId, iTeamId,
                           iMarketFairProb, iDebug = FALSE) {

  # All active probability sources
  sources <- dbGetQuery(iCon, "
    SELECT source_id, source_name
    FROM probability_sources
    WHERE active = 1
    ORDER BY source_id
  ")

  if (nrow(sources) == 0) return(iMarketFairProb)

  # Normalized weights (Σwₛ <= 0.50)
  src_weights <- get_source_weights(iCon, sources$source_id)

  # Game time for pre-game filter
  gt_row    <- dbGetQuery(iCon, "
    SELECT game_time FROM games WHERE game_id = ?
  ", params = list(iGameId))
  game_time <- if (nrow(gt_row) > 0) gt_row$game_time[1] else NA

  # Fetch pre-game probability from each source
  src_probs <- setNames(
    rep(NA_real_, nrow(sources)),
    as.character(sources$source_id)
  )

  for (ii in seq_len(nrow(sources))) {
    sid <- sources$source_id[ii]
    src_probs[as.character(sid)] <- get_source_probability(
      iCon, iGameId, iTeamId, sid, game_time
    )
  }

  posterior <- blend_probabilities(src_probs, src_weights,
                                    iMarketFairProb)

  if (iDebug) {
    valid_ids <- names(src_probs)[!is.na(src_probs)]
    avail_w   <- if (length(valid_ids) > 0) {
      ww <- src_weights[valid_ids]
      if (sum(ww) > 0.50) ww <- ww * (0.50 / sum(ww))
      ww
    } else numeric(0)

    parts <- vapply(valid_ids, function(sid) {
      sname <- sources$source_name[sources$source_id == as.integer(sid)]
      sprintf("%s(w=%.3f p=%.3f)",
              substr(sname, 1, 7),
              avail_w[sid],
              src_probs[sid])
    }, character(1))

    mkt_w <- max(0, 1 - sum(avail_w))
    cat(sprintf(
      "    [Blend] %s Market(w=%.3f p=%.3f) -> post=%.4f\n",
      if (length(parts) > 0) paste(parts, collapse = " ") else "(none)",
      mkt_w, iMarketFairProb, posterior
    ))
  }

  posterior
}

# =============================================================
# Main: compute posterior probabilities for all games on date
# (standalone diagnostic runner)
# =============================================================

compute_posterior_probabilities <- function(iDate  = Sys.Date(),
                                            iDebug = FALSE) {

  cat("=== Computing Posterior Probabilities ===\n")
  cat("Date:", as.character(iDate), "\n\n")

  config <- yaml::read_yaml(here("config", "config.yml"))

  db_path <- here("db", "betting.sqlite")
  con     <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))

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

  # Active sources and their weights
  sources <- dbGetQuery(con, "
    SELECT source_id, source_name
    FROM probability_sources
    WHERE active = 1
    ORDER BY source_id
  ")

  cat("=== Source Confidence Weights ===\n")
  src_weights <- get_source_weights(con, sources$source_id)
  for (ii in seq_len(nrow(sources))) {
    sid <- as.character(sources$source_id[ii])
    cat(sprintf("  %-20s  %.4f\n",
                sources$source_name[ii],
                src_weights[sid]))
  }
  cat("\n")

  closing_window <- config$feature_engineering$closing_window_hours

  cat(sprintf("%-30s  %-8s  %-8s  %-8s  %s\n",
              "Game", "Mkt%", "Post%", "Edge", "Sources"))
  cat(rep("-", 80), "\n", sep = "")

  for (ii in seq_len(nrow(games))) {

    game_id      <- games$game_id[ii]
    home_team_id <- games$home_team_id[ii]
    home_team    <- games$home_team[ii]
    away_team    <- games$away_team[ii]

    market <- get_consensus_fair_prob(
      con, game_id, closing_window
    )
    if (is.null(market)) next

    post_home <- get_posterior(
      con, game_id, home_team_id,
      market$consensus_home, iDebug
    )

    edge <- post_home - market$consensus_home
    display <- paste0(
      substr(away_team, 1, 10), "@",
      substr(home_team, 1, 10)
    )

    cat(sprintf("%-30s  %5.1f%%  %5.1f%%  %+.1f%%\n",
                display,
                market$consensus_home * 100,
                post_home * 100,
                edge * 100))
  }

  cat("\n")
  invisible(NULL)
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

# =============================================================
# optimize_portfolio.R
# Purpose:  Two-stage constrained Kelly portfolio optimizer.
#           Stage 1 (Kelly sizing) is computed upstream in
#           get_daily_opportunities.R. Stage 2 enforces
#           portfolio constraints with capital reallocation.
# Layer:    4 - Strategy
# Author:   Jonathan Erdmann
# =============================================================

library(yaml)
library(here)

# -------------------------------------------------------------
# Main: apply portfolio constraints with reallocation
#
# iOpportunities  data frame with columns:
#                   game_id, team_id, kelly_fractional,
#                   expected_value, moneyline,
#                   implied_prob_fair, posterior_probability
# iConfig         list from yaml::read_yaml(config.yml)
# iDebug          if TRUE, prints step-by-step actions;
#                 returns iOpportunities unchanged (no execution)
# -------------------------------------------------------------

optimize_portfolio <- function(iOpportunities, iConfig,
                               iDebug = FALSE) {

  daily_cap      <- iConfig$feature_engineering$daily_exposure_cap
  game_cap       <- iConfig$feature_engineering$per_game_exposure_cap
  same_game_cap  <- iConfig$feature_engineering$max_same_game_exposure
  max_single_bet <- iConfig$feature_engineering$max_single_bet

  convergence_tolerance <- 1e-6
  max_iterations        <- 10

  if (nrow(iOpportunities) == 0) return(iOpportunities)

  opp        <- iOpportunities
  opp$scaled <- FALSE

  game_ids <- unique(opp$game_id)

  # ------------------------------------------------------------
  # Step 1: Cap individual bets at max_single_bet
  # ------------------------------------------------------------

  for (ii in seq_len(nrow(opp))) {
    if (opp$kelly_fractional[ii] > max_single_bet) {
      if (iDebug) cat(sprintf(
        "  [Step 1] Cap row %d (game %s): %.6f -> %.6f\n",
        ii, opp$game_id[ii],
        opp$kelly_fractional[ii], max_single_bet
      ))
      opp$kelly_fractional[ii] <- max_single_bet
      opp$scaled[ii]           <- TRUE
    }
  }

  # ------------------------------------------------------------
  # Step 2: Cap same-game exposure when betting both sides
  # ------------------------------------------------------------

  for (gg in game_ids) {
    game_rows <- which(opp$game_id == gg)
    if (length(game_rows) == 2) {
      same_exp <- sum(opp$kelly_fractional[game_rows])
      if (same_exp > same_game_cap) {
        scale <- same_game_cap / same_exp
        if (iDebug) cat(sprintf(
          "  [Step 2] Scale same-game %s: %.6f -> %.6f\n",
          gg, same_exp, same_game_cap
        ))
        opp$kelly_fractional[game_rows] <-
          opp$kelly_fractional[game_rows] * scale
        opp$scaled[game_rows] <- TRUE
      }
    }
  }

  # ------------------------------------------------------------
  # Step 3: Cap per-game exposure
  # ------------------------------------------------------------

  for (gg in game_ids) {
    game_rows <- which(opp$game_id == gg)
    game_exp  <- sum(opp$kelly_fractional[game_rows])
    if (game_exp > game_cap) {
      scale <- game_cap / game_exp
      if (iDebug) cat(sprintf(
        "  [Step 3] Scale per-game %s: %.6f -> %.6f\n",
        gg, game_exp, game_cap
      ))
      opp$kelly_fractional[game_rows] <-
        opp$kelly_fractional[game_rows] * scale
      opp$scaled[game_rows] <- TRUE
    }
  }

  # Total clipped by Steps 1-3 (available for reallocation)
  total_clipped      <- sum(iOpportunities$kelly_fractional) -
    sum(opp$kelly_fractional)
  reallocation_amount <- total_clipped

  # ------------------------------------------------------------
  # Step 4: Cap daily total exposure
  # ------------------------------------------------------------

  total_exposure <- sum(opp$kelly_fractional)
  if (total_exposure > daily_cap) {
    scale <- daily_cap / total_exposure
    if (iDebug) cat(sprintf(
      "  [Step 4] Scale daily total: %.6f -> %.6f\n",
      total_exposure, daily_cap
    ))
    opp$kelly_fractional <- opp$kelly_fractional * scale
    opp$scaled           <- rep(TRUE, nrow(opp))
  }

  # ------------------------------------------------------------
  # Step 5: Reallocation with convergence check
  # ------------------------------------------------------------

  n_iterations <- 0
  converged    <- FALSE

  if (total_clipped > convergence_tolerance) {

    prev_fractions <- rep(0, nrow(opp))

    for (ii in seq_len(max_iterations)) {

      n_iterations <- ii

      # Bets with room to absorb more capital
      uncapped <- opp$kelly_fractional <
        (max_single_bet - convergence_tolerance)

      if (!any(uncapped) || total_clipped < convergence_tolerance) {
        converged <- TRUE
        break
      }

      # Redistribute proportionally by expected_value
      ev_weights <- opp$expected_value[uncapped]
      ev_weights <- ev_weights / sum(ev_weights)

      if (iDebug) cat(sprintf(
        "  [Step 5 iter %d] Reallocating %.6f to %d uncapped bets\n",
        ii, total_clipped, sum(uncapped)
      ))

      opp$kelly_fractional[uncapped] <-
        opp$kelly_fractional[uncapped] +
        total_clipped * ev_weights

      total_clipped <- 0

      # Re-check Step 1
      for (jj in which(uncapped)) {
        if (opp$kelly_fractional[jj] > max_single_bet) {
          excess                    <- opp$kelly_fractional[jj] -
            max_single_bet
          opp$kelly_fractional[jj] <- max_single_bet
          opp$scaled[jj]           <- TRUE
          total_clipped            <- total_clipped + excess
        }
      }

      # Re-check Step 2
      for (gg in game_ids) {
        game_rows <- which(opp$game_id == gg)
        if (length(game_rows) == 2) {
          same_exp <- sum(opp$kelly_fractional[game_rows])
          if (same_exp > same_game_cap) {
            excess <- same_exp - same_game_cap
            scale  <- same_game_cap / same_exp
            opp$kelly_fractional[game_rows] <-
              opp$kelly_fractional[game_rows] * scale
            opp$scaled[game_rows] <- TRUE
            total_clipped         <- total_clipped + excess
          }
        }
      }

      # Re-check Step 3
      for (gg in game_ids) {
        game_rows <- which(opp$game_id == gg)
        game_exp  <- sum(opp$kelly_fractional[game_rows])
        if (game_exp > game_cap) {
          excess <- game_exp - game_cap
          scale  <- game_cap / game_exp
          opp$kelly_fractional[game_rows] <-
            opp$kelly_fractional[game_rows] * scale
          opp$scaled[game_rows] <- TRUE
          total_clipped         <- total_clipped + excess
        }
      }

      # Re-check Step 4 — stop reallocation if daily cap hit
      total_exposure <- sum(opp$kelly_fractional)
      if (total_exposure > daily_cap) {
        scale                <- daily_cap / total_exposure
        opp$kelly_fractional <- opp$kelly_fractional * scale
        opp$scaled           <- rep(TRUE, nrow(opp))
        converged            <- TRUE
        if (iDebug) cat("  [Step 5] Daily cap hit — stopping reallocation\n")
        break
      }

      # Convergence check
      max_change <- max(abs(opp$kelly_fractional - prev_fractions))
      if (max_change < convergence_tolerance) {
        converged <- TRUE
        break
      }
      if (total_clipped < convergence_tolerance) {
        converged <- TRUE
        break
      }

      prev_fractions <- opp$kelly_fractional

    }

    if (!converged) {
      warning(sprintf(paste0(
        "Portfolio optimization did not converge after ",
        "%d iterations. Using last iteration."
      ), max_iterations))
    }

  }

  # ------------------------------------------------------------
  # Summary
  # ------------------------------------------------------------

  n_bets    <- nrow(opp)
  n_scaled  <- sum(opp$scaled)
  total_exp <- sum(opp$kelly_fractional)

  cat("\n=== Portfolio Optimization Summary ===\n")
  cat(sprintf("  total_exposure:      %.4f (%.2f%%)\n",
              total_exp, total_exp * 100))
  cat(sprintf("  n_bets:              %d\n", n_bets))
  cat(sprintf("  n_scaled:            %d\n", n_scaled))
  cat(sprintf("  reallocation_amount: %.6f (%.4f%%)\n",
              reallocation_amount, reallocation_amount * 100))
  cat(sprintf("  iterations:          %d\n", n_iterations))

  if (iDebug) {
    cat("\n[DEBUG MODE] Returning original opportunities unchanged\n")
    return(iOpportunities)
  }

  opp

}

# -------------------------------------------------------------
# Run test suite if executed directly
# -------------------------------------------------------------

if (sys.nframe() == 0) {

  config <- yaml::read_yaml(here("config", "config.yml"))

  cat("=== optimize_portfolio.R Test Suite ===\n\n")

  # ----------------------------------------------------------
  # Case 1: Single bet under all caps — no scaling
  # ----------------------------------------------------------

  cat("--- Case 1: Single bet under all caps ---\n")

  opp1 <- data.frame(
    game_id               = "G1",
    team_id               = 1L,
    kelly_fractional      = 0.02,
    expected_value        = 0.05,
    moneyline             = -110L,
    implied_prob_fair     = 0.52,
    posterior_probability = 0.55,
    scaled                = FALSE,
    stringsAsFactors      = FALSE
  )

  result1 <- optimize_portfolio(opp1, config)
  stopifnot(abs(result1$kelly_fractional - 0.02) < 1e-9)
  stopifnot(!result1$scaled)
  cat("PASS: kelly_fractional unchanged (0.0200), scaled = FALSE\n\n")

  # ----------------------------------------------------------
  # Case 2: Single bet over max_single_bet — capped, no
  #         reallocation possible (only one bet)
  # ----------------------------------------------------------

  cat("--- Case 2: Single bet over max_single_bet ---\n")

  opp2 <- data.frame(
    game_id               = "G1",
    team_id               = 1L,
    kelly_fractional      = 0.04,
    expected_value        = 0.08,
    moneyline             = -110L,
    implied_prob_fair     = 0.52,
    posterior_probability = 0.60,
    scaled                = FALSE,
    stringsAsFactors      = FALSE
  )

  result2 <- optimize_portfolio(opp2, config)
  stopifnot(abs(result2$kelly_fractional -
                  config$feature_engineering$max_single_bet) < 1e-9)
  stopifnot(result2$scaled)
  cat(sprintf("PASS: kelly_fractional capped at %.4f, scaled = TRUE\n\n",
              config$feature_engineering$max_single_bet))

  # ----------------------------------------------------------
  # Case 3: Both sides of same game — same-game cap applied
  # ----------------------------------------------------------

  cat("--- Case 3: Both sides of same game ---\n")

  opp3 <- data.frame(
    game_id               = c("G1", "G1"),
    team_id               = c(1L, 2L),
    kelly_fractional      = c(0.018, 0.018),
    expected_value        = c(0.04, 0.03),
    moneyline             = c(-110L, 110L),
    implied_prob_fair     = c(0.52, 0.48),
    posterior_probability = c(0.55, 0.50),
    scaled                = c(FALSE, FALSE),
    stringsAsFactors      = FALSE
  )

  result3 <- optimize_portfolio(opp3, config)
  same_game_cap <- config$feature_engineering$max_same_game_exposure
  stopifnot(sum(result3$kelly_fractional) <=
              same_game_cap + 1e-9)
  stopifnot(all(result3$scaled))
  cat(sprintf(
    "PASS: sum(kelly_fractional) = %.4f <= max_same_game_exposure = %.4f\n\n",
    sum(result3$kelly_fractional), same_game_cap
  ))

  # ----------------------------------------------------------
  # Case 4: Multiple bets over daily_exposure_cap — all scaled
  #         proportionally, verify sum == daily_exposure_cap
  # ----------------------------------------------------------

  cat("--- Case 4: Multiple bets over daily_exposure_cap ---\n")

  opp4 <- data.frame(
    game_id               = paste0("G", 1:5),
    team_id               = 1L:5L,
    kelly_fractional      = rep(0.025, 5),
    expected_value        = seq(0.05, 0.09, by = 0.01),
    moneyline             = rep(-110L, 5),
    implied_prob_fair     = rep(0.52, 5),
    posterior_probability = rep(0.57, 5),
    scaled                = rep(FALSE, 5),
    stringsAsFactors      = FALSE
  )

  result4 <- optimize_portfolio(opp4, config)
  daily_cap <- config$feature_engineering$daily_exposure_cap
  stopifnot(abs(sum(result4$kelly_fractional) - daily_cap) < 1e-9)
  stopifnot(all(result4$scaled))
  cat(sprintf(
    "PASS: sum = %.4f == daily_exposure_cap = %.4f, all scaled\n\n",
    sum(result4$kelly_fractional), daily_cap
  ))

  # ----------------------------------------------------------
  # Case 5: One bet capped, excess reallocated to second bet;
  #         verify reallocation does not exceed max_single_bet
  # ----------------------------------------------------------

  cat("--- Case 5: Reallocation without exceeding max_single_bet ---\n")

  # G1: 0.04 → capped to 0.03, excess 0.01 reallocated to G2
  # G2: 0.015 + 0.01 = 0.025 (still < 0.03)
  opp5 <- data.frame(
    game_id               = c("G1", "G2"),
    team_id               = c(1L, 2L),
    kelly_fractional      = c(0.04, 0.015),
    expected_value        = c(0.08, 0.05),
    moneyline             = c(-110L, 120L),
    implied_prob_fair     = c(0.52, 0.45),
    posterior_probability = c(0.60, 0.52),
    scaled                = c(FALSE, FALSE),
    stringsAsFactors      = FALSE
  )

  result5 <- optimize_portfolio(opp5, config)
  max_single <- config$feature_engineering$max_single_bet
  stopifnot(result5$kelly_fractional[result5$game_id == "G1"] <=
              max_single + 1e-9)
  stopifnot(result5$kelly_fractional[result5$game_id == "G2"] <=
              max_single + 1e-9)
  stopifnot(abs(
    result5$kelly_fractional[result5$game_id == "G2"] - 0.025
  ) < 1e-9)
  cat(sprintf(
    "PASS: G1 = %.4f (capped at %.4f), G2 = %.4f (reallocated, <= %.4f)\n\n",
    result5$kelly_fractional[result5$game_id == "G1"], max_single,
    result5$kelly_fractional[result5$game_id == "G2"], max_single
  ))

  # ----------------------------------------------------------
  # Case 6: Realistic 5-opportunity portfolio converges
  #         in < 10 iterations
  # ----------------------------------------------------------

  cat("--- Case 6: Realistic 5-opportunity portfolio convergence ---\n")

  # G1: over max_single_bet (excess reallocated to G2-G5)
  # G2-G5: separate games with room below the individual cap
  opp6 <- data.frame(
    game_id               = c("G1", "G2", "G3", "G4", "G5"),
    team_id               = 1L:5L,
    kelly_fractional      = c(0.035, 0.012, 0.010, 0.015, 0.014),
    expected_value        = c(0.09, 0.06, 0.05, 0.04, 0.07),
    moneyline             = c(-110L, -105L, 120L, -115L, 130L),
    implied_prob_fair     = c(0.52, 0.51, 0.45, 0.53, 0.44),
    posterior_probability = c(0.60, 0.57, 0.52, 0.57, 0.54),
    scaled                = rep(FALSE, 5),
    stringsAsFactors      = FALSE
  )

  result6 <- optimize_portfolio(opp6, config)

  # All constraints satisfied
  stopifnot(all(
    result6$kelly_fractional <=
      config$feature_engineering$max_single_bet + 1e-9
  ))
  stopifnot(
    sum(result6$kelly_fractional) <=
      config$feature_engineering$daily_exposure_cap + 1e-9
  )
  for (gg in unique(opp6$game_id)) {
    rows     <- result6$game_id == gg
    game_exp <- sum(result6$kelly_fractional[rows])
    stopifnot(game_exp <=
                config$feature_engineering$per_game_exposure_cap + 1e-9)
    if (sum(rows) == 2) {
      stopifnot(game_exp <=
                  config$feature_engineering$max_same_game_exposure + 1e-9)
    }
  }
  cat(sprintf(
    "PASS: All constraints satisfied. Total exposure = %.4f (%.2f%%)\n\n",
    sum(result6$kelly_fractional),
    sum(result6$kelly_fractional) * 100
  ))

  cat("=== All 6 test cases passed ===\n")

}

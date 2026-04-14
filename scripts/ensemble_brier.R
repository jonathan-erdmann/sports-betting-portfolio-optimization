# =============================================================
# ensemble_brier.R
# Computes ensemble (posterior) Brier score across all resolved
# games — one observation per game (home team perspective)
# Run from project root: Rscript scripts/ensemble_brier.R
# =============================================================
library(DBI); library(RSQLite); library(here); library(yaml)
con    <- dbConnect(RSQLite::SQLite(), here("db", "betting.sqlite"))
config <- yaml::read_yaml(here("config", "config.yml"))
on.exit(dbDisconnect(con))

cat("=== Ensemble Brier Score — All Games ===\n")
cat("(One observation per game, home team perspective)\n\n")

games <- dbGetQuery(con, "
  SELECT g.game_id, g.game_date, g.game_time,
         g.home_team_id, g.away_team_id,
         o.win AS home_win
  FROM games g
  JOIN outcomes o ON g.game_id = o.game_id
                 AND o.team_id = g.home_team_id
  WHERE g.status = 'final'
  ORDER BY g.game_date
")

cat(sprintf("Total resolved games: %d\n\n", nrow(games)))

weights <- dbGetQuery(con, "
  SELECT cw.source_id, src.source_name,
         cw.weight_value, src.prior_weight
  FROM confidence_weights cw
  JOIN probability_sources src ON cw.source_id = src.source_id
  WHERE cw.effective_date = (
    SELECT MAX(effective_date) FROM confidence_weights cw2
    WHERE cw2.source_id = cw.source_id
  )
  AND src.active = 1
")

cat("Current source weights:\n")
for (ii in seq_len(nrow(weights))) {
  cat(sprintf("  %-20s  %.4f\n",
              weights$source_name[ii],
              weights$weight_value[ii]))
}
total_src_weight <- sum(weights$weight_value)
mkt_weight       <- 1 - total_src_weight
cat(sprintf("  %-20s  %.4f\n\n", "Market", mkt_weight))

results <- data.frame()

for (ii in seq_len(nrow(games))) {
  gg <- games[ii, ]

  mkt <- dbGetQuery(con, "
    SELECT AVG(
      (CASE WHEN os.home_moneyline < 0
        THEN CAST(-os.home_moneyline AS REAL)/(-os.home_moneyline+100)
        ELSE 100.0/(os.home_moneyline+100) END) /
      ((CASE WHEN os.home_moneyline < 0
        THEN CAST(-os.home_moneyline AS REAL)/(-os.home_moneyline+100)
        ELSE 100.0/(os.home_moneyline+100) END) +
       (CASE WHEN os.away_moneyline < 0
        THEN CAST(-os.away_moneyline AS REAL)/(-os.away_moneyline+100)
        ELSE 100.0/(os.away_moneyline+100) END))
    ) AS fair_home
    FROM odds_snapshots os
    JOIN games g ON os.game_id = g.game_id
    WHERE os.game_id = ?
      AND os.scrape_timestamp < g.game_time
      AND os.scrape_timestamp = (
        SELECT MAX(scrape_timestamp) FROM odds_snapshots os2
        WHERE os2.game_id      = os.game_id
          AND os2.bookmaker_id = os.bookmaker_id
          AND os2.scrape_timestamp < g.game_time
      )
  ", params = list(gg$game_id))

  if (nrow(mkt) == 0 || is.na(mkt$fair_home[1])) next
  mkt_prob <- mkt$fair_home[1]

  src_probs <- dbGetQuery(con, "
    SELECT ps.source_id, ps.win_probability
    FROM probability_snapshots ps
    JOIN games g ON ps.game_id = g.game_id
    WHERE ps.game_id = ?
      AND ps.team_id = ?
      AND ps.scrape_timestamp < g.game_time
      AND ps.scrape_timestamp = (
        SELECT MAX(scrape_timestamp) FROM probability_snapshots ps2
        WHERE ps2.game_id   = ps.game_id
          AND ps2.source_id = ps.source_id
          AND ps2.team_id   = ps.team_id
          AND ps2.scrape_timestamp < g.game_time
      )
  ", params = list(gg$game_id, gg$home_team_id))

  if (nrow(src_probs) == 0) next

  posterior <- mkt_prob * mkt_weight
  for (jj in seq_len(nrow(src_probs))) {
    sid <- src_probs$source_id[jj]
    ww  <- weights$weight_value[weights$source_id == sid]
    if (length(ww) == 0) next
    posterior <- posterior + ww * src_probs$win_probability[jj]
  }

  results <- rbind(results, data.frame(
    game_date = gg$game_date,
    game_id   = gg$game_id,
    posterior = posterior,
    mkt_prob  = mkt_prob,
    actual    = gg$home_win,
    n_sources = nrow(src_probs)
  ))
}

cat(sprintf("Games with full data: %d\n\n", nrow(results)))

if (nrow(results) > 0) {
  ens_bs <- mean((results$posterior - results$actual)^2)
  mkt_bs <- mean((results$mkt_prob  - results$actual)^2)
  nn     <- nrow(results)
  ens_ci <- 1.96 * sqrt(ens_bs * (1-ens_bs) / nn)
  mkt_ci <- 1.96 * sqrt(mkt_bs * (1-mkt_bs) / nn)
  bss    <- 1 - ens_bs / mkt_bs

  cat(sprintf("%-22s  %.4f  (%.4f — %.4f)  n=%d\n",
              "Market Brier:", mkt_bs,
              mkt_bs-mkt_ci, mkt_bs+mkt_ci, nn))
  cat(sprintf("%-22s  %.4f  (%.4f — %.4f)  n=%d\n",
              "Ensemble Brier:", ens_bs,
              ens_bs-ens_ci, ens_bs+ens_ci, nn))
  cat(sprintf("%-22s  %+.4f\n", "vs Market:", ens_bs - mkt_bs))
  cat(sprintf("%-22s  %.4f\n", "Brier Skill Score:", bss))

  cat("\nBy date:\n")
  cat(sprintf("  %-12s  %-8s  %-8s  %-8s  %-5s\n",
              "Date", "Ensemble", "Market", "Delta", "n"))
  cat(sprintf("  %s\n", paste(rep("-", 50), collapse="")))
  for (dd in sort(unique(results$game_date))) {
    dd_rec <- results[results$game_date == dd, ]
    e_bs   <- mean((dd_rec$posterior - dd_rec$actual)^2)
    m_bs   <- mean((dd_rec$mkt_prob  - dd_rec$actual)^2)
    cat(sprintf("  %-12s  %.4f    %.4f    %+.4f    %d\n",
                dd, e_bs, m_bs, e_bs - m_bs, nrow(dd_rec)))
  }
}

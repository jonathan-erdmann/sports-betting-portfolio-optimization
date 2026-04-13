library(DBI); library(RSQLite); library(here)
library(yaml)

con    <- dbConnect(RSQLite::SQLite(), here("db", "betting.sqlite"))
config <- yaml::read_yaml(here("config", "config.yml"))
decay  <- config$bayesian$brier_decay

sources <- dbGetQuery(con, "
  SELECT source_id, source_name
  FROM probability_sources
  WHERE active = 1
")

brier_ci <- function(bs, n) {
  se <- sqrt(bs * (1 - bs) / n)
  c(lower = bs - 1.96 * se, upper = bs + 1.96 * se)
}

# ---------------------------------------------------------------
# Market Brier
# ---------------------------------------------------------------
market <- dbGetQuery(con, "
  SELECT g.game_date, g.game_id, o.team_id, o.win,
         AVG(CASE
           WHEN o.team_id = g.home_team_id THEN
             (CASE WHEN os.home_moneyline < 0
               THEN CAST(-os.home_moneyline AS REAL) /
                    (-os.home_moneyline + 100)
               ELSE 100.0 / (os.home_moneyline + 100) END) /
             ((CASE WHEN os.home_moneyline < 0
               THEN CAST(-os.home_moneyline AS REAL) /
                    (-os.home_moneyline + 100)
               ELSE 100.0 / (os.home_moneyline + 100) END) +
              (CASE WHEN os.away_moneyline < 0
               THEN CAST(-os.away_moneyline AS REAL) /
                    (-os.away_moneyline + 100)
               ELSE 100.0 / (os.away_moneyline + 100) END))
           ELSE
             (CASE WHEN os.away_moneyline < 0
               THEN CAST(-os.away_moneyline AS REAL) /
                    (-os.away_moneyline + 100)
               ELSE 100.0 / (os.away_moneyline + 100) END) /
             ((CASE WHEN os.home_moneyline < 0
               THEN CAST(-os.home_moneyline AS REAL) /
                    (-os.home_moneyline + 100)
               ELSE 100.0 / (os.home_moneyline + 100) END) +
              (CASE WHEN os.away_moneyline < 0
               THEN CAST(-os.away_moneyline AS REAL) /
                    (-os.away_moneyline + 100)
               ELSE 100.0 / (os.away_moneyline + 100) END))
         END) AS predicted
  FROM outcomes o
  JOIN games g ON o.game_id = g.game_id
  JOIN odds_snapshots os ON o.game_id = os.game_id
    AND os.scrape_timestamp < g.game_time
    AND os.scrape_timestamp = (
      SELECT MAX(scrape_timestamp)
      FROM odds_snapshots os2
      WHERE os2.game_id      = os.game_id
        AND os2.bookmaker_id = os.bookmaker_id
        AND os2.scrape_timestamp < g.game_time
    )
  GROUP BY g.game_id, o.team_id
  ORDER BY g.game_date, g.game_id
")

cat("=== Brier Score Summary by Source ===\n")
cat("(Cumulative weighted score, lower is better)\n\n")
cat(sprintf("%-22s  %7s  %5s  %22s  %s\n",
            "Source", "Brier", "n",
            "95% CI", "vs Market"))
cat(rep("-", 75), "\n", sep = "")

mkt_bs <- NA
if (nrow(market) > 0) {
  nn      <- nrow(market)
  weights <- decay ^ (nn - seq_len(nn))
  weights <- weights / sum(weights)
  mkt_bs  <- sum(weights * (market$predicted - market$win)^2)
  ci      <- brier_ci(mkt_bs, nn)
  cat(sprintf("%-22s  %.4f  %5d  (%.4f — %.4f)  %s\n",
              "Market (consensus)",
              mkt_bs, nn, ci["lower"], ci["upper"], "baseline"))
}

for (ii in seq_len(nrow(sources))) {
  records <- dbGetQuery(con, "
    SELECT g.game_date,
           ps.win_probability AS predicted,
           o.win
    FROM probability_snapshots ps
    JOIN outcomes o ON ps.game_id = o.game_id
                   AND ps.team_id = o.team_id
    JOIN games g    ON ps.game_id = g.game_id
    WHERE ps.source_id = ?
      AND ps.scrape_timestamp < g.game_time
      AND ps.scrape_timestamp = (
        SELECT MAX(scrape_timestamp)
        FROM probability_snapshots ps2
        WHERE ps2.game_id   = ps.game_id
          AND ps2.source_id = ps.source_id
          AND ps2.team_id   = ps.team_id
          AND ps2.scrape_timestamp < g.game_time
      )
    ORDER BY g.game_date
  ", params = list(sources$source_id[ii]))

  if (nrow(records) == 0) {
    cat(sprintf("%-22s  No data\n", sources$source_name[ii]))
    next
  }

  nn      <- nrow(records)
  weights <- decay ^ (nn - seq_len(nn))
  weights <- weights / sum(weights)
  bs      <- sum(weights * (records$predicted - records$win)^2)
  ci      <- brier_ci(bs, nn)
  vs_mkt  <- if (!is.na(mkt_bs)) bs - mkt_bs else NA
  sig     <- if (!is.na(vs_mkt)) {
    if (ci["upper"] < mkt_bs) "** Sig. better"
    else if (bs < mkt_bs)     "Beats market"
    else                       "Below market"
  } else ""

  cat(sprintf("%-22s  %.4f  %5d  (%.4f — %.4f)  %s\n",
              sources$source_name[ii],
              bs, nn, ci["lower"], ci["upper"],
              ifelse(is.na(vs_mkt), "",
                     sprintf("%+.4f  %s", vs_mkt, sig))))
}

cat(rep("-", 75), "\n", sep = "")
cat("\n** = upper bound of 95% CI is below market Brier (statistically significant)\n")
cat(sprintf("Next significance milestone: ~%.0f obs needed per source\n",
            if (!is.na(mkt_bs)) {
              # Approx n for significance at current gap sizes
              gap <- 0.03  # typical gap we're seeing
              (1.96 * sqrt(mkt_bs * (1-mkt_bs)))^2 / gap^2
            } else NA))

dbDisconnect(con)

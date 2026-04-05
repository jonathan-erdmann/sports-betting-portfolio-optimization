# =============================================================
# db_summary.R
# Purpose:  Prints a readable summary of all data collected
#           in the betting database to date.
# Usage:    Rscript scripts/db_summary.R
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(here)

con <- dbConnect(RSQLite::SQLite(), here("db", "betting.sqlite"))

cat("=======================================================\n")
cat("       SPORTS BETTING PORTFOLIO OPTIMIZATION\n")
cat("           Database Summary —", format(Sys.Date()), "\n")
cat("=======================================================\n\n")

cat("--- GAMES REGISTERED --------------------------------\n")
games <- dbGetQuery(con, "
  SELECT g.game_id, g.game_date, g.status,
         th.team_name AS home_team,
         ta.team_name AS away_team
  FROM games g
  JOIN teams th ON g.home_team_id = th.team_id
  JOIN teams ta ON g.away_team_id = ta.team_id
  ORDER BY g.game_date, g.game_id
")
for (ii in seq_len(nrow(games))) {
  cat(sprintf("  %-28s  %s vs %-24s  [%s]\n",
    games$game_id[ii],
    games$away_team[ii],
    games$home_team[ii],
    games$status[ii]
  ))
}

cat("\n--- PROBABILITY SNAPSHOTS ---------------------------\n")
probs <- dbGetQuery(con, "
  SELECT g.game_date, t.team_name,
         ps.win_probability, ps.scrape_timestamp,
         src.source_name
  FROM probability_snapshots ps
  JOIN games g   ON ps.game_id  = g.game_id
  JOIN teams t   ON ps.team_id  = t.team_id
  JOIN probability_sources src
       ON ps.source_id = src.source_id
  ORDER BY g.game_date, g.game_id,
           ps.win_probability DESC
")
if (nrow(probs) == 0) {
  cat("  No probability snapshots recorded\n")
} else {
  current_game <- ""
  for (ii in seq_len(nrow(probs))) {
    game_key <- paste(probs$game_date[ii],
                      probs$source_name[ii])
    if (game_key != current_game) {
      cat(sprintf("\n  %s | %s\n",
        probs$game_date[ii],
        probs$source_name[ii]))
      current_game <- game_key
    }
    cat(sprintf("    %-28s  %.1f%%\n",
      probs$team_name[ii],
      probs$win_probability[ii] * 100
    ))
  }
}

cat("\n--- ODDS SNAPSHOTS ----------------------------------\n")
odds_summary <- dbGetQuery(con, "
  SELECT g.game_id,
         th.team_name AS home_team,
         ta.team_name AS away_team,
         COUNT(DISTINCT os.bookmaker_id) AS bookmakers,
         MIN(os.home_moneyline) AS home_ml_min,
         MAX(os.home_moneyline) AS home_ml_max,
         MIN(os.away_moneyline) AS away_ml_min,
         MAX(os.away_moneyline) AS away_ml_max
  FROM odds_snapshots os
  JOIN games g  ON os.game_id     = g.game_id
  JOIN teams th ON g.home_team_id = th.team_id
  JOIN teams ta ON g.away_team_id = ta.team_id
  GROUP BY g.game_id
  ORDER BY g.game_id
")
if (nrow(odds_summary) == 0) {
  cat("  No odds snapshots recorded\n")
} else {
  for (ii in seq_len(nrow(odds_summary))) {
    cat(sprintf("\n  %s\n",
        odds_summary$game_id[ii]))
    cat(sprintf("    %-28s  ML range: %+d to %+d\n",
      odds_summary$home_team[ii],
      odds_summary$home_ml_min[ii],
      odds_summary$home_ml_max[ii]
    ))
    cat(sprintf("    %-28s  ML range: %+d to %+d\n",
      odds_summary$away_team[ii],
      odds_summary$away_ml_min[ii],
      odds_summary$away_ml_max[ii]
    ))
    cat(sprintf("    Bookmakers: %d\n",
        odds_summary$bookmakers[ii]))
  }
}

cat("\n--- OUTCOMES ----------------------------------------\n")
outcomes <- dbGetQuery(con, "
  SELECT g.game_date, g.game_id,
         t.team_name, o.win
  FROM outcomes o
  JOIN games g ON o.game_id = g.game_id
  JOIN teams t ON o.team_id = t.team_id
  ORDER BY g.game_date, g.game_id, o.win DESC
")
if (nrow(outcomes) == 0) {
  cat("  No outcomes recorded\n")
} else {
  current_game <- ""
  for (ii in seq_len(nrow(outcomes))) {
    if (outcomes$game_id[ii] != current_game) {
      cat(sprintf("\n  %s | %s\n",
        outcomes$game_date[ii],
        outcomes$game_id[ii]))
      current_game <- outcomes$game_id[ii]
    }
    result <- if (outcomes$win[ii] == 1) "WIN" else "LOSS"
    cat(sprintf("    %-28s  %s\n",
      outcomes$team_name[ii], result
    ))
  }
}

cat("\n--- SUMMARY -----------------------------------------\n")
summary <- dbGetQuery(con, "
  SELECT
    (SELECT COUNT(*) FROM games)                  AS total_games,
    (SELECT COUNT(*) FROM games
     WHERE status = 'final')                      AS final_games,
    (SELECT COUNT(*) FROM games
     WHERE status = 'scheduled')                  AS scheduled_games,
    (SELECT COUNT(*) FROM probability_snapshots)  AS prob_snapshots,
    (SELECT COUNT(*) FROM odds_snapshots)         AS odds_snapshots,
    (SELECT COUNT(*) FROM outcomes)               AS outcomes
")
cat(sprintf("  Total games registered:  %d\n",
    summary$total_games))
cat(sprintf("  Final games:             %d\n",
    summary$final_games))
cat(sprintf("  Scheduled games:         %d\n",
    summary$scheduled_games))
cat(sprintf("  Probability snapshots:   %d\n",
    summary$prob_snapshots))
cat(sprintf("  Odds snapshots:          %d\n",
    summary$odds_snapshots))
cat(sprintf("  Outcome records:         %d\n",
    summary$outcomes))

cat("\n=======================================================\n")
dbDisconnect(con)

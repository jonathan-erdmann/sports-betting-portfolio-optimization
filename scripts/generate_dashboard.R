# =============================================================
# generate_dashboard.R
# Purpose:  Generates a three-panel dashboard PNG and pushes
#           it to the GitHub Pages site repository.
#           Panels: EV scatter, line movement, pipeline status
# Usage:    Rscript scripts/generate_dashboard.R
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(ggplot2)
library(patchwork)
library(scales)
library(ggtext)
library(dplyr)
library(here)
library(yaml)

source(here("R", "features", "remove_vig.R"))

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------

config      <- yaml::read_yaml(here("config", "config.yml"))
site_repo   <- "~/projects/jonathan-erdmann.github.io"
output_file <- file.path(
  site_repo, "assets", "images", "dashboard.png"
)
iDate <- Sys.Date()

# -------------------------------------------------------------
# Connect to database
# -------------------------------------------------------------

db_path <- here("db", "betting.sqlite")
con     <- dbConnect(RSQLite::SQLite(), db_path)

# -------------------------------------------------------------
# Panel 1 — EV Scatter Plot
# -------------------------------------------------------------

build_ev_scatter <- function(con, iDate) {
  
  opps <- dbGetQuery(con, "
    SELECT do.game_id, do.moneyline,
           do.implied_prob_fair,
           do.posterior_probability,
           do.expected_value,
           do.kelly_fractional,
           t.abbreviation AS team_abbr,
           t.team_name
    FROM daily_opportunities do
    JOIN teams t ON do.team_id = t.team_id
    JOIN games g ON do.game_id = g.game_id
    WHERE g.game_date = ?
    ORDER BY do.expected_value DESC
  ", params = list(as.character(iDate)))
  
  # All games with market probabilities for background
  all_games <- dbGetQuery(con, "
    SELECT DISTINCT g.game_id,
           th.abbreviation AS home_abbr,
           ta.abbreviation AS away_abbr,
           g.home_team_id, g.away_team_id
    FROM games g
    JOIN teams th ON g.home_team_id = th.team_id
    JOIN teams ta ON g.away_team_id = ta.team_id
    JOIN odds_snapshots os ON g.game_id = os.game_id
    JOIN probability_snapshots ps ON g.game_id = ps.game_id
    WHERE g.game_date = ?
  ", params = list(as.character(iDate)))
  
  # Get market and source probs for all teams
  all_probs <- dbGetQuery(con, "
    SELECT g.game_id,
           t.abbreviation AS team_abbr,
           t.team_id
    FROM games g
    JOIN teams t ON (
      t.team_id = g.home_team_id OR
      t.team_id = g.away_team_id
    )
    JOIN odds_snapshots os ON g.game_id = os.game_id
    JOIN probability_snapshots ps ON (
      ps.game_id = g.game_id AND
      ps.team_id = t.team_id
    )
    WHERE g.game_date = ?
    GROUP BY g.game_id, t.team_id
  ", params = list(as.character(iDate)))
  
  # Compute market fair probs for all teams
  closing_window <- config$feature_engineering$closing_window_hours
  
  plot_data <- data.frame()
  
  game_ids <- dbGetQuery(con, "
    SELECT DISTINCT g.game_id
    FROM games g
    JOIN odds_snapshots os ON g.game_id = os.game_id
    JOIN probability_snapshots ps ON g.game_id = ps.game_id
    WHERE g.game_date = ?
  ", params = list(as.character(iDate)))$game_id
  
  for (gid in game_ids) {
    
    market <- get_consensus_fair_prob(con, gid, closing_window)
    if (is.null(market)) next
    
    game_teams <- dbGetQuery(con, "
      SELECT home_team_id, away_team_id FROM games
      WHERE game_id = ?
    ", params = list(gid))
    
    src_probs <- dbGetQuery(con, "
      SELECT ps.team_id, ps.win_probability,
             t.abbreviation AS team_abbr
      FROM probability_snapshots ps
      JOIN teams t ON ps.team_id = t.team_id
      WHERE ps.game_id = ?
      GROUP BY ps.team_id
      HAVING ps.scrape_timestamp = MAX(ps.scrape_timestamp)
    ", params = list(gid))
    
    if (nrow(src_probs) == 0) next
    
    for (ii in seq_len(nrow(src_probs))) {
      
      tid      <- src_probs$team_id[ii]
      src_prob <- src_probs$win_probability[ii]
      abbr     <- src_probs$team_abbr[ii]
      
      mkt_prob <- if (tid == game_teams$home_team_id) {
        market$consensus_home
      } else {
        market$consensus_away
      }
      
      ev <- src_prob - mkt_prob
      
      # Check if in opportunities
      in_opps <- nrow(opps) > 0 &&
        any(opps$game_id == gid &
            opps$team_abbr == abbr)
      
      kelly <- if (in_opps) {
        opps$kelly_fractional[
          opps$game_id == gid &
          opps$team_abbr == abbr][1]
      } else 0
      
      ml <- if (in_opps) {
        opps$moneyline[
          opps$game_id == gid &
          opps$team_abbr == abbr][1]
      } else NA
      
      plot_data <- rbind(plot_data, data.frame(
        game_id    = gid,
        team_abbr  = abbr,
        mkt_prob   = mkt_prob,
        src_prob   = src_prob,
        ev         = ev,
        kelly      = kelly,
        moneyline  = ml,
        in_opps    = in_opps,
        stringsAsFactors = FALSE
      ))
    }
  }
  
  if (nrow(plot_data) == 0) {
    return(ggplot() +
      annotate("text", x = 0.5, y = 0.5,
               label = "No data available",
               size = 5, color = "gray50") +
      theme_void())
  }
  
  # Label for opportunities
  plot_data$label <- ifelse(
    plot_data$in_opps,
    paste0(plot_data$team_abbr, "\n",
           ifelse(plot_data$moneyline > 0,
                  paste0("+", plot_data$moneyline),
                  plot_data$moneyline)),
    ""
  )
  
  ggplot(plot_data, aes(x = mkt_prob, y = src_prob)) +
    
    # Shaded region where src > mkt (potential edge)
    annotate("rect",
             xmin = 0, xmax = 1,
             ymin = 0, ymax = 1,
             fill = "#e8f5e9", alpha = 0.5) +
    
    # Diagonal reference line (no edge)
    geom_abline(slope = 1, intercept = 0,
                linetype = "dashed",
                color = "gray40", linewidth = 0.5) +
    
    # Above diagonal shading label
    annotate("text", x = 0.15, y = 0.85,
             label = "Source > Market\n(Potential Edge)",
             size = 3, color = "gray50",
             fontface = "italic") +
    
    # All points
    geom_point(aes(color = ev, size = kelly + 0.005),
               alpha = 0.8) +
    
    # Labels for opportunities
    geom_text(aes(label = label),
              size = 2.8, vjust = -0.8,
              fontface = "bold",
              color = "#1a6b2e") +
    
    scale_color_gradient2(
      low      = "#d32f2f",
      mid      = "gray70",
      high     = "#2e7d32",
      midpoint = 0,
      name     = "Edge",
      labels   = percent_format(accuracy = 0.1)
    ) +
    scale_size_continuous(
      range  = c(2, 8),
      name   = "Kelly%",
      labels = percent_format(accuracy = 0.1)
    ) +
    scale_x_continuous(
      labels = percent_format(accuracy = 1),
      limits = c(0.1, 0.9)
    ) +
    scale_y_continuous(
      labels = percent_format(accuracy = 1),
      limits = c(0.1, 0.9)
    ) +
    labs(
      title    = "Today's Games — Market vs Source Probability",
      subtitle = paste0("ESPN Analytics vs consensus fair market | ",
                        format(iDate, "%B %d, %Y")),
      x        = "Market Fair Probability",
      y        = "Source (ESPN) Probability"
    ) +
    theme_minimal(base_size = 11) +
    theme(
      plot.title    = element_text(face = "bold", size = 12),
      plot.subtitle = element_text(color = "gray40", size = 9),
      legend.position = "right",
      panel.grid.minor = element_blank()
    )
  
}

# -------------------------------------------------------------
# Panel 2 — Line Movement
# -------------------------------------------------------------

build_line_movement <- function(con, iDate) {
  
  # Get all odds snapshots for today
  odds <- dbGetQuery(con, "
    SELECT os.game_id,
           os.scrape_timestamp,
           os.home_moneyline,
           os.away_moneyline,
           th.abbreviation AS home_abbr,
           ta.abbreviation AS away_abbr,
           g.game_time
    FROM odds_snapshots os
    JOIN games g  ON os.game_id     = g.game_id
    JOIN teams th ON g.home_team_id = th.team_id
    JOIN teams ta ON g.away_team_id = ta.team_id
    WHERE g.game_date = ?
    ORDER BY os.scrape_timestamp
  ", params = list(as.character(iDate)))
  
  if (nrow(odds) == 0) {
    return(ggplot() +
      annotate("text", x = 0.5, y = 0.5,
               label = "No odds data available",
               size = 5, color = "gray50") +
      theme_void())
  }
  
  # Compute consensus fair probability per snapshot
  odds$implied_home <- ml_to_implied(odds$home_moneyline)
  odds$implied_away <- ml_to_implied(odds$away_moneyline)
  odds$overround    <- odds$implied_home + odds$implied_away
  odds$fair_home    <- odds$implied_home / odds$overround
  
  # Average across bookmakers per game/timestamp
  odds$scrape_dt <- as.POSIXct(
    odds$scrape_timestamp,
    format = "%Y-%m-%dT%H:%M:%SZ",
    tz = "UTC"
  )
  
  # Create game label
  odds$game_label <- paste0(odds$away_abbr, "@", odds$home_abbr)
  
  # Aggregate consensus per game per timestamp
  agg <- odds %>%
    group_by(game_id, game_label, scrape_dt, game_time) %>%
    summarise(
      fair_home_mean = mean(fair_home),
      .groups = "drop"
    )
  
  # Parse game time for vertical line
  agg$game_posix <- as.POSIXct(
    agg$game_time,
    format = "%Y-%m-%dT%H:%M:%SZ",
    tz = "UTC"
  )
  
  # Only show games with multiple snapshots (movement exists)
  multi_snap <- agg %>%
    group_by(game_id) %>%
    filter(n() > 1) %>%
    ungroup()
  
  if (nrow(multi_snap) == 0) {
    multi_snap <- agg
  }
  
  ggplot(multi_snap,
         aes(x = scrape_dt, y = fair_home_mean,
             color = game_label, group = game_label)) +
    geom_line(linewidth = 0.8, alpha = 0.8) +
    geom_point(size = 2, alpha = 0.9) +
    scale_y_continuous(
      labels = percent_format(accuracy = 1),
      limits = c(0.1, 0.9)
    ) +
    scale_x_datetime(
      date_labels = "%H:%M",
      date_breaks = "2 hours",
      timezone    = "America/Chicago"
    ) +
    scale_color_viridis_d(option = "turbo", name = "Game") +
    labs(
      title    = "Consensus Fair Probability — Line Movement",
      subtitle = paste0("Home team fair probability throughout the day | ",
                        format(iDate, "%B %d, %Y")),
      x        = "Time (CDT)",
      y        = "Home Team Fair Probability"
    ) +
    theme_minimal(base_size = 11) +
    theme(
      plot.title      = element_text(face = "bold", size = 12),
      plot.subtitle   = element_text(color = "gray40", size = 9),
      legend.position = "right",
      legend.text     = element_text(size = 7),
      legend.key.size = unit(0.4, "cm"),
      panel.grid.minor = element_blank()
    )
  
}

# -------------------------------------------------------------
# Panel 3 — Pipeline Status
# -------------------------------------------------------------

build_status_panel <- function(con, iDate) {
  
  # Collect status metrics
  games_today <- dbGetQuery(con, "
    SELECT COUNT(*) as n FROM games
    WHERE game_date = ?
  ", params = list(as.character(iDate)))$n
  
  final_today <- dbGetQuery(con, "
    SELECT COUNT(*) as n FROM games
    WHERE game_date = ? AND status = 'final'
  ", params = list(as.character(iDate)))$n
  
  prob_today <- dbGetQuery(con, "
    SELECT COUNT(*) as n
    FROM probability_snapshots ps
    JOIN games g ON ps.game_id = g.game_id
    WHERE g.game_date = ?
  ", params = list(as.character(iDate)))$n
  
  odds_today <- dbGetQuery(con, "
    SELECT COUNT(*) as n
    FROM odds_snapshots os
    JOIN games g ON os.game_id = g.game_id
    WHERE g.game_date = ?
  ", params = list(as.character(iDate)))$n
  
  opps_today <- dbGetQuery(con, "
    SELECT COUNT(*) as n
    FROM daily_opportunities do
    JOIN games g ON do.game_id = g.game_id
    WHERE g.game_date = ?
  ", params = list(as.character(iDate)))$n
  
  pending <- dbGetQuery(con, "
    SELECT COUNT(DISTINCT g.game_id) as n
    FROM games g
    LEFT JOIN outcomes o ON g.game_id = o.game_id
    WHERE o.outcome_id IS NULL
      AND g.status = 'scheduled'
      AND g.game_date <= DATE('now', 'localtime')
  ")$n
  
  best_opp <- dbGetQuery(con, "
    SELECT do.expected_value, do.kelly_fractional,
           do.moneyline, t.team_name,
           b.bookmaker_name
    FROM daily_opportunities do
    JOIN games g  ON do.game_id  = g.game_id
    JOIN teams t  ON do.team_id  = t.team_id
    JOIN bookmakers b ON b.bookmaker_id = (
      SELECT bookmaker_id FROM odds_snapshots
      WHERE game_id = do.game_id
        AND (home_moneyline = do.moneyline
             OR away_moneyline = do.moneyline)
      LIMIT 1
    )
    WHERE g.game_date = ?
    ORDER BY do.expected_value DESC
    LIMIT 1
  ", params = list(as.character(iDate)))
  
  # Build status table data
  status_items <- data.frame(
    metric = c(
      "Date",
      "Last updated",
      "Games registered",
      "Games completed",
      "Outcomes pending",
      "Prob. snapshots",
      "Odds snapshots",
      "Opportunities"
    ),
    value = c(
      format(iDate, "%B %d, %Y"),
      format(Sys.time(), "%H:%M CDT"),
      as.character(games_today),
      as.character(final_today),
      as.character(pending),
      as.character(prob_today),
      as.character(odds_today),
      as.character(opps_today)
    ),
    stringsAsFactors = FALSE
  )
  
  # Add best opportunity if exists
  if (nrow(best_opp) > 0) {
    ml_str <- ifelse(best_opp$moneyline > 0,
                     paste0("+", best_opp$moneyline),
                     as.character(best_opp$moneyline))
    status_items <- rbind(status_items, data.frame(
      metric = "Best opportunity",
      value  = paste0(
        best_opp$team_name, " (", ml_str, " @ ",
        best_opp$bookmaker_name, ") — ",
        round(best_opp$expected_value * 100, 1), "% EV"
      ),
      stringsAsFactors = FALSE
    ))
  }
  
  # Render as ggplot table
  nn <- nrow(status_items)
  status_items$y <- rev(seq_len(nn))
  
  ggplot(status_items) +
    geom_text(aes(x = 0.05, y = y, label = metric),
              hjust = 0, size = 3.5,
              fontface = "bold", color = "gray30") +
    geom_text(aes(x = 0.55, y = y, label = value),
              hjust = 0, size = 3.5, color = "gray10") +
    geom_hline(yintercept = seq(0.5, nn + 0.5),
               color = "gray90", linewidth = 0.3) +
    scale_x_continuous(limits = c(0, 1.5)) +
    scale_y_continuous(limits = c(0.5, nn + 0.5)) +
    labs(title = "Pipeline Status") +
    theme_void() +
    theme(
      plot.title = element_text(face = "bold", size = 12,
                                margin = margin(b = 8)),
      plot.margin = margin(10, 10, 10, 10)
    )
  
}

# -------------------------------------------------------------
# Generate dashboard markdown page with dynamic content
# -------------------------------------------------------------

generate_dashboard_page <- function(con, iDate, iRunTime) {
  
  # Fetch today's opportunities
  opps <- dbGetQuery(con, "
    SELECT t.team_name,
           g.game_id,
           do.moneyline,
           do.implied_prob_fair,
           do.posterior_probability,
           do.expected_value,
           do.kelly_fractional,
           do.time_to_game_hours
    FROM daily_opportunities do
    JOIN teams t ON do.team_id  = t.team_id
    JOIN games g ON do.game_id  = g.game_id
    WHERE g.game_date = ?
    ORDER BY do.expected_value DESC
  ", params = list(as.character(iDate)))
  
  # Get bookmaker for each opportunity
  if (nrow(opps) > 0) {
    opps$bookmaker <- sapply(seq_len(nrow(opps)), function(ii) {
      bm <- dbGetQuery(con, "
        SELECT b.bookmaker_name
        FROM odds_snapshots os
        JOIN bookmakers b ON os.bookmaker_id = b.bookmaker_id
        WHERE os.game_id = ?
          AND (os.home_moneyline = ? OR os.away_moneyline = ?)
        ORDER BY os.scrape_timestamp DESC
        LIMIT 1
      ", params = list(opps$game_id[ii],
                       opps$moneyline[ii],
                       opps$moneyline[ii]))
      if (nrow(bm) > 0) bm$bookmaker_name[1] else "?"
    })
  }
  
  # Build markdown content
  ml_fmt <- function(ml) {
    ifelse(ml > 0, paste0("+", ml), as.character(ml))
  }
  
  opp_table <- if (nrow(opps) == 0) {
    "\n*No positive EV opportunities identified today.*\n"
  } else {
    rows <- paste(sapply(seq_len(nrow(opps)), function(ii) {
      sprintf("| %s | %s | %.1f%% | %.1f%% | %.1f%% | %.2f%% | %s |",
              opps$team_name[ii],
              ml_fmt(opps$moneyline[ii]),
              opps$implied_prob_fair[ii] * 100,
              opps$posterior_probability[ii] * 100,
              opps$expected_value[ii] * 100,
              opps$kelly_fractional[ii] * 100,
              opps$bookmaker[ii]
      )
    }), collapse = "\n")
    
    paste0(
      "| Team | ML | Market% | Posterior% | EV% | Kelly% | Book |\n",
      "|---|---|---|---|---|---|---|\n",
      rows, "\n"
    )
  }
  
  content <- paste0(
    "---\n",
    "layout: page\n",
    "title: \"\"\n",
    "permalink: /projects/portfolio-optimization/dashboard/\n",
    "---\n\n",
    "## Daily Pipeline Dashboard\n\n",
    "**Last updated:** ", format(iRunTime, "%B %d, %Y %H:%M CDT"), "\n\n",
    "---\n\n",
    "### Today's +EV Opportunities\n\n",
    opp_table, "\n",
    "---\n\n",
    "### Charts\n\n",
    "![Pipeline Dashboard](/assets/images/dashboard.png)\n\n",
    "*Dashboard refreshes at 6:00am, 11:00am, 2:00pm, and 4:00pm CDT.*\n"
  )
  
  page_path <- file.path(
    site_repo,
    "projects", "portfolio-optimization", "dashboard.md"
  )
  writeLines(content, page_path)
  cat("Dashboard page updated\n")
  
}

# -------------------------------------------------------------
# Build and save dashboard
# -------------------------------------------------------------

cat("=== Generating Dashboard ===\n")
cat("Date:", as.character(iDate), "\n\n")

cat("Building EV scatter...\n")
p1 <- build_ev_scatter(con, iDate)

cat("Building line movement...\n")
p2 <- build_line_movement(con, iDate)

cat("Building status panel...\n")
p3 <- build_status_panel(con, iDate)

# Generate dynamic markdown page
generate_dashboard_page(con, iDate, Sys.time())

# Combine with patchwork
dashboard <- (p1 | p2) / p3 +
  plot_annotation(
    title    = "Sports Betting Portfolio Optimization",
    subtitle = paste0("Jonathan Erdmann | Updated: ",
                      format(Sys.time(), "%Y-%m-%d %H:%M CDT")),
    theme    = theme(
      plot.title    = element_text(face = "bold", size = 14,
                                   hjust = 0.5),
      plot.subtitle = element_text(color = "gray40", size = 10,
                                   hjust = 0.5)
    )
  )

# Save PNG
output_dir <- dirname(output_file)
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

ggsave(
  filename = output_file,
  plot     = dashboard,
  width    = 16,
  height   = 10,
  dpi      = 150,
  bg       = "white"
)

cat("Dashboard saved to:", output_file, "\n\n")

# -------------------------------------------------------------
# Push to GitHub Pages
# -------------------------------------------------------------

cat("Pushing to GitHub Pages...\n")

push_cmd <- paste0(
  "cd ", site_repo, " && ",
  "git add assets/images/dashboard.png ",
  "projects/portfolio-optimization/dashboard.md && ",
  "git diff --cached --quiet || ",
  "git commit -m \"Update dashboard: ",
  format(Sys.time(), "%Y-%m-%d %H:%M"), "\" && ",
  "git push origin master"
)

result <- system(push_cmd, intern = TRUE)
cat(paste(result, collapse = "\n"), "\n")

dbDisconnect(con)

cat("\n=== Dashboard Complete ===\n")

# -------------------------------------------------------------
# Run if executed directly
# -------------------------------------------------------------

if (sys.nframe() == 0) {
  # Already runs on source — no additional call needed
}

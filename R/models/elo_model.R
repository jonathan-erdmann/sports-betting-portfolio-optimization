# =============================================================
# elo_model.R
# Purpose:  Pitcher-adjusted MLB Elo model (Approach A).
#           Initializes team ratings from 2025 season results
#           with regression-to-mean adjustment, updates after
#           each 2026 game, adjusts pre-game win probability
#           for starting pitcher FIP, and stores predictions
#           in probability_snapshots.
# Layer:    4 - Models
# Author:   Jonathan Erdmann
# =============================================================

library(DBI)
library(RSQLite)
library(httr)
library(jsonlite)
library(yaml)
library(here)

# =============================================================
# ELO MATH HELPERS
# =============================================================

# -------------------------------------------------------------
# Expected home win probability
# R_home - R_away + HFA shift relative to 400-point scale
# -------------------------------------------------------------

elo_win_prob <- function(iRatingHome, iRatingAway, iHFA) {
  1 / (1 + 10^(-((iRatingHome - iRatingAway) + iHFA) / 400))
}

# -------------------------------------------------------------
# Zero-sum Elo rating update after a game
# Returns list(home = new_home_rating, away = new_away_rating)
# -------------------------------------------------------------

elo_update <- function(iRatingHome, iRatingAway,
                        iActualHomeWin, iK, iHFA) {
  expected <- elo_win_prob(iRatingHome, iRatingAway, iHFA)
  delta    <- iK * (iActualHomeWin - expected)
  list(
    home = iRatingHome + delta,
    away = iRatingAway - delta
  )
}

# =============================================================
# MLB STATS API HELPERS
# =============================================================

# -------------------------------------------------------------
# Helper: safely extract a column value from a data frame row
# Returns NA on any error (missing column, out-of-bounds, etc.)
# -------------------------------------------------------------

safe_val <- function(iDF, iCol, iRow) {
  tryCatch({
    val <- iDF[[iCol]]
    if (is.null(val)) return(NA)
    val[iRow]
  }, error = function(e) NA)
}

# -------------------------------------------------------------
# Helper: fetch 2025 regular season schedule for a date range
# Fetches month by month — call with narrow ranges to avoid
# timeouts. Returns data frame: home_team_id, away_team_id,
# home_win, game_date — or NULL on failure.
# Team IDs in our DB match MLB Stats API team IDs directly.
# -------------------------------------------------------------

fetch_season_schedule <- function(iStartDate, iEndDate,
                                   iGameType = "R",
                                   iDebug = FALSE) {
								   
  url <- paste0(
    "https://statsapi.mlb.com/api/v1/schedule",
    "?sportId=1",
    "&startDate=", format(as.Date(iStartDate), "%Y-%m-%d"),
    "&endDate=",   format(as.Date(iEndDate),   "%Y-%m-%d"),
    "&hydrate=decisions,linescore",
	"&gameType=", iGameType
  )

  if (iDebug) cat(sprintf("  [API] schedule %s to %s\n",
                            iStartDate, iEndDate))

  parsed <- tryCatch({
    resp <- GET(url, add_headers("User-Agent" = "Mozilla/5.0"))
    if (status_code(resp) != 200) return(NULL)
    fromJSON(
      content(resp, as = "text", encoding = "UTF-8"),
      flatten = TRUE
    )
  }, error = function(e) {
    warning("Schedule fetch failed (", iStartDate, "-", iEndDate,
            "): ", conditionMessage(e))
    NULL
  })

  if (is.null(parsed)) return(NULL)
  total <- tryCatch(parsed$totalGames, error = function(e) 0)
  if (is.null(total) || total == 0) return(NULL)

  date_games_list <- tryCatch(parsed$dates$games,
                               error = function(e) NULL)
  if (is.null(date_games_list) || length(date_games_list) == 0) {
    return(NULL)
  }

  game_rows <- list()

  for (ii in seq_along(date_games_list)) {

    dg <- date_games_list[[ii]]
    if (is.null(dg) || !is.data.frame(dg) || nrow(dg) == 0) next

    for (jj in seq_len(nrow(dg))) {

      status <- safe_val(dg, "status.abstractGameState", jj)
      if (is.na(status) || status != "Final") next

      home_id    <- safe_val(dg, "teams.home.team.id",  jj)
      away_id    <- safe_val(dg, "teams.away.team.id",  jj)
      home_score <- safe_val(dg, "teams.home.score",    jj)
      away_score <- safe_val(dg, "teams.away.score",    jj)
      game_date  <- safe_val(dg, "officialDate",        jj)

      # Secondary fallback: linescore.runs
      if (is.na(home_score) || is.na(away_score)) {
        home_score <- safe_val(dg, "linescore.runs.home", jj)
        away_score <- safe_val(dg, "linescore.runs.away", jj)
      }

      if (any(is.na(c(home_id, away_id,
                       home_score, away_score, game_date)))) next

      home_id    <- as.integer(home_id)
      away_id    <- as.integer(away_id)
      home_score <- as.numeric(home_score)
      away_score <- as.numeric(away_score)

      if (home_score == away_score) next  # No ties in MLB

      game_rows[[length(game_rows) + 1]] <- data.frame(
        home_team_id = home_id,
        away_team_id = away_id,
        home_win     = as.integer(home_score > away_score),
        game_date    = as.character(game_date),
        stringsAsFactors = FALSE
      )
    }
  }
  if (length(game_rows) == 0) return(NULL)

  result <- do.call(rbind, game_rows)
  result[order(result$game_date), ]
}

# =============================================================
# PITCHER FIP HELPERS
# =============================================================

# -------------------------------------------------------------
# Helper: convert innings pitched string to numeric
# "100.2" → 100 + 2/3 = 100.667  ("100.1" → 100.333, etc.)
# -------------------------------------------------------------

parse_ip <- function(iIP) {
  iIP <- as.character(iIP)
  if (is.na(iIP) || nchar(trimws(iIP)) == 0) return(NA_real_)
  parts <- strsplit(trimws(iIP), "\\.")[[1]]
  full   <- suppressWarnings(as.numeric(parts[1]))
  thirds <- if (length(parts) >= 2)
    suppressWarnings(as.numeric(parts[2])) else 0
  if (is.na(full)) return(NA_real_)
  if (is.na(thirds)) thirds <- 0
  full + thirds / 3
}

# -------------------------------------------------------------
# Helper: extract FIP from a pitching stats block (flatten=FALSE)
# iStatsObj: person$stats from fromJSON(flatten=FALSE)
# iStatType: "season" or "career"
# Returns list(fip=..., ip=...) or NULL
# -------------------------------------------------------------

extract_pitching_fip <- function(iStatsObj, iStatType,
                                  iFipConst) {

  if (is.null(iStatsObj) || length(iStatsObj) == 0) return(NULL)

  for (kk in seq_along(iStatsObj)) {

    sg <- iStatsObj[[kk]]

    grp <- tryCatch(sg$group$displayName, error = function(e) "")
    typ <- tryCatch(sg$type$displayName,  error = function(e) "")

    if (!identical(grp, "pitching") ||
        !identical(typ, iStatType)) next

    splits <- tryCatch(sg$splits, error = function(e) NULL)
    if (is.null(splits) || length(splits) == 0) next

    # Take first (aggregated) split
    stat <- tryCatch(splits[[1]]$stat, error = function(e) NULL)
    if (is.null(stat)) next

    ip  <- parse_ip(tryCatch(stat$inningsPitched, error = function(e) NA))
    hr  <- suppressWarnings(as.numeric(
      tryCatch(stat$homeRuns,    error = function(e) NA)))
    bb  <- suppressWarnings(as.numeric(
      tryCatch(stat$baseOnBalls, error = function(e) NA)))
    hbp <- suppressWarnings(as.numeric(
      tryCatch(stat$hitByPitch,  error = function(e) 0)))
    so  <- suppressWarnings(as.numeric(
      tryCatch(stat$strikeOuts,  error = function(e) NA)))

    if (is.na(hbp)) hbp <- 0
    if (any(is.na(c(ip, hr, bb, so))) || ip <= 0) next

    fip <- ((13 * hr) + (3 * (bb + hbp)) - (2 * so)) / ip +
      iFipConst
    return(list(fip = fip, ip = ip))
  }

  NULL
}

# -------------------------------------------------------------
# Helper: look up pitcher FIP from MLB Stats API
# Tries 2026 season first; falls back to career; then default.
# Returns list(fip=..., source="season"/"career"/"default",
#             ip=...)
# iPitcherCache is modified in the CALLING environment via <<-
# (use only inside store_elo_predictions closure)
# -------------------------------------------------------------

get_pitcher_fip <- function(iPitcherName, iConfig) {

  league_avg <- iConfig$elo$league_avg_fip
  min_ip     <- iConfig$elo$min_innings_for_season_fip
  fip_const  <- 3.10

  default_result <- list(fip = league_avg, source = "default",
                         ip  = 0)

  # Search for pitcher by name
  search_url <- paste0(
    "https://statsapi.mlb.com/api/v1/people/search",
    "?names=",
    utils::URLencode(iPitcherName, reserved = TRUE),
    "&sportId=1"
  )

  search_parsed <- tryCatch({
    resp <- GET(search_url, add_headers("User-Agent" = "Mozilla/5.0"))
    if (status_code(resp) != 200) return(NULL)
    fromJSON(content(resp, as = "text", encoding = "UTF-8"),
             flatten = FALSE)
  }, error = function(e) NULL)

  if (is.null(search_parsed) ||
      is.null(search_parsed$people) ||
      length(search_parsed$people) == 0) {
    warning("Pitcher not found in MLB API: '", iPitcherName,
            "' — using league average FIP")
    return(default_result)
  }

  # Find first match with primary position = Pitcher (code "1")
  pitcher_id <- NA_integer_
  people     <- search_parsed$people

  for (ii in seq_along(people)) {
    pp       <- people[[ii]]
    pos_code <- tryCatch(pp$primaryPosition$code,
                         error = function(e) "")
    if (identical(pos_code, "1")) {
      pitcher_id <- tryCatch(as.integer(pp$id),
                             error = function(e) NA_integer_)
      break
    }
  }

  if (is.na(pitcher_id)) {
    warning("No pitcher position match for: '", iPitcherName,
            "' — using league average FIP")
    return(default_result)
  }

  # Try 2026 season stats first
  stats_2026 <- tryCatch({
    url <- paste0(
      "https://statsapi.mlb.com/api/v1/people/", pitcher_id,
      "?hydrate=stats(group=pitching,type=season,season=2026)"
    )
    resp <- GET(url, add_headers("User-Agent" = "Mozilla/5.0"))
    if (status_code(resp) != 200) return(NULL)
    fromJSON(content(resp, as = "text", encoding = "UTF-8"),
             flatten = FALSE)
  }, error = function(e) NULL)

  fip_result <- NULL

  if (!is.null(stats_2026) &&
      !is.null(stats_2026$people) &&
      length(stats_2026$people) > 0) {

    person     <- stats_2026$people[[1]]
    fip_season <- extract_pitching_fip(
      tryCatch(person$stats, error = function(e) NULL),
      "season", fip_const
    )

    if (!is.null(fip_season) && fip_season$ip >= min_ip) {
      fip_result <- list(
        fip    = fip_season$fip,
        source = "season",
        ip     = fip_season$ip
      )
    }
  }

  # Fall back to career stats if season insufficient
  if (is.null(fip_result)) {

    stats_career <- tryCatch({
      url <- paste0(
        "https://statsapi.mlb.com/api/v1/people/", pitcher_id,
        "?hydrate=stats(group=pitching,type=career)"
      )
      resp <- GET(url, add_headers("User-Agent" = "Mozilla/5.0"))
      if (status_code(resp) != 200) return(NULL)
      fromJSON(content(resp, as = "text", encoding = "UTF-8"),
               flatten = FALSE)
    }, error = function(e) NULL)

    if (!is.null(stats_career) &&
        !is.null(stats_career$people) &&
        length(stats_career$people) > 0) {

      person      <- stats_career$people[[1]]
      fip_career  <- extract_pitching_fip(
        tryCatch(person$stats, error = function(e) NULL),
        "career", fip_const
      )

      if (!is.null(fip_career) && fip_career$ip > 0) {
        fip_result <- list(
          fip    = fip_career$fip,
          source = "career",
          ip     = fip_career$ip
        )
      }
    }
  }

  if (is.null(fip_result)) {
    warning("FIP unavailable for '", iPitcherName,
            "' — using league average")
    return(default_result)
  }

  fip_result
}

# -------------------------------------------------------------
# Helper: compute pitcher Elo point adjustment
# delta = clip(w * (avg_fip - pitcher_fip) / avg_fip, min, max)
# Positive = better than average (boosts team's effective rating)
# Negative = worse than average (reduces effective rating)
# -------------------------------------------------------------

compute_pitcher_adj <- function(iPitcherFip, iConfig) {
  avg_fip  <- iConfig$elo$league_avg_fip
  weight   <- iConfig$elo$pitcher_adjustment_weight
  clip_min <- iConfig$elo$fip_clip_min
  clip_max <- iConfig$elo$fip_clip_max

  raw <- weight * (avg_fip - iPitcherFip) / avg_fip
  max(clip_min, min(clip_max, raw))
}

# =============================================================
# DATABASE HELPERS
# =============================================================

# -------------------------------------------------------------
# Helper: get current Elo rating for a team (or default)
# Returns list(rating=..., games_played=...)
# -------------------------------------------------------------

get_current_rating <- function(iCon, iTeamId, iSeason,
                                iConfig) {

  result <- dbGetQuery(iCon, "
    SELECT rating, games_played
    FROM elo_ratings
    WHERE team_id = ? AND season = ?
  ", params = list(iTeamId, iSeason))

  if (nrow(result) == 0) {
    return(list(rating       = iConfig$elo$initial_rating,
                games_played = 0L))
  }

  list(rating       = result$rating[1],
       games_played = result$games_played[1])
}

# -------------------------------------------------------------
# Helper: insert or update Elo rating row
# One row per (team_id, season); updates in place
# -------------------------------------------------------------

upsert_elo_rating <- function(iCon, iTeamId, iRating,
                               iDate, iGamesPlayed,
                               iSeason, iDebug) {

  if (iDebug) return(invisible(NULL))

  existing <- dbGetQuery(iCon, "
    SELECT rating_id FROM elo_ratings
    WHERE team_id = ? AND season = ?
  ", params = list(iTeamId, iSeason))

  if (nrow(existing) == 0) {
    dbExecute(iCon, "
      INSERT INTO elo_ratings
      (team_id, rating, effective_date, games_played, season)
      VALUES (?, ?, ?, ?, ?)
    ", params = list(iTeamId, iRating, as.character(iDate),
                     iGamesPlayed, iSeason))
  } else {
    dbExecute(iCon, "
      UPDATE elo_ratings
      SET rating = ?, effective_date = ?, games_played = ?
      WHERE team_id = ? AND season = ?
    ", params = list(iRating, as.character(iDate),
                     iGamesPlayed, iTeamId, iSeason))
  }
}

# =============================================================
# STEP 1: INITIALIZE FROM 2025 SEASON
# =============================================================

initialize_elo_2025 <- function(iCon, iConfig, iDebug = FALSE) {

  cat("\nInitializing from 2025 season...\n")

  K          <- iConfig$elo$k_factor
  HFA        <- iConfig$elo$home_field_advantage
  reg        <- iConfig$elo$regression_to_mean
  R0         <- iConfig$elo$initial_rating
  season_end <- iConfig$elo$season_end_2025

  # Fetch all MLB teams from database
  teams <- dbGetQuery(iCon, "
    SELECT t.team_id
    FROM teams t
    JOIN leagues l ON t.league_id = l.league_id
    WHERE l.abbreviation = 'MLB'
  ")

  # Initialize all teams at 1500
  ratings <- setNames(
    rep(R0, nrow(teams)),
    as.character(teams$team_id)
  )

  # Fetch schedule month by month to avoid API timeouts
  month_ranges <- list(
    list(start = iConfig$elo$season_start_2025, end = "2025-04-30"),
    list(start = "2025-05-01",                  end = "2025-06-30"),
    list(start = "2025-07-01",                  end = "2025-08-31"),
    list(start = "2025-09-01",                  end = season_end)
  )

  all_games <- list()
  total_fetched <- 0

  for (ii in seq_along(month_ranges)) {

    rng <- month_ranges[[ii]]
    cat(sprintf("  Fetching %s to %s ...", rng$start, rng$end))

    chunk <- fetch_season_schedule(rng$start, rng$end, iGameType = "R", iDebug)

    # Retry once on failure
    if (is.null(chunk)) {
      cat(" retry...")
      Sys.sleep(2)
      chunk <- fetch_season_schedule(rng$start, rng$end, iGameType = "R", iDebug)
    }

    if (!is.null(chunk)) {
      cat(sprintf(" %d games\n", nrow(chunk)))
      chunk$is_playoff <- FALSE
      all_games[[length(all_games) + 1]] <- chunk
      total_fetched <- total_fetched + nrow(chunk)
      Sys.sleep(3)
    } else {
      warning("Failed to fetch schedule: ", rng$start,
              " - ", rng$end)
      cat(" SKIPPED (API error)\n")
    }
  }
  # Fetch 2025 playoffs with reduced K-factor
  # gameType codes: F=Wild Card, D=Division, L=League CS, W=World Series
  playoff_types <- c("F", "D", "L", "W")
  playoff_start <- iConfig$elo$season_end_2025
  playoff_end   <- iConfig$elo$season_end_2025_playoffs
  total_playoff <- 0L

  for (pt in playoff_types) {
    cat(sprintf("  Fetching 2025 playoffs (gameType=%s) ...", pt))
    pt_chunk <- fetch_season_schedule(
      playoff_start, playoff_end,
      iGameType = pt,
      iDebug    = iDebug
    )
    if (is.null(pt_chunk)) {
      Sys.sleep(2)
      pt_chunk <- fetch_season_schedule(
        playoff_start, playoff_end,
        iGameType = pt,
        iDebug    = iDebug
      )
    }
    if (!is.null(pt_chunk) && nrow(pt_chunk) > 0) {
      cat(sprintf(" %d games\n", nrow(pt_chunk)))
      pt_chunk$is_playoff <- TRUE
      all_games[[length(all_games) + 1]] <- pt_chunk
      total_fetched <- total_fetched + nrow(pt_chunk)
      total_playoff <- total_playoff + nrow(pt_chunk)
    } else {
      cat(" 0 games\n")
    }
  }
  cat(sprintf("  Total playoff games: %d\n", total_playoff))


  if (length(all_games) == 0) {
    warning("No 2025 data — storing default ratings")
    for (ii in seq_len(nrow(teams))) {
      upsert_elo_rating(iCon, teams$team_id[ii], R0,
                        "2026-03-27", 0L, 2026, iDebug)
    }
    return(invisible(NULL))
  }

  all_games_df <- do.call(rbind, all_games)
  all_games_df <- all_games_df[order(all_games_df$game_date), ]

  cat(sprintf("\nProcessing %d regular season games in order...\n",
              nrow(all_games_df)))

  n_processed <- 0
  n_skipped   <- 0

  for (ii in seq_len(nrow(all_games_df))) {

    game <- all_games_df[ii, ]
    h_id <- as.character(game$home_team_id)
    a_id <- as.character(game$away_team_id)

    if (!h_id %in% names(ratings) || !a_id %in% names(ratings)) {
      n_skipped <- n_skipped + 1
      next
    }

	# Use reduced K-factor for playoff games
    K_game <- if (!is.null(game$is_playoff) &&
                  !is.na(game$is_playoff) &&
                  game$is_playoff) {
      iConfig$elo$playoff_k_factor
    } else {
      K
    }
    updated <- elo_update(
      ratings[[h_id]], ratings[[a_id]],
      game$home_win, K_game, HFA
    )

    ratings[[h_id]] <- updated$home
    ratings[[a_id]] <- updated$away
    n_processed     <- n_processed + 1
  }

  cat(sprintf("  Processed: %d  |  Skipped (unknown teams): %d\n",
              n_processed, n_skipped))

  # Apply regression to mean for 2026
  cat(sprintf("\nApplying %.0f%% regression to mean...\n", reg * 100))

  ratings_2026 <- lapply(ratings, function(rr) {
    rr * (1 - reg) + R0 * reg
  })

  # Build display data frame
  rating_df <- data.frame(
    team_id = as.integer(names(ratings_2026)),
    rating  = unlist(ratings_2026),
    stringsAsFactors = FALSE
  )
  team_names <- dbGetQuery(iCon,
    "SELECT team_id, team_name, abbreviation FROM teams"
  )
  rating_df <- merge(rating_df, team_names, by = "team_id")
  rating_df <- rating_df[order(-rating_df$rating), ]

  cat("\n=== Initial 2026 Elo Ratings (Post-Regression) ===\n")
  cat("Top 5:\n")
  for (ii in seq_len(min(5L, nrow(rating_df)))) {
    cat(sprintf("  %2d. %-25s  %.1f\n",
                ii,
                rating_df$team_name[ii],
                rating_df$rating[ii]))
  }
  nn <- nrow(rating_df)
  cat("Bottom 5:\n")
  for (ii in seq(max(1L, nn - 4L), nn)) {
    cat(sprintf("  %2d. %-25s  %.1f\n",
                nn - ii + 1L,
                rating_df$team_name[ii],
                rating_df$rating[ii]))
  }

  # Store in elo_ratings
  cat(sprintf("\nStoring %d initial ratings for season 2026...\n",
              nrow(rating_df)))

  for (ii in seq_len(nrow(rating_df))) {
    upsert_elo_rating(
      iCon,
      rating_df$team_id[ii],
      rating_df$rating[ii],
      "2026-03-27",
      0L,
      2026,
      iDebug
    )
  }

  if (!iDebug) {
    cat(sprintf("Stored %d ratings.\n", nrow(rating_df)))
  } else {
    cat("[DEBUG] Rating storage skipped.\n")
  }

  invisible(rating_df)
}

# =============================================================
# STEP 2: UPDATE 2026 RATINGS
# =============================================================

update_2026_ratings <- function(iCon, iConfig, iDebug = FALSE) {

  cat("\nUpdating 2026 ratings...\n")

  K   <- iConfig$elo$k_factor
  HFA <- iConfig$elo$home_field_advantage

  # Games with outcomes not yet in elo_game_log
  pending <- dbGetQuery(iCon, "
    SELECT g.game_id,
           g.game_date,
           g.game_time,
           g.home_team_id,
           g.away_team_id,
           o_home.win AS home_win
    FROM games g
    JOIN outcomes o_home ON  g.game_id   = o_home.game_id
                         AND o_home.team_id = g.home_team_id
    WHERE g.game_date >= '2026-03-27'
      AND g.status     = 'final'
      AND g.game_id   NOT IN (
        SELECT game_id FROM elo_game_log WHERE season = 2026
      )
    ORDER BY g.game_date, g.game_time
  ")

  if (nrow(pending) == 0) {
    cat("  No new 2026 games to process.\n")
    return(invisible(0L))
  }

  cat(sprintf("  Processing %d new games...\n", nrow(pending)))

  n_processed <- 0L

  for (ii in seq_len(nrow(pending))) {

    game <- pending[ii, ]
    h_id <- game$home_team_id
    a_id <- game$away_team_id

    h_info <- get_current_rating(iCon, h_id, 2026, iConfig)
    a_info <- get_current_rating(iCon, a_id, 2026, iConfig)

    h_pre <- h_info$rating
    a_pre <- a_info$rating

    expected_home <- elo_win_prob(h_pre, a_pre, HFA)
    updated       <- elo_update(h_pre, a_pre, game$home_win, K, HFA)

    h_post <- updated$home
    a_post <- updated$away

    if (iDebug) {
      cat(sprintf(
        "  [DEBUG] %-35s  H %.1f→%.1f  A %.1f→%.1f  p=%.3f actual=%d\n",
        game$game_id,
        h_pre, h_post, a_pre, a_post,
        expected_home, game$home_win
      ))
    } else {
      dbExecute(iCon, "
        INSERT INTO elo_game_log
        (game_id, home_team_id, away_team_id,
         home_rating_pre, away_rating_pre,
         home_rating_post, away_rating_post,
         home_pitcher_adj, away_pitcher_adj,
         predicted_home_prob, actual_home_win,
         game_date, season)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, 2026)
      ", params = list(
        game$game_id, h_id, a_id,
        h_pre, a_pre, h_post, a_post,
        expected_home, as.integer(game$home_win),
        as.character(game$game_date)
      ))

      upsert_elo_rating(iCon, h_id, h_post, game$game_date,
                        h_info$games_played + 1L, 2026, FALSE)
      upsert_elo_rating(iCon, a_id, a_post, game$game_date,
                        a_info$games_played + 1L, 2026, FALSE)
    }

    n_processed <- n_processed + 1L
  }

  cat(sprintf("  Updated ratings for %d games.\n", n_processed))
  invisible(n_processed)
}

# =============================================================
# STEP 3: VALIDATION
# =============================================================

validate_elo_model <- function(iCon, iConfig, iDebug = FALSE) {

  cat("\n=== Elo Model Validation (2026 Season) ===\n")

  elo_games <- dbGetQuery(iCon, "
    SELECT log_id, game_id, game_date,
           predicted_home_prob, actual_home_win
    FROM elo_game_log
    WHERE season = 2026
      AND actual_home_win IS NOT NULL
    ORDER BY game_date
  ")

  nn <- nrow(elo_games)

  if (nn == 0) {
    cat("  No 2026 outcomes in elo_game_log yet — skipping.\n")
    return(invisible(NULL))
  }

  cat(sprintf(
    "  Note: Small sample — %d games. Wide confidence intervals.\n",
    nn
  ))

  # Elo Brier score and confidence interval
  sq_err  <- (elo_games$predicted_home_prob - elo_games$actual_home_win)^2
  bs_elo  <- mean(sq_err)
  ci_half <- 1.96 * sqrt(bs_elo * (1 - bs_elo) / nn)

  # Directional accuracy
  correct <- (elo_games$predicted_home_prob > 0.5 &
                elo_games$actual_home_win == 1L) |
    (elo_games$predicted_home_prob < 0.5 &
       elo_games$actual_home_win == 0L)
  dir_acc <- mean(correct, na.rm = TRUE)

  cat(sprintf("\n  Games evaluated:       %d\n", nn))
  cat(sprintf(
    "  Elo Brier score:       %.4f (95%% CI: %.4f - %.4f)\n",
    bs_elo, max(0, bs_elo - ci_half), bs_elo + ci_half
  ))
  cat(sprintf("  Directional accuracy:  %.1f%%\n", dir_acc * 100))

  # ----------------------------------------------------------
  # Market Brier score (consensus fair probability)
  # ----------------------------------------------------------

  market_sq <- vapply(seq_len(nn), function(ii) {
    gid    <- elo_games$game_id[ii]
    actual <- elo_games$actual_home_win[ii]

    snaps <- dbGetQuery(iCon, "
      SELECT os.home_moneyline,
             os.away_moneyline,
             os.bookmaker_id
      FROM odds_snapshots os
      JOIN games g ON os.game_id = g.game_id
      WHERE os.game_id = ?
        AND (g.game_time IS NULL
             OR os.scrape_timestamp < g.game_time)
      ORDER BY os.scrape_timestamp DESC
    ", params = list(gid))

    if (nrow(snaps) == 0) return(NA_real_)

    snaps <- snaps[!duplicated(snaps$bookmaker_id), ]

    impl_h <- ifelse(snaps$home_moneyline < 0,
                     (-snaps$home_moneyline) /
                       (-snaps$home_moneyline + 100),
                     100 / (snaps$home_moneyline + 100))
    impl_a <- ifelse(snaps$away_moneyline < 0,
                     (-snaps$away_moneyline) /
                       (-snaps$away_moneyline + 100),
                     100 / (snaps$away_moneyline + 100))
    overround <- impl_h + impl_a
    fair_home <- mean(impl_h / overround)

    (fair_home - actual)^2
  }, numeric(1))

  n_market <- sum(!is.na(market_sq))

  # ----------------------------------------------------------
  # Per-source Brier score helper
  # ----------------------------------------------------------

  source_brier <- function(iSourceName) {
    src <- dbGetQuery(iCon, "
      SELECT source_id FROM probability_sources
      WHERE source_name = ?
    ", params = list(iSourceName))

    if (nrow(src) == 0) return(list(bs = NA_real_, n = 0L))

    src_id <- src$source_id[1]

    sq_vals <- vapply(seq_len(nn), function(ii) {
      gid    <- elo_games$game_id[ii]
      actual <- elo_games$actual_home_win[ii]

      home_tid <- dbGetQuery(iCon, "
        SELECT home_team_id FROM games WHERE game_id = ?
      ", params = list(gid))

      if (nrow(home_tid) == 0) return(NA_real_)

      snap <- dbGetQuery(iCon, "
        SELECT ps.win_probability
        FROM probability_snapshots ps
        JOIN games g ON ps.game_id = g.game_id
        WHERE ps.game_id   = ?
          AND ps.source_id = ?
          AND ps.team_id   = ?
          AND (g.game_time IS NULL
               OR ps.scrape_timestamp < g.game_time)
        ORDER BY ps.scrape_timestamp DESC
        LIMIT 1
      ", params = list(gid, src_id, home_tid$home_team_id[1]))

      if (nrow(snap) == 0) return(NA_real_)
      (snap$win_probability[1] - actual)^2
    }, numeric(1))

    nn_valid <- sum(!is.na(sq_vals))
    list(
      bs = if (nn_valid > 0) mean(sq_vals, na.rm = TRUE) else NA_real_,
      n  = nn_valid
    )
  }

  espn_res  <- source_brier("ESPN Analytics")
  fg_res    <- source_brier("FanGraphs")
  elo_s_res <- source_brier("Elo Model")

  fmt_row <- function(iLabel, iBS, iN) {
    if (is.na(iBS)) {
      cat(sprintf("  %-25s N/A           (n=0)\n", iLabel))
    } else {
      cat(sprintf("  %-25s %.4f        (n=%d)\n", iLabel, iBS, iN))
    }
  }

  cat("\n=== Comparison (same games) ===\n")
  fmt_row("Market Brier:",
          if (n_market > 0) mean(market_sq, na.rm = TRUE) else NA_real_,
          n_market)
  fmt_row("ESPN Analytics Brier:", espn_res$bs,  espn_res$n)
  fmt_row("FanGraphs Brier:",      fg_res$bs,    fg_res$n)
  fmt_row("Elo Model Brier:",      elo_s_res$bs, elo_s_res$n)

  invisible(list(bs_elo = bs_elo, dir_acc = dir_acc, n = nn))
}

# =============================================================
# Helper: build bulk pitcher FIP cache from MLB Stats API
# =============================================================

build_pitcher_fip_cache <- function(iConfig,
                                     iSeason = 2026) {

  fip_const  <- 3.10
  league_avg <- iConfig$elo$league_avg_fip
  min_ip     <- iConfig$elo$min_innings_for_season_fip

  r <- tryCatch(
    GET(
      "https://statsapi.mlb.com/api/v1/stats",
      query = list(
        stats      = "season",
        group      = "pitching",
        season     = as.character(iSeason),
        playerPool = "All",
        sportId    = "1",
        limit      = "1000"
      ),
      add_headers("User-Agent" = "Mozilla/5.0")
    ),
    error = function(e) NULL
  )

  if (is.null(r) || status_code(r) != 200) {
    warning("Pitcher stats API failed — using league average FIP")
    return(list())
  }

  parsed <- tryCatch(
    fromJSON(
      content(r, as = "text", encoding = "UTF-8"),
      flatten = FALSE
    ),
    error = function(e) NULL
  )

  if (is.null(parsed)) return(list())

  splits <- parsed$stats$splits[[1]]
  if (is.null(splits) || nrow(splits) == 0) return(list())

  cache <- list()

  for (ii in seq_len(nrow(splits))) {

    name <- splits$player$fullName[ii]
    if (is.null(name) || is.na(name)) next

    ip  <- suppressWarnings(
             as.numeric(splits$stat$inningsPitched[ii]))
    hr  <- suppressWarnings(
             as.integer(splits$stat$homeRuns[ii]))
    bb  <- suppressWarnings(
             as.integer(splits$stat$baseOnBalls[ii]))
    hbp <- suppressWarnings(
             as.integer(splits$stat$hitByPitch[ii]))
    so  <- suppressWarnings(
             as.integer(splits$stat$strikeOuts[ii]))

    if (is.na(ip) || ip < min_ip) {
      fip    <- league_avg
      source <- "default_low_ip"
    } else {
      fip    <- ((13 * hr) + (3 * (bb + hbp)) - (2 * so)) /
                ip + fip_const
      fip    <- max(0, min(10, fip))
      source <- paste0("season_", iSeason)
    }

    cache[[name]] <- list(fip = fip, ip = ip, source = source)
  }

  cache

}

# =============================================================
# STEP 4: STORE PREDICTIONS FOR TODAY
# =============================================================

store_elo_predictions <- function(iCon, iDate, iConfig,
                                   iDebug = FALSE) {

  cat(sprintf("\nComputing Elo predictions for %s...\n",
              as.character(iDate)))

  HFA <- iConfig$elo$home_field_advantage

  # Get Elo Model source_id
  elo_src <- dbGetQuery(iCon, "
    SELECT source_id FROM probability_sources
    WHERE source_name = 'Elo Model'
  ")

  if (nrow(elo_src) == 0) {
    warning("'Elo Model' not found in probability_sources")
    return(invisible(NULL))
  }
  elo_source_id <- elo_src$source_id[1]

  # Scheduled games for today
  games <- dbGetQuery(iCon, "
    SELECT g.game_id,
           g.home_team_id, g.away_team_id,
           g.home_starter,  g.away_starter,
           g.game_time,
           th.team_name AS home_team,
           ta.team_name AS away_team
    FROM games g
    JOIN teams th ON g.home_team_id = th.team_id
    JOIN teams ta ON g.away_team_id = ta.team_id
    WHERE g.game_date = ?
      AND g.status    = 'scheduled'
    ORDER BY g.game_time
  ", params = list(as.character(iDate)))

  if (nrow(games) == 0) {
    cat("  No scheduled games found for today.\n")
    return(invisible(0L))
  }

  cat(sprintf("  Found %d scheduled game(s).\n\n", nrow(games)))

  # Build bulk pitcher FIP cache from MLB Stats API
  cat("  Building pitcher FIP cache...\n")
  pitcher_cache <- build_pitcher_fip_cache(iConfig)
  cat(sprintf("  Cached %d pitchers.\n\n", length(pitcher_cache)))

  lookup_fip <- function(iPitcherName) {
    if (iPitcherName %in% names(pitcher_cache)) {
      return(pitcher_cache[[iPitcherName]])
    }
    list(fip    = iConfig$elo$league_avg_fip,
         ip     = 0,
         source = "default_not_found")
  }

  scrape_ts <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  cat(sprintf("%-24s  %-24s  %-18s  %-18s  %6s  %6s  %s\n",
              "Home", "Away",
              "Home Starter(FIP)", "Away Starter(FIP)",
              "pHome", "pAway", "Note"))
  cat(rep("-", 108), "\n", sep = "")

  n_stored <- 0L

  for (ii in seq_len(nrow(games))) {

    game <- games[ii, ]
    h_id <- game$home_team_id
    a_id <- game$away_team_id

    h_info   <- get_current_rating(iCon, h_id, 2026, iConfig)
    a_info   <- get_current_rating(iCon, a_id, 2026, iConfig)
    h_rating <- h_info$rating
    a_rating <- a_info$rating

    h_adj  <- 0
    a_adj  <- 0
    h_fip  <- iConfig$elo$league_avg_fip
    a_fip  <- iConfig$elo$league_avg_fip
    h_src  <- "avg"
    a_src  <- "avg"
    note   <- ""

    # Home starter
    if (!is.na(game$home_starter) &&
        nchar(trimws(game$home_starter)) > 0) {
      h_res <- lookup_fip(game$home_starter)
      h_fip <- h_res$fip
      h_adj <- compute_pitcher_adj(h_fip, iConfig)
      h_src <- substr(h_res$source, 1, 3)
    } else {
      note <- paste0(note, "home:TBD ")
    }

    # Away starter
    if (!is.na(game$away_starter) &&
        nchar(trimws(game$away_starter)) > 0) {
      a_res <- lookup_fip(game$away_starter)
      a_fip <- a_res$fip
      a_adj <- compute_pitcher_adj(a_fip, iConfig)
      a_src <- substr(a_res$source, 1, 3)
    } else {
      note <- paste0(note, "away:TBD")
    }

    # Pitcher-adjusted win probabilities
    # R_home_eff = R_home + delta_home + HFA (folded in)
    # R_away_eff = R_away + delta_away
    h_eff  <- h_rating + h_adj
    a_eff  <- a_rating + a_adj
    p_home <- 1 / (1 + 10^(-((h_eff - a_eff) + HFA) / 400))
    p_away <- 1 - p_home

    # Format starter display strings
    h_starter_str <- if (!is.na(game$home_starter) &&
                          nchar(trimws(game$home_starter)) > 0)
      sprintf("%s(%.2f,%s)",
              substr(game$home_starter, 1, 10), h_fip, h_src)
    else "(TBD)"

    a_starter_str <- if (!is.na(game$away_starter) &&
                          nchar(trimws(game$away_starter)) > 0)
      sprintf("%s(%.2f,%s)",
              substr(game$away_starter, 1, 10), a_fip, a_src)
    else "(TBD)"

    cat(sprintf("%-24s  %-24s  %-18s  %-18s  %6.3f  %6.3f  %s\n",
                substr(game$home_team, 1, 24),
                substr(game$away_team, 1, 24),
                h_starter_str,
                a_starter_str,
                p_home, p_away,
                trimws(note)))

    if (!iDebug) {
      # Remove existing Elo snapshots for this game today
      dbExecute(iCon, "
        DELETE FROM probability_snapshots
        WHERE game_id   = ?
          AND source_id = ?
          AND DATE(scrape_timestamp) = ?
      ", params = list(game$game_id, elo_source_id,
                       as.character(iDate)))

      # Home team
      dbExecute(iCon, "
        INSERT INTO probability_snapshots
        (game_id, source_id, team_id, scrape_timestamp,
         win_probability)
        VALUES (?, ?, ?, ?, ?)
      ", params = list(game$game_id, elo_source_id,
                       h_id, scrape_ts, p_home))

      # Away team
      dbExecute(iCon, "
        INSERT INTO probability_snapshots
        (game_id, source_id, team_id, scrape_timestamp,
         win_probability)
        VALUES (?, ?, ?, ?, ?)
      ", params = list(game$game_id, elo_source_id,
                       a_id, scrape_ts, p_away))

      n_stored <- n_stored + 2L
    }
  }

  cat(rep("-", 108), "\n", sep = "")

  if (iDebug) {
    cat(sprintf(
      "\n[DEBUG MODE] Would store %d probability snapshots.\n",
      nrow(games) * 2L
    ))
  } else {
    cat(sprintf(
      "\nStored %d probability snapshots for %d game(s).\n",
      n_stored, nrow(games)
    ))
  }

  invisible(nrow(games))
}

# =============================================================
# MAIN ENTRY POINT
# =============================================================

run_elo_model <- function(iDate        = Sys.Date(),
                           iDebug       = FALSE,
                           iForceReinit = FALSE) {

  cat("=== MLB Elo Model ===\n")
  cat("Date:", as.character(iDate), "\n")
  if (iDebug) cat("[DEBUG MODE — no database writes]\n")
  cat("\n")

  config  <- yaml::read_yaml(here("config", "config.yml"))
  db_path <- here("db", "betting.sqlite")

  con <- dbConnect(RSQLite::SQLite(), db_path)
  dbExecute(con, "PRAGMA foreign_keys = ON")
  on.exit(dbDisconnect(con))

  # Verify Elo tables exist
  tables <- dbListTables(con)
  missing <- setdiff(c("elo_ratings", "elo_game_log"), tables)
  if (length(missing) > 0) {
    stop("Missing Elo tables: ", paste(missing, collapse = ", "),
         "\nRun: Rscript setup/migrate_004_add_elo_tables.R")
  }

  # Register Elo Model as probability source (idempotent)
  if (!iDebug) {
    cols <- dbGetQuery(con,
      "PRAGMA table_info(probability_sources)")$name
    if ("weight_method" %in% cols) {
      dbExecute(con, "
        INSERT OR IGNORE INTO probability_sources
        (source_name, scrape_method, active,
         weight_method, prior_weight)
        VALUES ('Elo Model', 'model', 1, 'brier', 0.25)
      ")
    } else {
      dbExecute(con, "
        INSERT OR IGNORE INTO probability_sources
        (source_name, scrape_method, active)
        VALUES ('Elo Model', 'model', 1)
      ")
    }
  }

  # Step 1: Initialize from 2025 if needed
  n_ratings <- dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM elo_ratings WHERE season = 2026
  ")$n[1]

  if (iForceReinit || n_ratings == 0) {
    initialize_elo_2025(iCon = con, iConfig = config,
                        iDebug = iDebug)
  } else {
    cat(sprintf(
      "2026 ratings already initialized (%d teams).",
      n_ratings
    ))
    cat(" Use iForceReinit=TRUE to rebuild.\n")
  }

  # Step 2: Update ratings for completed 2026 games
  update_2026_ratings(iCon = con, iConfig = config,
                      iDebug = iDebug)

  # Step 3: Validate
  validate_elo_model(iCon = con, iConfig = config,
                     iDebug = iDebug)

  # Steps 4-5: Predict and store
  store_elo_predictions(iCon    = con,
                        iDate   = iDate,
                        iConfig = config,
                        iDebug  = iDebug)

  cat("\n=== Elo Model complete ===\n")
  invisible(NULL)
}

# =============================================================
# Run if executed directly
# =============================================================

if (sys.nframe() == 0) {
  run_elo_model(iDate = Sys.Date(), iDebug = TRUE)
}

# Sports Betting Portfolio Optimization — TODO

Last updated: 2026-04-12

## Layer 1 — Data Acquisition
- [x] ESPN Analytics probability scraper
- [x] FanGraphs probability scraper with starter tracking
- [x] Odds API moneyline scraper
- [x] MLB Stats API outcomes
- [x] Schedule registration with doubleheader handling
- [x] 7-day lookahead schedule registration
- [x] Opening odds capture for CLV measurement
- [ ] FanGraphs starter fetch for future games (1-2 day lookahead)
- [ ] Polymarket MLB integration (find correct API endpoint)
- [ ] Baseball Reference scraper (optional)
- [ ] Pinnacle odds tracking (sharper closing line reference)

## Layer 2 — Normalization
- [x] Complete via registry approach

## Layer 3 — Feature Engineering
- [x] Vig removal with pre-game line filter
- [x] Bayesian posterior probability
- [x] Multi-source posterior true weighted blend
- [x] EV calculation incorporating payouts
- [x] Kelly sizing with fractional adjustment
- [x] Portfolio optimization with constraint enforcement
- [x] Confidence weight updates via empirical Brier scores
- [ ] CLV calculation script
      - Entry line: morning odds snapshot day-of
      - Opening line: first snapshot captured 2-3 days out
      - Closing line: last snapshot before game time
      - Store in clv_records table (already in schema)
- [ ] Line dispersion signal — std dev of fair probs across
      bookmakers as market uncertainty measure; Kelly adjustment

## Layer 4 — Strategy and Simulation
- [ ] Bootstrap simulator (R/simulation/bootstrap_simulator.R)
      - Resample existing bets with replacement N=10,000 times
      - Distribution of ROI, max drawdown, Sharpe ratio
      - Bankroll trajectory fan chart (5th/25th/50th/75th/95th
        percentile paths)
      - Kelly fraction sensitivity analysis against historical bets
- [ ] Historical backtest (R/simulation/historical_backtest.R)
      - Fetch 2024 MLB outcomes from MLB Stats API (free)
      - Source historical odds (~$50-100 one-time cost)
      - Run full pipeline retroactively against 2024 data
      - ~2,400 observations for meaningful statistical validation
- [ ] Capital trajectory tracking
- [ ] Strategy definition framework — compare parameter sets

## Layer 5 — Evaluation
- [ ] Calibration curves — check at 30-day milestone (~Apr 27)
- [ ] CLV analysis — once CLV script complete
- [ ] Strategy comparison diagnostics

## Models
- [x] Pitcher-adjusted Elo model
      - 2025 regular season + playoff initialization (2,479 games)
      - 33% regression to mean
      - Bulk FIP cache from MLB Stats API (437 pitchers)
      - Current spread: 93.4 points (Rockies 1438 to Dodgers 1532)
      - Brier score: 0.2336 (beats market 0.2758 on same games)
- [ ] Elo-based EV for future games (2-3 day lookahead)
      - Base team ratings without pitcher adjustment
      - Flag as lower-confidence vs day-of predictions
- [ ] Logistic regression model (needs full season of stats)

## Infrastructure
- [x] Constrained Kelly portfolio optimizer
- [x] Windows Task Scheduler automation (morning + 4 refreshes)
- [x] GitHub Pages dashboard auto-push
- [x] Confidence weight updates after each pipeline run
- [x] In-progress game status tracking
- [x] Doubleheader conflict resolution with memory

## Performance (as of 2026-04-12)
- Days of data: 8 (Apr 5-12, excl Apr 4 and Apr 7 incomplete)
- Simulated bets at 1% threshold: 35
- Win rate: 54.3% (19/35)
- Total P&L: +$37.77 on $262.40 staked
- ROI: +14.4%
- Note: n=35 far too small — CI is roughly +/-36% on ROI
- Consistent pattern: underdogs outperforming favorites

## Brier Scores (as of 2026-04-12)
- Elo Model:      0.2336 ✓ beats market (n=34, wide CI)
- FanGraphs:      0.2399 ✓ beats market (n=120)
- ESPN Analytics: 0.2552 ✓ beats market (n=168)
- Market:         0.2758 baseline (n=130)
- April 27 milestone: ~400 obs, first meaningful comparison

## API Budget
- The Odds API: 114/500 used (22.8%) as of Apr 12
- ~8-9 requests per morning pipeline run
- Estimated depletion: mid-May at current rate
- Consider upgrading to paid tier before May

## Site
- [ ] Push Elo ratings plot to GitHub Pages (HTTP 408 pending)
- [ ] Push Brier score plot to GitHub Pages (HTTP 408 pending)
- [ ] Journal Entry 001 — theoretical foundation
- [ ] README.md — project overview
- [ ] Dashboard page formatting cleanup

## Research Articles (in progress via Claude Code)
- [ ] Fundamental Theory
- [ ] Applying Portfolio Theory to Sports Betting Markets
- [ ] Technical Implementation
- [ ] Real World Frictions in Sports Betting Strategy
- [ ] Why Closing Line Value Beats P&L as an Edge Metric
- [ ] Sports Betting as Binary Options: A Black-Scholes Framework

## Priority Order
1. CLV calculation script
2. Bootstrap simulator
3. FanGraphs starter fetch for future games
4. Calibration curves (Apr 27 milestone)
5. Historical backtest (requires odds data purchase)
6. Elo-based EV for future games
7. Line dispersion signal
8. Logistic regression model (September)
9. Multi-sport expansion (NBA wrapping up, NFL September)
10. Prediction market integration (Kalshi/Novig)
11. Site polish, README, journal entries

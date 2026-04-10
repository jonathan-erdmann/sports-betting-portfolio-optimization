# Sports Betting Portfolio Optimization — TODO

Last updated: 2026-04-10

## Layer 1 — Data Acquisition
- [x] ESPN Analytics probability scraper
- [x] FanGraphs probability scraper with starter tracking
- [x] Odds API moneyline scraper
- [x] MLB Stats API outcomes
- [x] Schedule registration
- [ ] Baseball Reference scraper (optional)
- [ ] Pinnacle odds tracking (higher quality closing line reference)

## Layer 2 — Normalization
- [x] Complete via registry approach

## Layer 3 — Feature Engineering
- [x] Vig removal
- [x] Bayesian posterior probability
- [x] EV calculation
- [x] Kelly sizing
- [x] Portfolio optimization with constraints
- [ ] Multi-source posterior — blend ESPN, FanGraphs, Elo
      weighted by Brier scores (currently uses best single source)
- [ ] CLV calculation — closing line value per bet

## Layer 4 — Strategy and Simulation
- [ ] run_simulation.R — replay historical data to back-test
- [ ] capital_trajectory.R — track simulated bankroll over time
- [ ] Strategy definition framework

## Layer 5 — Evaluation
- [ ] calibration.R — Brier scores per source, calibration curves
- [ ] clv_analysis.R — closing line value analysis
- [ ] strategy_comparison.R — cross-strategy performance diagnostics

## Models
- [x] Pitcher-adjusted Elo model with 2025 initialization
- [ ] Logistic regression model (needs full season of stats)
- [ ] Elo model playoff initialization
- [ ] Elo model directional accuracy improvement
  (currently 44% — will improve as ratings spread mid-season)

## Infrastructure
- [x] Constrained Kelly portfolio optimizer
- [x] Windows Task Scheduler automation
- [x] GitHub Pages dashboard auto-push
- [x] Confidence weight updates after each pipeline run
- [ ] db_summary.R update — include Elo ratings, FIP data
- [ ] renv sync between WSL and Windows environments

## Site
- [ ] Journal Entry 001 — theoretical foundation
- [ ] Dashboard page formatting cleanup
- [ ] Add repo link to portfolio optimization overview page
- [ ] MathJax rendering for framework pages
- [ ] README.md with project overview

## Operational
- [x] Cron/Task Scheduler running reliably
- [ ] Outcomes pipeline fully catching doubleheaders (monitor)

## Priority Order
1. Multi-source posterior blend (high value, quick win)
2. CLV calculation (enables signal validation now)
3. Calibration/Brier scores (accumulating — check at 30 days)
4. Simulation framework (needs more historical data)
5. Logistic regression model (needs full season)
6. Site polish and README
7. Pinnacle odds integration

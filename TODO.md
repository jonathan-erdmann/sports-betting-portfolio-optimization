# Sports Betting Portfolio Optimization — TODO

Last updated: 2026-04-05

## Layer 1 — Data Acquisition
- [ ] Baseball Reference scraper (optional — third probability source)
- [ ] Pinnacle odds tracking (higher quality closing line reference)

## Layer 2 — Normalization
- [x] Complete via registry approach

## Layer 3 — Feature Engineering
- [x] Vig removal
- [x] Bayesian posterior probability
- [x] EV calculation
- [x] Kelly sizing
- [x] Portfolio optimization with constraints
- [ ] Multi-source posterior — blend ESPN and FanGraphs
      weighted by Brier scores (currently uses best single source)
- [ ] CLV calculation — closing line value per bet,
      requires identifying closing snapshot

## Layer 4 — Strategy and Simulation
- [ ] run_simulation.R — replay historical data to back-test
- [ ] capital_trajectory.R — track simulated bankroll over time
- [ ] Strategy definition framework — define and compare
      multiple parameter sets

## Layer 5 — Evaluation
- [ ] calibration.R — Brier scores per source, calibration curves
- [ ] clv_analysis.R — closing line value analysis
- [ ] strategy_comparison.R — cross-strategy performance diagnostics

## Infrastructure
- [ ] Custom Elo model with pitcher adjustments
- [ ] db_summary.R update — include starter names,
      FanGraphs probabilities
- [ ] renv sync between WSL and Windows environments

## Site
- [ ] Journal Entry 001 — theoretical foundation
- [ ] Dashboard page formatting cleanup
- [ ] Add repo link to portfolio optimization overview page
- [ ] MathJax rendering for framework pages

## Operational
- [ ] Cron job in WSL as backup to Task Scheduler

## Priority Order
1. Multi-source posterior (high value, quick win)
2. CLV calculation (enables signal validation now)
3. Calibration/Brier scores (needs ~30 outcomes — a few more days)
4. Simulation framework (needs more historical data)
5. Elo model (high effort, high value)
6. Site polish
7. Additional probability sources

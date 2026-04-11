# Sports Betting Portfolio Optimization — TODO

Last updated: 2026-04-10

## Layer 1 — Data Acquisition
- [x] ESPN Analytics probability scraper
- [x] FanGraphs probability scraper with starter tracking
- [x] Odds API moneyline scraper
- [x] MLB Stats API outcomes
- [x] Schedule registration with doubleheader handling
- [ ] Baseball Reference scraper (optional)
- [ ] Pinnacle odds tracking (sharper closing line reference)

## Layer 2 — Normalization
- [x] Complete via registry approach

## Layer 3 — Feature Engineering
- [x] Vig removal with pre-game line filter
- [x] Bayesian posterior probability
- [x] EV calculation incorporating payouts
- [x] Kelly sizing with fractional adjustment
- [x] Portfolio optimization with constraint enforcement
- [x] Confidence weight updates via empirical Brier scores
- [ ] Multi-source posterior — true weighted blend across
      ESPN, FanGraphs, Elo (currently averages source probs)
- [ ] CLV calculation — closing line value per bet

## Layer 4 — Strategy and Simulation
- [ ] run_simulation.R — replay historical data to back-test
- [ ] capital_trajectory.R — track simulated bankroll over time
- [ ] Strategy definition framework — compare parameter sets

## Layer 5 — Evaluation
- [ ] calibration.R — calibration curves per source
- [ ] clv_analysis.R — closing line value analysis
- [ ] strategy_comparison.R — cross-strategy diagnostics

## Models
- [x] Pitcher-adjusted Elo model
      - 2025 regular season + playoff initialization
      - 33% regression to mean
      - Bulk FIP cache from MLB Stats API
      - Brier score: 0.2567 (beats market 0.2904,
        ESPN 0.2861, FanGraphs 0.2752 on same games)
- [ ] Logistic regression model (needs full season of stats)
- [ ] Elo directional accuracy improvement
      (currently 44% — will improve as ratings spread mid-season)

## Infrastructure
- [x] Constrained Kelly portfolio optimizer
- [x] Windows Task Scheduler automation (morning + 4 refreshes)
- [x] GitHub Pages dashboard auto-push
- [x] Confidence weight updates after each pipeline run
- [x] In-progress game status tracking
- [x] Doubleheader conflict resolution with memory
- [ ] db_summary.R update — include Elo ratings, FIP data
- [ ] renv sync between WSL and Windows

## Performance (as of 2026-04-10)
- Days of data: 6 (Apr 4-9, excluding Apr 4 incomplete)
- Simulated bets at 1% threshold: 20
- Win rate: 60% (12/20)
- Total P&L: +$7.13 on $152.50 staked
- ROI: +4.7%
- Note: Far too small sample for conclusions —
        need 500+ bets for statistical validity

## Brier Scores (as of 2026-04-10, n=79 games)
- Elo Model:      0.2567 ✓ beats market
- FanGraphs:      0.2752 ✓ beats market
- ESPN Analytics: 0.2861 ~ near market
- Market:         0.2904 baseline
- Note: Wide confidence intervals at n=79 —
        meaningful comparison needs n=300+

## Site
- [ ] Journal Entry 001 — theoretical foundation
- [ ] Dashboard page formatting cleanup
- [ ] Add Brier score plot to dashboard page
- [ ] Add repo link to portfolio optimization overview page
- [ ] README.md with project overview

## Priority Order
1. Multi-source posterior true weighted blend
2. CLV calculation script
3. Calibration curves (check at 30 days ~Apr 27)
4. Simulation framework
5. Logistic regression model (needs full season)
6. Site polish and README
7. Pinnacle odds integration

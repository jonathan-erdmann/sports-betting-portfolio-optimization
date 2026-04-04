# Sports Betting Portfolio Optimization

A quantitative research platform for capital allocation under
uncertainty, built on MLB moneyline markets as a transparent
and outcome-measurable proving ground.

The platform applies Kelly criterion optimization, Bayesian
probability adjustment, and portfolio theory to identify and
size bets across a simultaneous slate of games. It is designed
as a research tool first — the immediate objective is to
determine whether publicly available probability sources
contain information not already embedded in market odds, the
foundational question on which any edge depends.

## Architecture

The codebase is organized into five discrete layers:

- **R/acquisition** — Raw timestamped data capture from
  external APIs. Append-only, immutable outputs.
- **R/normalization** — Canonical game identity resolution
  across sources. Unified per-game-side-timestamp records.
- **R/features** — Derived analytical variables: vig removal,
  Bayesian posterior probability, expected value, line
  movement, closing line value.
- **R/strategy** — Parameterized strategy definitions, Kelly
  sizing functions, and simulation engine.
- **R/evaluation** — Diagnostics: calibration, Brier scores,
  CLV distribution, drawdown analysis, cross-strategy
  comparison.

## Theoretical Foundation

- Kelly, E.O. Thorp (2007) — Kelly Criterion in Blackjack,
  Sports Betting, and the Stock Market
- Bayesian probability adjustment via confidence-weighted
  posterior blending
- Joint portfolio optimization: max E[log(1 + Σ fᵢ · Xᵢ)]

## Data

- **Source:** The Odds API (MLB moneylines)
- **Database:** SQLite via RSQLite
- **Scope:** MLB moneylines, one bet per game

## Status

Research platform under active development.
Execution deferred pending signal validation.

## Documentation

Full theoretical foundation, system architecture, and
implementation journal available at:
https://jonathan-erdmann.github.io/projects/portfolio-optimization


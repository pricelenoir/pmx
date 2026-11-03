# pmx

A trading system for prediction markets (Kalshi). It records market and outside data, turns it into fair-value
forecasts, runs strategies in paper or live mode under layered risk limits, tracks portfolio risk and performance,
backtests on recorded and historical data, runs pre-registered research experiments, and shows it all in a terminal
dashboard.

Live trading, paper trading, and backtests run the same engine; only the clock, the data source, and the broker
differ. Money is integer, every price and position is in YES terms, and raw data is stored verbatim so everything
derived from it can be rebuilt.

## Setup

Requires [uv](https://docs.astral.sh/uv/) and, for the dashboard, Rust. macOS and Linux; for a server, see
[`runbooks/linux-vm.md`](docs/runbooks/linux-vm.md) (`scripts/setup-linux.sh`, systemd services, `pmx remote`).

```sh
uv sync --python 3.13
ln -s "$PWD/.venv/bin/pmx" ~/.local/bin/pmx       # `pmx` from anywhere
pmx help                                          # the manual: where to start, guides (pmx help live), commands
pmx doctor                                        # venv, TUI build, keys, clock, disk
```

Market data works without a key (REST polling); the websocket, account, and orders need one. Demo (`demo.kalshi.co`)
and prod are separate accounts; commands take `--env demo|prod`. Create a key on Kalshi (Account → API Keys) and run
`pmx setkey --env prod`, or make one here with `pmx keygen --env prod` and register its public half. Then
`pmx check --env prod` (add `--order` on demo for a 1¢ round trip).

## Usage

```sh
pmx up --demo && pmx tui --demo          # an invented session: no account needed

pmx markets --series KXNFLGAME --env prod   # look
pmx book TICKER --env prod
pmx record --env prod                    # record config/recorder.toml's series + sources (raw, replayable)

pmx config check                         # config/strategies.toml: sources, models, strategies, risk
pmx up --env prod                        # the engine in the background (pmx run: foreground) · pmx down
pmx tui --env prod                       # Markets · Portfolio · Strategies · Slots · Log
pmx status · positions · risk · perf --strategy ID
pmx order buy TICKER 5 @ 0.40            # manual orders, same risk checks and ledger (--mode live, --ioc)
pmx halt [ID | ID/TICKER] · resume · cancel --all
pmx ops install --record --run --backup  # run unattended (launchd or systemd) · pmx ops status
pmx remote vm tui · status · logs        # watch and control pmx on a server over SSH

pmx backtest --from DAY [--to DAY]       # recorded days (or instants) through the same engine
pmx backtest --nfl-history --seasons 2025,2026
pmx backtest report RUN · compare RUN paper:DAY · view RUN

pmx data kalshi --series KXHIGHNY        # any series' settled markets + candles (+ --trades) for research
pmx research init · new SLUG · run NAME · status    # pre-registered experiments (docs/RESEARCH.md)
```

**Live** needs `mode = "live"` on the strategy and `pmx run --live`, which starts `pmx guard` (cancels the engine's
orders if it dies); prod also asks you to type `prod`. Unattended operation: `docs/runbooks/operations.md`.

## How it works

Five kinds of plugin, registered by `kind` and wired together in `config/strategies.toml`:

| Plugin | Does | Examples |
|---|---|---|
| **Source** | brings data in; recorded raw | Kalshi books and trades, ESPN game state, a timer |
| **Linker** | says what a market is about | `KXNFLGAME-26OCT18HOUJAC-HOU` → the HOU @ JAX game |
| **Model** | turns data into fair probabilities | `espn_wp`, `market_mid` |
| **Strategy** | trades one market or one group of related markets | `noop`, `watch`, `scripted` |
| **Advisor** | slow reasoning outside the loop (an AI agent, or you); advice, never orders | `scripted_advice` |

Each strategy runs as an isolated slot per market with its own position, PnL, limits, and failures. A strategy only
asks (quote, take, target a position); the order manager, risk checks, scheduler, and broker decide what reaches the
venue. Viewers (the TUI, `pmx status`) attach to the running engine and can't slow it down.

```python
@strategy("edge_taker")
class EdgeTaker(Strategy):
    class Params(msgspec.Struct, frozen=True):
        min_edge: float = 0.04
        size: str = "5"

    def on_forecast(self, f, m: MarketContext) -> None:
        ask = m.book.best_ask() if m.book else None
        fair = round(f.p * ONE_DOLLAR)
        if ask and fair - ask[0] - m.taker_fee(ask[0]) >= self.p.min_edge * ONE_DOLLAR:
            m.take("bid", limit=ask[0], qty=parse_qty(self.p.size))
```

The same class is scenario-tested (`pmx.testing.Scenario`), backtested, paper traded, then traded live.

| Path | What |
|---|---|
| `config/` | `strategies.toml` (`pmx run`), `recorder.toml` (`pmx record`) |
| `data/` | raw recordings, `ledger.sqlite` (orders, fills, equity; positions derived), history, backtests, logs |
| `research/` | the private research workspace (`pmx research init`; git-ignored) |
| `src/pmx/` | the engine and everything around it · `tui/`: the Rust dashboard |

## Docs

[`ARCHITECTURE`](docs/ARCHITECTURE.md) (components, diagrams, contracts, invariants) ·
[`GUIDE`](docs/GUIDE.md) (adding sources, models, strategies, advisors) ·
[`RESEARCH`](docs/RESEARCH.md) (data, experiments, the pre-registration rules) ·
[`runbooks/`](docs/runbooks/) (operations, recording) · [`CLAUDE.md`](CLAUDE.md) (every command, design rules).

## Development

```sh
uv run pytest                  # offline suite · -m slow: performance budgets
uv run ruff check src tests
(cd tui && cargo test && cargo clippy --all-targets && cargo fmt --check)
```

CI runs all of it on every push, plus a clean install of the package, the server setup script on a fresh Ubuntu,
checks against Kalshi's live API (`pytest -m network`), and dependency audits.

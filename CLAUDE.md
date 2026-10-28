# pmx

A trading system for prediction markets (Kalshi) — sports, weather, economics, crypto. Read
`docs/ARCHITECTURE.md` (components, interface contracts, invariants) and `docs/GUIDE.md` (adding sources,
models, strategies, advisors) before changing code.

## Commands

```
pmx help [CMD] [--all]                  # every command with a one-liner; CMD's options; --all: every command's options
pmx markets --series KXNFLGAME --env prod
pmx book TICKER --env prod
pmx watch TICKER ...                    # needs API key (websocket is authenticated)
pmx record --env prod --series KXNFLGAME   # Kalshi (REST poll without a key) + config/recorder.toml [[source]]s
pmx record --counts                     # records per source today; --dry-run-retention: what retention would delete
pmx compact                             # raw log -> data/parquet/
pmx account
pmx setkey                              # after creating a key on Kalshi: asks for the key ID + the file it gave you,
                                        #   moves it to ~/.kalshi/pmx-<env>.key (0600), writes .env, runs pmx check
pmx keygen --env demo                   # or make an Ed25519 pair here, register its public key, then pmx setkey
pmx check --env demo --order            # auth + REST + websocket (+ 1¢ order round-trip, demo only)
pmx config check                        # validate config/strategies.toml and print the plan (no network)
pmx run --env prod                      # the engine: config/strategies.toml (sources, models, strategies, risk, paper
                                        #   broker, ledger, recorder) → feed on data/pmx.sock, foreground; Ctrl-C = orderly stop
pmx up --env prod                       # pmx run headless in the background (data/pmx.pid); pmx down stops it (--feed-only: serve)
pmx halt [ID] [--reason …]              # kill switch (data/HALT): everything, a strategy, or a slot <strategy>/<ticker>; pmx resume
pmx order buy TICKER 5 @ 0.40 [--mode live] [--ioc]   # manual orders via data/pmx-ctl.sock: slot manual/<ticker>
pmx cancel --all · status · positions · advise TICKER --p 0.6   # the running engine (status: or its health file)
pmx run --env demo --live               # live: starts `pmx guard` (cancels this instance's orders if the engine dies)
pmx doctor                              # venv = uv.lock, TUI, keys 0600, clock, disk;  pmx ops install|uninstall|backup
pmx risk [--mode paper]                 # the running engine's portfolio: equity, capital at risk, scenarios, limit usage
pmx backtest --from DAY [--to DAY] [--sources kalshi,espn]   # recorded days through the config → data/backtests/
                                        #   (--from/--to also take instants, DAYTHH:MM:SSZ: replay a live session)
pmx serve --env prod                    # pmx run without strategies or recording: market data + models → feed
pmx tui --env prod                      # attach the Rust dashboard; q detaches, pmx keeps running (starts serve, never run)
pmx tui --env prod --series KXNFLGAME --models market_mid,espn_wp   # override [watch], add models
pmx up --demo / tui --demo              # an invented session (own socket + pidfile, DEMO badge)
pmx up --demo --markets 5000 --rate 20000   # the load generator instead (slots on --slots share)
pmx bench feed [--inprocess]  # feed under load, 0/1/3 viewers: obs cost, traffic, conflation, drops
pmx bench core                          # engine-core dispatch latency, bare and with the tap → feed process
pmx bench soak --hours 4 --viewer-pid PID   # memory/files/queues over hours → data/soak.csv
pmx tui --build ...                     # force a rebuild (automatic when tui/ sources are newer)
```

Tests: `uv run pytest` (~14 s); performance budgets: `uv run pytest -m slow` (~35 s).
Lint: `uv run ruff check src tests`. Strategy tests use `pmx.testing.Scenario` (GUIDE).
Rust (`tui/`): `cargo test`, `cargo clippy --all-targets`, `cargo fmt` — keep all clean.
TUI budgets (frame, sort, apply at 5k/10k markets; fail past 2×): `cargo test --release scale -- --ignored --nocapture`.
venv: `uv sync` (Python 3.13 via `uv`; the dev tools are a default dependency group). `pmx` is the venv's script:
on the PATH via `ln -s "$PWD/.venv/bin/pmx" ~/.local/bin/pmx`, or `uv run pmx`.

## Design rules

- **Integer units only** (`core/units.py`): Price = 1e-4 $, Qty = 1e-2 contracts,
  Micros = 1e-6 $. Kalshi sends fixed-point strings (`*_dollars`, `*_fp`); subpenny
  grids and fractional contracts are real.
- **Everything in YES terms**: `bid` buys YES, `ask` sells YES. Kalshi NO bids become
  YES asks at 1 - p in `connectors/kalshi/wire.py`. Positions are signed YES qty.
- **Raw first**: the recorder stores venue frames verbatim (+ `Control` records for
  connects/gaps). Parsed tables are derived and rebuildable; replay uses the same
  decoder as live.
- **Seq gaps reconnect**: a gap closes the socket; reconnect re-snapshots every book.
  `BookTracker` drops books on `StreamReset` until the next snapshot.
- **Ledger derives, never stores, positions** (`ledger/db.py`).
- Order writes retry only on 429; a 5xx may have executed.
- **The TUI is a view.** `pmx serve` (Python) owns connectors and state; its feed process
  (`feed/server.py`) conflates and sends NDJSON over a Unix socket (`feed/proto.py` ⇄
  `tui/src/proto.rs` — change both together, and regenerate `tests/fixtures/feed/v3.ndjson`
  with `make_v3.py`; both test suites parse it). New viewers get full state first, then
  `synced`; a lagging viewer is dropped and resyncs.
- **Observability never blocks the hot path**: the engine's cost of being watched is
  O(1) and independent of viewers; parallelism via processes, not threads; no C++ or shared
  memory; Rust inside Python only where profiling proves it. Budgets: ARCHITECTURE.
- **Headless first**: viewers attach/detach; `pmx tui` never starts a trading engine.
- **Generic layers are market-agnostic**: engine, feed, TUI, portfolio,
  analytics never say "game"/"score"/"home". Outside data is an entity
  (`engine/events.py:EntityState`); domains (`sports/nfl/`) supply summary, fields, outcomes.

## Layout

core/ (units, types, book, fees, position) · connectors/ (Venue protocol; kalshi/) ·
recorder/ (rawlog, sink, daemon, replay, compact, retention) · sources/ (base, kalshi, timer, espn_nfl) ·
execution/ (risk, orders: OrderManager, scheduler, paper, live: LiveBroker, broker) · ledger/ · plugins.py (registries, build_params) ·
portfolio/ (tracker: PortfolioTracker, scenarios: outcome netting, metrics: limit usage) ·
advisors/ (base, view: FeedView, worker: AdvisorRunner + AdvisorLink; reference/scripted_advice) · testing/ (Scenario) ·
bench/ (feed, core, engine, soak, viewer) · control/ (protocol, server, client) · ops/ (guard, health, alerts, awake, backup, doctor, launchd) · backtest/ (runner: Runner, walk_forward; raw_source: RawLogSource) ·
engine/ (events, clock, state, engine: dispatcher + Tap, slots: Router, config: RunConfig, run: System) ·
strategies/ (base: Strategy, contexts, intents; reference/: noop, watch, scripted) · lifecycle.py ·
feed/ (proto v3, hub, publisher, server: feed process + EngineLink, demo + LoadGen, sources) ·
cli/ (one module per area, each `register(sub, common) -> {name: fn}`; `AREAS` in `cli/__init__.py`).
config/strategies.toml is what `pmx run` runs (engine/config.py validates it, engine/run.py builds it);
config/recorder.toml is `pmx record`'s.
tui/ — Rust/ratatui: proto, feed (server state), book, app (UI state + keys), ui (frame),
charts, views/ (markets, portfolio, strategies, slots, log), net (socket client).
models/ (base, runtime: loop/process/backtest latency, market_mid, espn_wp; nfl_wp research) ·
sports/nfl/ (domain: teams, catalog, state, features, data, espn, linker, outcomes) · harness/ (metrics, holdout eval).
Full module layout: `docs/ARCHITECTURE.md`.

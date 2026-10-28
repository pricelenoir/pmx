# Architecture

## Goals

1. **Modular by construction.** Data sources, models, and strategies are plugins: write a
   small class, register it by `kind`, wire instances together in one config file. Adding a
   data source or model never requires touching the engine.
2. **Each slot independent.** A strategy is written for *one* market (or one group of
   related markets). The framework runs an isolated copy (a **slot**) per market or group it
   is attached to — its own state, position, PnL, risk limits, and failure isolation. One
   slot's bug or loss limit never affects another.
3. **Many models at once.** Every configured model publishes its forecast per market;
   strategies pick the model they trade on; the TUI can show them side by side.
4. **Develop strategies by testing.** Scenario tests (scripted events → expected orders),
   then backtests, then paper, then live — the same strategy code at every step.
5. **One engine for live, paper, and backtest.** Only the clock, the event source, and the
   broker differ, so a backtest exercises exactly the code that trades.
6. **Honest evaluation.** Models are scored against the market price at the same instant,
   with realistic data latency, before any strategy trades them.
7. **Know whether it's working.** Portfolio risk (capital at risk, worst case by outcome,
   drawdown, limit usage) is tracked continuously across every slot and mode, and every
   strategy has performance stats with statistical confidence — computed by one library for
   live, paper, and backtests so they compare directly.
8. **Observe optionally.** A Rust terminal UI watches markets, portfolio performance, and
   per-strategy performance live; the system runs the same with or without it.

## Non-goals

- Sportsbooks as trading venues or live data sources.
- Sub-second latency. Kalshi round-trips are ~100 ms; Python is fine.
- Hosted/multi-user deployment, web UI, controlling the system from the TUI.

## The building blocks

| Block | What it does | You write | Registered as |
|---|---|---|---|
| **Source** | Brings data in (Kalshi books, ESPN games, weather obs…), stamps arrival time, records raw | a class with `async run(sink, stop)` | `@source("espn_nfl")` |
| **Linker** | Says which outside entity a Kalshi market is about (`KXNFLGAME-…-HOU` → NFL game `2026-10-18 HOU@JAX`) | a function `entity(contract) -> str \| None` | `@linker("nfl_game")` |
| **Model** | Turns events into `Forecast(model, ticker, p)` for linked markets | a class with `on_event(ev, state, now) -> list[Event]` | `@model("nfl_wp")` |
| **Strategy** | Decides orders for **one market** (or one group of related markets) from its book, forecasts, linked data, declared inputs, advice, position | a class with `on_event(ev, m: MarketContext)` | `@strategy("edge_taker")` |
| **Advisor** | Slow or non-deterministic reasoning — an AI agent, an external model service — running **outside** the engine loop and sending `Advice` in; never places orders | a class with `async run(view, sink, stop)` | `@advisor("llm_news")` |

Instances are declared in `config/strategies.toml` (see Config). The engine wires sources →
state → models → strategy slots → risk → order scheduler → broker, and publishes everything
to the TUI feed.

### Strategy design space

Every strategy is a setting of five independent dials. A new kind of strategy is a new
combination of settings, not a new engine feature.

| Dial | Settings |
|---|---|
| **Scope** — what one slot sees and trades | one market (`slot_by = "market"`) · a Kalshi event (`"event"`) · a linked entity (`"entity"`) · a custom group from a grouping linker (`{ linker = "fed_path" }`) · everything the strategy selects (`"all"`) |
| **Inputs** — what it reads | its markets' books and prints · linked entity data · model forecasts · declared source `inputs` · advice (AI agents, local models, or you via `pmx advise`) · a read-only portfolio view · contract rules and resolution times |
| **Triggers** — when it runs | every event · `cadence_ms` · timers · schedule windows (`Schedule` events, contract resolution times) |
| **Actions** — how it trades | `quote` / `take` · target positions worked by an execution helper (`m.target`) — always through risk → order scheduler → broker |
| **Compute** — where its thinking runs | in the loop (µs: arithmetic, small models) · a model process (ms: local ML, deterministic, backtestable) · an advisor worker (seconds: LLM agents) |

| Shape | Scope | Inputs | Triggers | Actions | Compute |
|---|---|---|---|---|---|
| Market making / microstructure | market | book, prints, `queue_ahead` | cadence | quote | loop |
| Consistency arbitrage (brackets sum to 1, nested thresholds, both teams, futures vs games) | event / entity / custom group | books | every event | take, quote | loop |
| Fair value from external feeds (spot vs threshold, live weather obs) | market | `inputs`, or a model over them | cadence | take, quote | loop or model process |
| Statistical / ML forecast | market | forecasts | forecast | take | model process |
| Scheduled releases (CPI, Fed, kickoffs, injury reports) | market / entity | schedules + a fast release source | schedule windows | pull before, take after | loop |
| AI agent — hosted or local LLM | market / entity | advice | advice | take | advisor worker |
| Human in the loop (you set the fair, it executes) | market | advice from `pmx advise` | advice | target | loop |
| Portfolio / basket allocation (size many edges together) | all | forecasts, portfolio view | timer | target | loop or model process |
| Hedging overlay (keep worst case under a cap) | all | portfolio view | timer | take its own hedges | loop |
| Near-resolution / rules-aware | market | contract rules, resolution time | schedule windows | take | loop |
| Cross-venue arbitrage (with a second venue) | custom group across venues | books on both | every event | take | loop |
| Learned / adaptive (online, RL) | any | any | any | any | loop + checkpoints; trained on the backtest runner |

### How the blocks connect

Rendered diagrams (the processes and storage; one event's path through risk) are in the repo's
[`README.md`](../README.md#how-it-works).

```
 sources ──► engine queue ──► state ──► models (shared; in loop or model process) ──► Forecast events
 (kalshi,     (ordered,       (books,                                            │
  espn_nfl,    single          entities,       advisors (worker process) ──► Advice events
  timer…)      consumer)       forecasts)                                        ▼
    │                                              strategy slots: one per (strategy, market or group)
    │ raw                                          ┌────────────────┐ ┌────────────────┐
    ▼                                              │ nfl-edge × HOU │ │ nfl-edge × JAX │ …
 recorder                                          │ own pos/PnL/   │ │ own pos/PnL/   │
                                                   │ risk/errors    │ │ risk/errors    │
                                                   └───────┬────────┘ └───────┬────────┘
                                                           ▼ quote / take / target
                               order manager ─► risk ─► order scheduler ─► broker (paper sim | Kalshi live)
                                                           │
                                     ledger ◄── fills ◄────┘        tap ──► feed process ──► TUI viewers

 pmx backtest:   recorded raw logs (live decoders) ──► the same Core, SimClock + PaperBroker ──► summary + ledger
 pmx.testing:    scripted events ──► one strategy slot ──► assert intents
```

### Portfolio and performance

Two components sit beside the order flow:

- **Portfolio tracker** (`portfolio/`, in the engine): sees every fill, mark (book), forecast,
  and settlement. Keeps positions and PnL per slot → strategy → mode → portfolio, scenario
  risk per entity (PnL if each outcome happens), capital at risk, expected value under the
  model and under the market, drawdown, and limit usage. Risk reads it for portfolio-level
  limits; it writes equity snapshots and per-fill annotations (fair, mid, markouts) to the
  ledger; it publishes `PortfolioSnapshot`s to the feed.
- **Analytics** (`analytics/`, pure functions over the ledger): per-strategy and per-slot
  performance stats, breakdowns, and a profitability verdict with clustered-bootstrap
  confidence intervals. Used by the live publisher (periodically), `pmx perf`, and backtest
  reports — same numbers everywhere.

### Linking data to markets

Outside data is keyed by an **entity** string, e.g. `nfl:2026-10-18:HOU-JAX`,
`wx:KNYC:2026-10-18`, or `econ:CPI:2026-10`. Source events that describe an entity satisfy
`EntityState` (`engine/events.py`): an `entity` key, `ts_ms`, and the domain's own
`summary()` / `display_fields()` for viewers. Linkers map each Kalshi `Contract` to an
entity. State keeps `entity_of[ticker]` and `tickers_of[entity]`, so a model receiving an
entity event knows which markets to forecast, and a strategy slot can read its market's
linked data (`m.data()`).

**Generic layers are market-agnostic.** The engine, feed, TUI, portfolio, and
analytics never name a domain concept ("game", "score", "home/away"). Domains live in their own packages (`sports/nfl/`) and plug in through entity events, linkers,
outcomes, and models.

## Process model and the hot path

The **hot path** is market data → engine state → models → slots → risk → orders. Everything
else — recording, analytics, the viewer feed, the TUI — is *observability*, and observability
never makes the hot path wait. pmx runs **headless**; viewers attach and detach at will.

```
 sources: one asyncio task per connection;            Kalshi ◄──── orders (≤ 10 writes/s, basic tier)
 heavy ones (big JSON, inference) in own processes        ▲
        │ normalized events (+ raw → recorder)            │
        ▼                                                 │
 ┌─ engine process ─────────────────────────────────────┴──┐
 │ single-threaded dispatch loop — the only hot path         │
 │ tap: every event → bounded in-memory ring, O(1), never    │
 │      blocks; full ring = drop + count (trading never waits)│
 └──────────────┬──────────────────────────────────────────┘
                │ batched, non-blocking writes (msgpack, Unix socket) — cost constant in viewers
                ▼
 feed process: snapshot state · conflation · fan-out                       recorder, analytics
                │ data/pmx.sock (NDJSON)                                       also off the engine thread
       ┌────────┴──────────┬───────────────┐
   pmx tui (attach)    pmx tui (2nd)    pmx risk / perf …   ← any number, any time; q detaches
```

### Rules

1. **O(1) observability on the engine.** The engine appends each event (already a struct)
   to a bounded ring; a writer task drains it in batches to the feed process. When the ring
   is full, the feed loses events (counted, shown in the TUI header as `dropped`); the engine
   never blocks on the feed, disk, or a viewer. The recorder follows the same rule (raw
   capture is buffered and written off the dispatch path).
2. **Conflation in the feed process (protocol v3).** For every market that changed:
   a summary (`top`: best bid/ask, last print, every model's fair, position per mode) and its
   top-10 `book`, at most 4 times a second (10 for markets with slots or working orders),
   latest value wins. Viewer traffic is bounded by markets × rate however fast the venue
   updates, and the feed is one-way. Prints, orders,
   fills, rejects, halts, and logs are never conflated. The recorder keeps every raw delta for
   analysis and replay.
3. **Processes, not threads, for parallelism.** Python threads share one interpreter lock,
   so CPU work runs in separate processes connected by Unix sockets. Processes also isolate
   crashes: a dead feed or viewer never stops trading; a dead engine leaves viewers showing
   "feed down" and reconnecting.
4. **Languages.** Python for the engine, strategies, and models. Rust for the TUI. Rust *inside* Python (pyo3) only where
   profiling shows a hot spot — first candidate: Kalshi frame decoding + book maintenance,
   which every market pays on every event. **No C++. No shared memory**: the conflated feed is
   a few thousand messages/s at most, which a Unix socket carries at < 1% CPU; shared memory
   (lock-free rings, struct layouts kept in sync across languages, crash recovery) pays only at
   millions of messages/s or microsecond budgets — colocated HFT, not Kalshi, where a round
   trip is ~100 ms and the basic tier allows ~10 order writes/s.
5. **Scale out only when measured.** If one engine core isn't enough, shard engines by market
   group into processes (slots are already per market), with a portfolio risk service
   granting each shard a risk budget — only once measurements show the need.

**The feed process (`feed/server.py`).** The serving process (`pmx run` or `pmx serve`)
holds the pidfile and starts the feed process as its child over a socketpair; the child serves
`data/pmx.sock`. The tap's delivery is `EngineLink.deliver`: one msgpack frame per batch, events
grouped by type, written without waiting. Past `high_water` (4 MB) queued in the socket, or with
the child down, batches are dropped and counted; once the child has caught up (or been restarted,
with back-off from 0.25 s) the link discards what the tap still holds and sends a snapshot instead:
the high-rate market state from engine `State` (full books, latest forecasts, last prints) plus
the low-rate state the link keeps from every batch (contracts, entities, slots, working orders,
halts, source status, account, portfolio, models, perf, equity history). The child resets its
current state (history stays), applies the snapshot, and resends every viewer its full state
(a `hello` resets a viewer). Entity states go over already rendered (`EntityMsg`), so the feed
process has no domain code. The child exits when the engine goes away: a dead engine still reads
as "feed down" to viewers. The feed process reports the engine's drop count as
`status(source="engine", dropped=N)`; the TUI header shows it when nonzero.

### Budgets

Measured on an Apple M1 Pro (8 cores, 16 GB, Python 3.13) with the load generator at 5,000 markets and 20k market-data events/s (`pmx serve --demo --markets
5000 --rate 20000`; skewed like a venue: a few markets get most events; 2% of markets with a slot).
`pytest -m slow` (`tests/test_budgets.py`) and the Rust `scale` test fail past 2× of these.

| What | Budget | Measured | Reproduce |
|---|---|---|---|
| Engine dispatch latency | p99 < 1 ms at 20k events/s | engine core (state + 2 `market_mid` models + a `noop` slot per market, 5,000 markets) paced at 20k inputs/s: bare p50 5.8 µs, p99 11.8 µs; with the tap → feed process p50 6.5 µs, p99 13.3 µs, 0 dropped. The whole engine (`pmx bench engine`: risk, orders, paper fills, portfolio; 1,000 markets each with a looping two-sided quote): p50 4.8 µs, p99 11.9 µs, max 27 ms (a Timer that has all 1,000 slots quote at once, ≈ 13 µs per order) | `pmx bench core`, `pmx bench engine` |
| Observability cost on the engine | < 5% of its CPU, constant in viewer count | ≈ 1.0 µs per input: tap append ~0.13–0.45 µs (the high end is GC with a large live heap; `gc.freeze()` after start-up brings it to 0.13) + delivery 0.43–0.7 µs/event (encode 0.26, keep 0.05, socket write) — ~18% of the bare core above, ~2% of one core at 20k events/s. Constant in viewers: 0.73 / 0.50 / 0.54 µs/event delivery with 0 / 1 / 3 viewers. Publishing in the engine process instead (`--inprocess`) costs 7–8 µs/event and grows with viewers (engine CPU 27 → 30 → 34% for 0 → 1 → 3) | `pmx bench feed` (vs `--inprocess`), `pmx bench core` |
| Feed → each viewer | ≤ 4 books/s per changed market (10 when traded) + unconflated prints/own events | busiest quiet market 3.9 books/s, busiest traded 9.5; per viewer 15.6k msg/s, 4.1 MB/s (book 9.1k, top 4.1k, trade 2.0k, entity 0.2k, slot 0.1k, order 0.08k /s); feed process 24–32% of a core | `pmx bench feed` |
| Publisher throttle | O(1) per event regardless of market count | cost per event with 5,000 markets held back = with 10 (1.9 µs); feed-process publisher + hub 4.1 µs/event at 5,000 markets. Reintroducing the O(pending) scan: 319 µs / 66 µs — both budget tests fail | `pytest -m slow -k held_back` |
| TUI frame | < 5 ms at 10k markets | 0.63 ms at 10,000 (0.73 at 5,000), 200×60; re-sort 7.4 ms at 10k (≤ 2/s); live at 16.5k msg/s: frame 1.0 ms avg | `cargo test --release scale -- --ignored --nocapture`; live: `PMX_DEBUG=1 pmx tui` header |
| TUI apply | — | delta 0.48 µs, 10-level book 3.1 µs, top 1.3 µs per message; live 1.3 µs/msg | same |
| Soak | memory, files, queues plateau over hours | 4 h at 5,000 × 20k/s with a TUI: engine 41–45 MB, feed 61–73 MB, TUI flat at 124–149 MB after the first hour; files flat; tap and link buffer empty; 0 drops; a forced 6 s stall recovers with one resync | `pmx bench soak --hours 4 --viewer-pid <tui pid>` |

### Lifecycle: headless, attach, detach

| Command | What happens |
|---|---|
| `pmx up [--demo \| --feed-only] [run args]` | starts `pmx run` (the engine with `config/strategies.toml`) in the background in its own session (closing the terminal doesn't stop it); returns once the socket accepts. `--feed-only` starts `pmx serve` instead, `--demo` the demo |
| `pmx tui [--demo]` | attaches a viewer; `q` detaches and **never stops pmx**; any number of viewers, each gets full state on connect and reconnects on its own. If nothing is running it starts the market-data feed (or demo) headless first — **never a trading engine**, which is only ever started explicitly (`pmx up` / `pmx run`; live needs both switches, invariant 11) |
| `pmx down [--demo]` | SIGTERM → orderly shutdown (halt, cancel, flush, close); SIGKILL after 10 s |
| `pmx run` / `pmx serve` | the same in the foreground (tmux, launchd, systemd); `pmx serve` is `pmx run` with no strategies and nothing recorded |
| `pmx halt [ID] [--scope slot\|strategy\|all] [--reason …]` / `pmx resume [ID]` | the kill-switch file `data/HALT`: risk reads it every `Timer` (≤ 2 s to halt and cancel); it outlives restarts |

The serving process holds `data/pmx.pid` (demo: `data/pmx-demo.pid`) next to its socket for
as long as it serves, so `pmx down` works however it was started; its feed process serves
the socket and exits with it. `pmx run` refuses to start while another engine holds that socket.

## Live safety, money correctness, operations

### Live safety
- **Dead-man switch.** If the engine dies or hangs, live resting orders stay at Kalshi with
  nobody managing them. A separate watchdog (`pmx guard`) cancels every live order of
  this engine instance when the engine's heartbeat stops for `guard_s` (default 10 s); it is
  started by `pmx up`/`pmx run --live`, and a live run doesn't start without it. Kalshi has no
  cancel-on-disconnect, so this is the protection.
- **No self-trades.** Two slots (or a manual order and a slot) can quote the same market; a
  buy crossing our own sell is a wash trade, which regulated exchanges prohibit. The order
  manager rejects any order that would cross one of our working orders on that market, in
  any slot (rule `self_cross`), and live orders carry Kalshi's self-trade prevention
  (`taker_at_cross`). The paper broker applies the same rule.
- **One engine per account.** Client order ids carry an instance id (`pmx-<instance>-…`);
  reconciliation adopts or cancels only its own instance's orders and warns loudly about
  other pmx instances' orders on the account.
- **Demo and prod never mix.** Every ledger row, `runs` row, and feed `hello` carries `env`;
  analytics and the portfolio never sum across envs (invariant 17). Books are paper, live on
  demo, and live on prod.
- **Venue rules are risk rules.** Market status (`paused`, `closed`, `determined`) blocks new
  orders (rule `market_not_open`) and pausing cancels resting orders; per-market position
  limits, where a venue publishes them, become a `venue_limit` rule (Kalshi publishes none); insufficient
  balance is checked before sending (rule `insufficient_balance`).
- **Order lifecycle races** are tested explicitly: a fill after our cancel was sent,
  a fill before the ack, duplicate fills after a reconnect (deduplicated by fill id), a
  cancel for an already-filled order, partial fill then cancel, out-of-order websocket
  messages.

### Money correctness
- **Fees per series.** Kalshi's fee schedule varies by series (some charge maker fees too). Contracts carry their schedule; paper,
  backtests, and `m.taker_fee`/`m.maker_fee` use it; live fills' reported fees are compared
  with the estimate and differences logged.
- **Cash and collateral.** Kalshi holds collateral for resting orders and positions (a YES
  bid at p ties up p per contract). The paper broker keeps a paper
  balance per run (configurable starting cash) with the same collateral rules, so paper and
  backtests can't trade capital they wouldn't have. Settled cash returns when the market
  settles, not when it is determined.
- **Settlement beyond yes/no.** Markets can be voided or settle at a value other than 0/1
  (an NFL tie settles at $0.50). The ledger records the settlement price; scenario risk
  assumes binary outcomes, so selectors exclude markets whose settlement isn't binary
  unless a domain declares their outcomes.
- **The ledger is durable.** SQLite in WAL mode, `synchronous = FULL` for live runs, an
  integrity check on start, and a daily backup copy. The venue is the truth for live
  positions (reconciliation); the ledger is the only record of paper history and attribution.
- `pmx export --year YYYY` writes fills, fees, and settlements per contract for taxes and to
  reconcile with Kalshi's statements.

### Operations and research hygiene
- **The machine stays awake.** A laptop sleeping stops recording and trading. While pmx runs it holds
  a no-sleep assertion (`caffeinate -s`, AC power), warns on battery, and records a
  `Control("sleep_gap")` when it detects a jump in wall time, so replays and markouts see
  the gap; books are re-snapshotted after one.
- **Clock check.** At start and hourly, compare local time with the venue's server time
  (HTTP `Date`); warn over 250 ms (markouts and latency depend on it).
- **Backups of what can't be rebuilt.** Raw recordings are irreplaceable; `pmx ops backup` copies
  `data/raw/` and the ledger to a second disk or cloud bucket daily.
- **Reproducible environments.** `uv.lock` checked in next to `Cargo.lock`; Python version
  pinned.
- **Sources degrade, they don't crash.** A parser failure (schema drift at an outside API)
  marks the source unhealthy, keeps recording raw (so it can be re-parsed after a fix), and
  alerts.
- **Models are versioned like strategies.** Forecasts carry the model's artifact hash;
  `fill_meta` stores it, so performance splits by model version too.
- **No secrets in config.** `pmx config check` rejects values that look like keys; secrets live
  in `.env` / `~/.kalshi/` only, and the config hash never includes them.
- **Soak and multiple-testing.** `pmx bench soak` runs the load generator for hours to prove memory and
  handles stay bounded. Backtest reports state how many parameter variants were tried
  ("best of N"), and sweeps use walk-forward windows.

## Invariants (never break these)

1. **Integer money.** `Price`/`Qty`/`Micros` ints everywhere (`core/units.py`). Floats only
   for probabilities and statistics. Money/quantity config values are strings parsed with
   `parse_price`/`parse_qty`/`parse_micros`.
2. **YES terms everywhere.** Venue conventions are converted only in connectors.
3. **Raw first.** Every source records raw payloads before parsing; parsed tables are
   rebuildable; replay uses live decoders/parsers.
4. **No wall clock in components.** Linkers, models, strategies, risk, order manager, paper
   broker read time only from the engine clock. Only sources and `LiveClock` call `time.time()`.
5. **No lookahead.** Components see only dispatched events. Historical sources stamp each
   event with when it would have been *known*, including modeled data latency.
6. **Single consumer, deterministic order.** Same inputs + config ⇒ byte-identical
   backtest ledger.
7. **Slot isolation.** A slot sees only the markets in its scope, its declared inputs and
   advisors, a read-only portfolio view, and its own orders/fills; it acts only on its own
   orders. An exception or risk halt in one slot stops only that slot.
8. **Book honesty.** Gaps or impossible deltas drop the book until the next snapshot;
   nothing trades on a stale book.
9. **Ledger derives positions** from fills; never stores them.
10. **Order writes retry only on HTTP 429.**
11. **Live needs two keys:** config `mode = "live"` *and* `pmx run --live`.
12. **The TUI is a view.** It holds no authority; the engine resends full state on connect.
    Viewers attach and detach without affecting the engine.
13. **Observability never blocks the hot path.** The engine's cost of being watched is O(1)
    per event and independent of viewers; when the feed falls behind, the feed loses data,
    the engine doesn't wait.
14. **Advisors never place orders.** They send bounded, expiring `Advice`; strategies decide
    and risk applies.
15. **Every order and fill is attributable** to its run, strategy code version, and params
    hash.
16. **No self-trades.** No order may cross one of our own working orders on the same market,
    in any slot or mode.
17. **Demo and prod never mix.** Every ledger and feed record carries its env; nothing sums
    across envs.

## Module layout

```
src/pmx/
  core/           units, types (Contract, OrderRequest, Fill, Trade, Book*), book, fees, position
  connectors/     base.py (Venue, MarketStream, Decoder) · kalshi/ (auth, REST, websocket, decoders, history)
  plugins.py      registries + decorators: @source @linker @model @strategy @advisor @outcomes, build_params
  lifecycle.py    pidfile, background start/stop for `pmx up` / `pmx down`
  engine/         events, clock, state, engine (+ Tap), slots (Router), config (RunConfig), run (Core, System)
  sources/        base (protocols, SourceSpec, build_source) · kalshi (kalshi_market, kalshi_account) · timer · espn_nfl
  sports/nfl/     the NFL domain: teams, catalog, state (GameState), features, data, espn, linker, outcomes, testing
  models/         base, runtime (loop / process / backtest latency), market_mid, espn_wp, nfl_wp (training)
  strategies/     base (Strategy, contexts, intents) · reference/ (noop, watch, scripted)
  execution/      risk, orders (OrderManager), scheduler, paper, live (LiveBroker), broker, algos (target positions)
  advisors/       base (Advisor, StateView, AdvisorEnv), view (FeedView), worker · reference/ (scripted_advice)
  portfolio/      tracker (positions, marks, PnL, equity, exposures, markouts), scenarios, metrics (limit usage)
  analytics/      stats, breakdowns, verdict, publish (performance from the ledger)
  backtest/       runner (Runner, walk_forward), raw_source (RawLogSource), nfl_history (candle fidelity), report
  recorder/       rawlog, sink (any source), daemon, replay (+ merged), compact, retention
  ledger/         db (orders, fills, equity, runs, settlements, checkpoints; positions derived)
  feed/           proto (v3), hub, publisher (conflation), server (feed process + EngineLink), demo (+ LoadGen), sources
  control/        protocol, server, client: manual orders, cancel, halt, status, positions, advice
  ops/            guard, health, alerts, notify, awake, backup, doctor, launchd
  testing/        the scenario kit for strategy tests
  bench/          load benchmarks: feed, core, engine, soak, viewer
  harness/        model evaluation: metrics, nfl_eval, kalshi_history
  cli/            one module per command area
tui/              Rust (ratatui) viewer: proto, feed, app, ui, charts, views/{markets,portfolio,strategies,slots,log}
config/           strategies.toml (`pmx run` / `serve` / `tui`) · recorder.toml (`pmx record`) · scripts/
docs/             architecture, guide, runbooks
tests/            pytest; fixtures under tests/fixtures/
```

## Event model

Every input and output is an engine event. Existing types are reused.

```python
# core/types.py (exists)
BookSnapshot, BookDelta, Trade, Fill, StreamReset, Contract, VenuePosition
VenueOrder(venue, order_id, client_order_id, ticker, side, price, qty, filled, remaining,
           status: resting | canceled | executed | unknown, ts_ms, reason="")   # the venue's view of our order

# engine/events.py
class EntityState(Protocol):                    # any domain's state about one entity
    ts_ms: int
    @property
    def entity(self) -> str: ...                # "nfl:2026-10-18:HOU-JAX", "wx:KNYC:2026-10-18"
    def summary(self) -> str: ...               # one line for viewers: "Q3 4:12 HOU 17-14 JAX"
    def display_fields(self) -> list[tuple[str, str]]: ...   # label/value rows for viewers

class Timer(msgspec.Struct, frozen=True):
    ts_ms: int                                  # heartbeat, default every 1 s

class SourceStatus(msgspec.Struct, frozen=True):
    source: str
    connected: bool
    detail: str
    ts_ms: int

class AccountSnapshot(msgspec.Struct, frozen=True):
    balance: Micros
    positions: list[VenuePosition]
    ts_ms: int

class Forecast(msgspec.Struct, frozen=True):
    model: str                                  # model *instance* name from config
    ticker: str
    p: float                                    # P(YES), 0..1
    ts_ms: int                                  # engine time it was made
    data_ts_ms: int                             # newest input it used (for staleness)
    meta: dict[str, float] = {}
    version: str = ""                           # model artifact hash / code version

class Advice(msgspec.Struct, frozen=True):      # from an advisor; strategies treat it like a forecast
    advisor: str                                # advisor instance name from config
    target: str                                 # ticker, or entity key (reaches every linked ticker)
    p: float | None                             # P(YES) for a ticker target
    direction: Literal["buy", "sell", "flat"] | None
    confidence: float                           # 0..1, the advisor's own
    expires_ms: int                             # engine time after which nothing may act on it
    rationale: str                              # one line, for logs and the TUI
    ts_ms: int

class Schedule(msgspec.Struct, frozen=True):    # a known future moment
    target: str                                 # entity key or ticker
    kind: str                                   # "release" | "kickoff" | "report" | "resolution" | …
    at_ms: int                                  # when it happens
    detail: str                                 # e.g. "CPI m/m, BLS 08:30 ET"
    ts_ms: int                                  # when this became known

class OrderUpdate(msgspec.Struct, frozen=True):
    slot: str                                   # "<strategy_id>/<ticker>"
    mode: Literal["paper", "live"]
    client_order_id: str
    order_id: str | None
    ticker: str
    side: BookSide
    price: Price
    qty: Qty
    filled: Qty
    status: Literal["queued", "pending", "open", "filled", "canceled", "rejected"]   # queued = waiting for write capacity
    ts_ms: int
    reason: str = ""

class SlotFill(msgspec.Struct, frozen=True):
    slot: str
    mode: Literal["paper", "live"]
    fill: Fill

class RiskReject(msgspec.Struct, frozen=True):
    slot: str
    ticker: str
    side: BookSide
    price: Price
    qty: Qty
    rule: str                                   # "max_position" | "stale_book" | "halted" | …
    detail: str
    ts_ms: int

class SlotStatus(msgspec.Struct, frozen=True):  # published for the TUI, throttled
    slot: str
    strategy: str
    ticker: str
    mode: Literal["paper", "live"]
    state: Literal["warming", "idle", "active", "halted", "error"]
    position: Qty
    realized: Micros
    unrealized: Micros
    fees: Micros
    working: int
    last_action: str
    ts_ms: int

class Halt(msgspec.Struct, frozen=True):        # a halt set or cleared
    scope: str                                  # slot id, strategy id, or "" = everything
    reason: str
    active: bool
    ts_ms: int

class Exposure(msgspec.Struct, frozen=True):    # one row per entity (or lone ticker) per mode
    entity: str
    mode: Literal["paper", "live"]              # paper and live are never netted
    tickers: list[str]
    strategies: list[str]
    capital_at_risk: Micros
    scenarios: dict[str, Micros]                # outcome name -> total PnL if it happens
    worst_case: Micros
    ev_model: Micros | None
    ev_market: Micros | None

class ScopeStats(msgspec.Struct, frozen=True):  # live numbers for one scope
    scope: str                                  # "portfolio" | "strategy:<id>" | "slot:<id>"
    mode: Literal["paper", "live"]
    realized: Micros
    unrealized: Micros                          # at mid
    fees: Micros
    equity: Micros                              # realized + unrealized − fees (since inception)
    today: Micros                               # equity change since local midnight
    hwm: Micros                                 # today's high-water mark (local day)
    drawdown: Micros                            # hwm − equity
    capital_at_risk: Micros
    worst_case: Micros
    ev_model: Micros | None
    stdev_model: Micros | None                  # settlement PnL stdev under the model
    gross_exposure: Micros                      # Σ |q| × mid
    working_exposure: Micros                    # extra capital at risk if all working orders fill
    open_positions: int
    stale: bool = False                         # some mark is a last-known mid, not a live book

class PortfolioSnapshot(msgspec.Struct, frozen=True):   # published every 1 s, throttled
    ts_ms: int
    scopes: list[ScopeStats]
    exposures: list[Exposure]
    limits: list[LimitUsage]

class LimitUsage(msgspec.Struct, frozen=True):
    scope: str
    limit: str                                  # e.g. "max_loss", "max_capital_at_risk"
    used: int
    cap: int
    breached: bool

class LogEvent(msgspec.Struct, frozen=True):
    level: Literal["info", "warn", "error"]
    source: str                                 # component or slot id
    text: str
    ts_ms: int

type Event = (BookSnapshot | BookDelta | Trade | Fill | StreamReset | Contract | Forecast | Advice | Schedule | Timer
              | SourceStatus | AccountSnapshot | OrderUpdate | SlotFill | RiskReject | SlotStatus | Halt
              | PortfolioSnapshot | LogEvent | EntityState)
```

Domain events live in their domain package and enter the union through `EntityState`
(e.g. NFL `sports/nfl/state.py:GameState`, kind `nfl_game`). Domain-only inputs that viewers
don't show — like the NFL WP model's pregame prior (`GamePrior`) — are defined by that
domain and consumed by its own models. `Contract` has no timestamp; in backtests events
travel as `(known_at_ms, seq, event)`.

## Dispatch (per event, `engine/engine.py`)

1. **State** applies it: contracts (+ linking via linkers), books, entities (latest
   domain state per entity), latest forecast per (model, ticker), latest unexpired advice per
   (advisor, target), upcoming schedules, last trades.
2. **Broker** sees it: the paper broker simulates fills for resting orders (trades, book
   moves). Fill/order events are queued.
2b. **Portfolio tracker** applies fills, settlements, and mark changes (books, forecasts);
   on `Timer` it snapshots equity, fills due markouts, and emits a throttled `PortfolioSnapshot`.
3. **Models** whose `wants(ev)` is true run (process-runtime models are sent the event and
   reply later, as events); returned events (forecasts, domain events, logs) are queued.
4. **Router** delivers it to the slots it concerns: market events → slots on that ticker;
   entity events → slots whose ticker links to the entity; a `Forecast` → slots on that
   ticker whose strategy uses that model; an `Advice` → slots on its target whose strategy
   lists that advisor; events from a source named in a strategy's `inputs` → all of that
   strategy's slots; fills/orders/rejects → their own slot; `Timer` → all slots. Slots with
   `cadence_ms` get book/trade/input hooks at most that often (latest state; fills and order
   updates are never delayed). Each slot's hooks run with its context and are timed against
   its CPU budget; intents are buffered.
5. **Order manager** turns each slot's intents into place/cancel requests; **risk** checks
   them (slot limits, then strategy-wide, then global); rejects become `RiskReject`; the
   **order scheduler** sends what passes within the venue's write limits.
6. **Publisher** forwards events and throttled `SlotStatus` to the feed hub.

Events a step returns are dispatched FIFO after the current event and **before the next
input**, so the dispatch sequence depends only on the inputs (live and replay agree). Every
event's `now_ms` is its input's receive time (`emit` stamps it). A slot never sees an event
before state reflects it. Exceptions in a model disable that model for that entity/ticker;
exceptions in a slot halt that slot (its orders are canceled); both emit `LogEvent`s.

## Interface contracts

The interfaces between components. Changing one means updating this file and every caller in
the same change.

### Plugins — `plugins.py`

```python
def source(kind: str): ...       # class decorator; class(name: str, params: Struct, ctx: SourceEnv)
def linker(kind: str): ...       # function decorator; fn(contract: Contract) -> str | None
def model(kind: str): ...        # class decorator; class(name: str, params: Struct, env: ModelEnv)
def strategy(kind: str): ...     # class decorator; class(params: Struct)
def advisor(kind: str): ...      # class decorator; class(name: str, params: Struct, env: AdvisorEnv)
def outcomes(linker_kind: str): ...  # function decorator; fn(entity, contracts) -> {outcome: {ticker: leg}}

SOURCES, LINKERS, MODELS, STRATEGIES, ADVISORS, OUTCOMES: dict[str, ...]
class PluginError(ValueError)        # duplicate kind, bad params; the message names the plugin
def load_builtin_plugins(packages=None, *, domains=True) -> None   # imports pmx.sources.*, pmx.models.*,
                                     #   pmx.strategies.*, pmx.advisors.* (or just `packages`), and <domain>/linker.py
                                     #   under pmx.sports (PLUGIN_PACKAGES, DOMAIN_PACKAGES)
def build_params(cls, table: dict | None) -> Params | None
def describe(obj) -> str             # "model 'market_mid'"
```

Each plugin class declares a nested `Params(msgspec.Struct)`; `build_params` decodes config
`params` tables into it with `msgspec.convert`. Unknown keys are errors at any depth (msgspec
alone ignores them); a plugin without `Params` accepts only an empty table. Re-registering the
same definition (a module reload) replaces it; a different class under a taken kind is an
error. Packages that don't exist are skipped by `load_builtin_plugins`; a module that fails
to import raises. Domains put linkers in `<domain>/linker.py` so the engine never imports their
research code.

### Clock — `engine/clock.py`

```python
class Clock(Protocol):
    def now_ms(self) -> int: ...
class LiveClock:                                # wall clock; never goes backwards (holds if the system clock steps back)
    def __init__(self, time_fn=time.time)
class SimClock:
    def __init__(self, start_ms: int = 0)
    def advance(self, ts_ms: int) -> None       # raises if time goes backwards
```

### Sources — `sources/`

```python
class Sink(Protocol):
    def emit(self, ev: Event) -> None
    def record(self, source: str, kind: str, payload: str) -> None    # raw capture

class Source(Protocol):                         # live
    name: str
    async def run(self, sink: Sink, stop: asyncio.Event) -> None

class HistoricalSource(Protocol):               # backtest (backtest/raw_source.py, backtest/nfl_history.py)
    name: str
    def events(self) -> Iterator[tuple[int, Event]]   # (known_at_ms, event), non-decreasing

# A source plugin may say what it records and how a replay reads it; without it, only `m`/`e` records replay:
@staticmethod
def raw_logs(name: str) -> dict[str, type[Decoder] | None]   # raw source -> decoder class
#   kalshi_market: {"kalshi": KalshiDecoder, "kalshi_rest": KalshiRestDecoder}; espn_nfl: {name: EspnDecoder}

class SourceSpec(Struct): name: str; kind: str; params: dict = {}   # one [[source]] from config
class SourceEnv: env: str; venues: dict[str, client]      # what `pmx run` hands every source
def build_source(spec, env=None) -> Source     # SOURCES[kind](name, build_params(cls, params), env); PluginError if bad
async def sleep_or_stop(stop, seconds) -> bool

# engine/engine.py — the engine side of a source
Engine.source(name, *, max=10_000, policy="conflate" | "drop_oldest" | "reset", key=None, venue="") -> SourceSink
class SourceSink:                               # a Sink: emit → the engine queue (stamped), record → the recorder
    pending: int; overflows: int; on_overflow: list[Callable[[], None]]   # reset: the source re-snapshots
```

Built-in sources: `kalshi_market` (`sources/kalshi.py`: universe from `series`/`tickers`; websocket with a
key, raw source `kalshi`; REST poll without one, raw source `kalshi_rest`, ≤ `rps` requests/s, unchanged
books not re-emitted; universe refresh every `refresh_s`; a `universe` control record and every contract's `m`
record at start and on each new UTC day; `resync()` re-snapshots; clock check),
The REST poller parses with `connectors/kalshi/decode.py:KalshiRestDecoder` (a snapshot per order-book response,
each tape trade once by id, for markets in the universe), which replays of `kalshi_rest` run too.
`kalshi_account` (fills and `VenueOrder` updates from the websocket `fill` + `user_orders` channels — never a
`StreamReset`: it holds no books — `AccountSnapshot` every `poll_s`, `on_connect` hooks; needs a key),
`timer` (`sources/timer.py`), `espn_nfl` (`sources/espn_nfl.py`). `pmx record` and `pmx serve` run the
same `kalshi_market` source, so a recording and a live session write identical raw logs.

**Contracts.** The Kalshi connector fills `Contract.rules` (`rules_primary` + `rules_secondary`),
`settlement_source` (the series' sources), `fees` (the series' `fee_type`/`fee_multiplier`: `quadratic` =
taker 7% only, `quadratic_with_maker_fees` = taker 7% + maker 1.75%; unknown types keep the conservative
default and log), and `settlement` (`settlement_value_dollars`, set once settled — not always 0 or 1: NFL
ties settle at 0.50). Kalshi publishes no per-market position limit, so `venue_limit` has nothing to read for it. State turns each contract's resolution time (`event_ts_ms`, else
`close_ts_ms`) into a `Schedule(target=ticker, kind="resolution")` whenever it is new or changes.

The Kalshi market-data source is always present; its universe is the union of all
strategy market selectors plus `[watch]` markets from config.

Rules every source follows:
- **Receive time.** The engine stamps each event with its local receive time when it enters
  the queue (the engine clock in backtests). Venue/exchange timestamps stay in the event;
  each source reports its median clock skew in `SourceStatus.detail`. Replays merge sources
  by recorded receive time, so a session replays in the order it was seen.
- **Bounded queue per source** — config `queue = { max = 10_000, policy = … }`:
  `conflate` keeps only the latest event per key (ticker, symbol — the default for
  high-rate feeds); `drop_oldest`; `reset` for venue books, where overflow is treated as a
  gap: the queue is cleared and a `StreamReset` forces re-snapshots (invariant 8). Overflows
  are counted in `SourceStatus` and the health file.
- **Aggregate fast feeds at the source.** A crypto trade stream at thousands of messages/s
  emits bars or features every 10–100 ms, not raw ticks; the raw ticks are still recorded.
- **No credentials in raw records.** `Sink.record` gets payloads only — never auth headers,
  signed URLs, or API keys; every source's fixtures are run through a redaction test.
- A source's `name` is what strategies list in `inputs` and what staleness is tracked by.

**Raw capture.** `recorder/sink.py:RecordingSink` implements `Sink.record` — one
`RawLogWriter` per source under `raw/<source>/` — and, as the recorder's whole `Sink`, drops emitted
events. `pmx record` runs the venue plus every `[[source]]` plugin in `config/recorder.toml` the same
way `pmx run` would, keeping only their raw records; the recorder itself knows no domain. Record kinds: `f` frame (a venue's message verbatim,
or an HTTP poll's `{"url", "status", "ms", "body"}` envelope with the body spliced in verbatim), `c`
control (connect, gap, `sleep_gap`, `redacted`), `m` contract metadata, `e` an engine event replays
must reproduce (`{"type": <name>, "event": …}`; domain types register with
`rawlog.register_event_type`). The sink refuses payloads that look like credentials
(`sink.looks_secret`, also used by the redaction test) and writes `sleep_gap` to every log when the
wall clock jumps more than 5 s past the monotonic clock. `replay(root, source, decoder)` replays one
source; `replay_merged(root, {source: decoder | None})` merges sources by receive time.

### Engine state — `engine/state.py`

Read-only to models and strategies. Markets are keyed by `MarketId` — the ticker on Kalshi,
`"<venue>:<ticker>"` on other venues — so group slots can span venues without changing
strategy code. Code that needs the
venue reads `contract.venue`; nothing else may assume tickers are globally unique.

```python
type MarketId = str                             # "<ticker>" on Kalshi, "<venue>:<ticker>" elsewhere
def market_id(venue: str, ticker: str) -> MarketId
def venue_ticker(mid: MarketId) -> tuple[str, str]

class State:
    def __init__(self, linkers: Mapping[str, Linker] | None = None, *, keep_past_schedules_ms=3_600_000)
    contracts: dict[MarketId, Contract]
    books: BookTracker
    entity_of: dict[MarketId, str]              # ticker -> entity (first configured linker that answers)
    tickers_of: dict[str, set[MarketId]]        # entity -> tickers
    entities: dict[str, EntityState]            # entity -> latest domain state (an older ts_ms never replaces a newer)
    forecasts: dict[tuple[str, MarketId], Forecast]  # (model, ticker) -> latest
    advice: dict[tuple[str, str], Advice]       # (advisor, target) -> latest unexpired (actionable through expires_ms)
    schedules: dict[str, list[Schedule]]        # target -> upcoming + passed within keep_past_schedules_ms, soonest first
    last_trade: dict[MarketId, Trade]
    inputs: dict[str, tuple[int, Event]]        # source name -> (receive ms, latest event): `m.input()`
    source: str                                 # source of the event being dispatched; "" = derived in the engine
    now_ms: int                                 # engine time of the event being dispatched
    def book(self, ticker: MarketId) -> OrderBook | None
    def forecast(self, ticker: MarketId, model: str) -> Forecast | None
    def data(self, ticker: MarketId) -> EntityState | None     # linked entity's latest state
    def keys_of(self, ev: Event) -> tuple[MarketId | None, str | None]   # (market, entity) an event is about
    def is_entity(self, ev) -> bool             # satisfies EntityState (cached per type)
    def venues(self) -> set[str]
    def apply(self, ev: Event, now_ms: int, source: str = "") -> list[Event]   # the engine only; returns linker logs
```

Advice whose `expires_ms` has already passed when it arrives removes that advisor's advice for
the target; malformed advice raises `Refused` (*Advisors*). `Timer`s prune lapsed advice and schedules older than `keep_past_schedules_ms`
(risk windows need a release for `after_s` once it has happened). A linker that raises is
logged and the next one is tried; the contract stays unlinked if none answers.

### Engine — `engine/engine.py`

```python
class Component(Protocol):                      # broker, portfolio tracker, router, order manager
    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]   # derived events (None = none)
class Model(Protocol):                          # the engine's view; models/base.py re-exports it
    name: str
    def wants(self, ev: Event) -> bool
    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]
class Publisher(Protocol):                      # the observability tap
    def append(self, ev: Event, now_ms: int) -> None                       # O(1), never blocks
class Recorder(Protocol):
    def record(self, source: str, kind: str, payload: str) -> None         # same shape as Sink.record

class Engine:
    def __init__(self, clock, state, *, models=(), router=None, order_manager=None, broker=None,
                 portfolio=None, publisher=None, recorder=None, sleep_gap_ms=5_000, max_derived=100_000)
    def emit(self, ev: Event, source: str = "") -> None       # queue an input stamped with clock.now_ms() (unbounded)
    def source(self, name, *, max=10_000, policy="conflate", key=None, venue="") -> SourceSink   # a bounded Sink
    async def dispatch_all(self) -> int                        # until the queue is empty (backtest runner)
    async def run(self, stop: asyncio.Event) -> None           # live; queued inputs are left for shutdown
    def dispatch(self, ev: Event, now_ms: int, source: str = "") -> None   # one input + everything it causes
    on_sleep_gap: list[Callable[[int], None]]   # called with the gap in ms (a venue source re-snapshots)
    now_ms: int; dispatched: int; queue_depth: int; max_queue_depth: int; errors: dict[str, int]
    def disabled(self, model: str) -> set[str]                 # markets/entities a model was switched off for

class Tap:                                      # Publisher: bounded ring + writer task
    def __init__(self, deliver: Callable[[list[tuple[int, Event]]], Awaitable[None]], *, capacity=65_536, batch=1_024)
    dropped: int
    async def run(self, stop: asyncio.Event) -> None           # drains batches to `deliver`
    def discard(self) -> Batch                  # take what's queued undelivered (on a resync a snapshot replaces it)
def to_publisher(publisher) -> deliver          # in-process delivery (replays, tests, `pmx bench feed --inprocess`)

# feed/server.py — the engine side of the feed process
class EngineLink:
    def __init__(self, sock: Path, hello: proto.Hello, *, snapshot: Callable[[], Iterable[Event]] = tuple,
                 tickers_of=…, tap: Tap | None = None, clock=…, high_water=4 << 20, resync_s=5.0)
    async def deliver(self, batch) -> None      # the Tap's delivery: never waits; drops + counts when slow/down
    async def run(self, stop) -> None           # start/supervise the feed process; restart with back-off
    dropped: int; sent: int; resyncs: int; restarts: int; up: bool; ready: asyncio.Event; total_dropped() -> int
def market_snapshot(state: State) -> Iterable[Event]          # `snapshot` for the engine: books, forecasts, prints
class TapPublisher                              # Publisher's methods (on_event, set_models, set_status, …) → the tap
```

- **Order.** state → broker → portfolio → models (`wants`) → router → order manager → tap; derived
  events FIFO before the next input; `now_ms` = the input's receive time.
- **Isolation.** A model that raises is switched off for the event's market (market events), entity
  (entity events), or everything (events about neither), with an error `LogEvent`. An event `State`
  refuses (`events.py:Refused`, e.g. malformed advice) becomes a warning `LogEvent` instead. Any other
  collaborator that raises gets an error `LogEvent` and dispatch continues; a failure while
  handling a `LogEvent` is only logged, so nothing feeds itself. An event state can't apply goes
  no further. More than `max_derived` events from one input cut the chain with an error log.
- **Sleep.** With a `LiveClock`, a `Timer` received more than `sleep_gap_ms` after the
  previous one records `Control("sleep_gap", gap_ms)` (`recorder.record("engine", "c", …)`), calls
  `on_sleep_gap`, and dispatches a warning plus a `StreamReset` per venue *before* the `Timer`.
- **Tap.** Full ring → the event is dropped and counted; a warning `LogEvent` is delivered at
  the gap's position. A `deliver` that stalls never delays dispatch.

### Models — `models/`

```python
class Model(Protocol):
    name: str                                   # instance name from config
    def wants(self, ev: Event) -> bool          # cheap filter
    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]   # Forecasts, domain events, logs
```

```python
class ModelEnv: data_root: Path; log: Logger     # cls(name, params, env)
def forecast(name, ticker, p, now_ms, data_ts_ms, *, version="", meta=None) -> Forecast   # p clamped to [0, 1]
class ModelSpec(Struct): name; kind; params: dict = {}; runtime: "loop" | "process" = "loop"; latency_ms: int = 0
def build_model(spec, env=None, *, backtest=False, linkers=None) -> Model   # models/runtime.py; PluginError if bad
class ProcessModel: attach(sink, loop=None); close(); dropped: int   # the engine's side of a worker process
class Delayed: ...                              # backtests: outputs held `latency_ms` of engine time
```

Stateful and incremental; no I/O in `on_event`; deterministic for a given event sequence.
Shipped: `market_mid` (P = book mid, `min_change` throttle) and, for the NFL domain, `espn_wp` (ESPN's
win probability to the home/away markets; final → 1/0, a tie → 0.5 as Kalshi settles it). Every forecast
carries `version` (`market_mid/1`; an artifact hash for trained models).
Anything needing I/O (e.g. fetching a market's opening price when the engine starts late) is
a **source** that emits an event and records it raw, so replays see it.

**Runtime.** `runtime = "loop"` (default) runs `on_event` inline — keep it to
microseconds. `runtime = "process"` runs the same class in a model worker process: the engine
sends it the events it `wants` and receives its `Forecast`s back as events (recorded raw, so
replays are exact), with `data_ts_ms` showing what each forecast saw. Use it for local ML
inference (scikit-learn, ONNX, MLX, a small local network) that takes milliseconds. In
backtests the model runs inline with a configured `latency_ms`, so results stay
deterministic. Workers run at lower OS priority than the engine and may share a local
inference server so weights load once.

A process-runtime model's class declares `event_types` (the types it consumes); `ProcessModel`
forwards those plus every `Contract` and `StreamReset` to the worker, which keeps its own `State`
(same linkers) and runs `wants`/`on_event` there. `on_event` never blocks: past `max_queue` pending
events, further ones are dropped and counted. The worker's outputs come back through
`attach(sink)` — a source sink named after the model — and are recorded raw (`e` records).

### Strategies — `strategies/base.py`, `engine/slots.py`

A strategy is written for one market (or one group of related markets, below). The engine
creates one instance per slot.

```python
class Strategy:                                 # subclass; all hooks optional
    Params: type[msgspec.Struct]
    inputs: ClassVar[tuple[str, ...]] = ()      # source names it reads (config `inputs` adds to these)
    cadence_ms: ClassVar[int] = 0               # 0 = every event; N = book/trade/input hooks at most every N ms
    warmup_s: ClassVar[float] = 0               # no orders until this much engine time has passed
    def __init__(self, params): self.p = params
    def ready(self, m: MarketContext) -> bool: return True     # extra readiness check after warm-up
    def on_start(self, m: MarketContext) -> None: ...
    def on_event(self, ev: Event, m: MarketContext) -> None: ...   # dispatches to the hooks below
    def on_book(self, book: OrderBook, m: MarketContext) -> None: ...
    def on_trade(self, t: Trade, m: MarketContext) -> None: ...
    def on_forecast(self, f: Forecast, m: MarketContext) -> None: ...
    def on_data(self, data: EntityState, m: MarketContext) -> None: ...  # linked entity changed
    def on_input(self, name: str, ev: Event, m: MarketContext) -> None: ...   # a declared input changed
    def on_advice(self, a: Advice, m: MarketContext) -> None: ...       # from an advisor it lists
    def on_fill(self, f: SlotFill, m: MarketContext) -> None: ...
    def on_timer(self, m: MarketContext) -> None: ...
    def on_stop(self, m: MarketContext) -> None: ...

class WorkingOrder(msgspec.Struct, frozen=True):
    client_order_id: str
    side: BookSide
    price: Price
    qty: Qty
    filled: Qty
    status: Literal["queued", "pending", "open"]
    queue_ahead: Qty | None                     # size ahead of us at our price: paper exact, live estimated
    placed_ms: int

class MarketContext:
    slot: str                                   # "<strategy_id>/<ticker>"
    ticker: str
    mode: Literal["paper", "live"]
    now_ms: int
    contract: Contract
    book: OrderBook | None                      # None = stale or not yet snapshotted
    position: Qty                               # this slot only, signed YES
    working: list[WorkingOrder]
    ready: bool                                 # warm-up done and `Strategy.ready` true
    throttled: bool                             # the order scheduler is holding writes for this slot
    def forecast(self, model: str | None = None) -> Forecast | None   # default: the strategy's model
    def data(self) -> EntityState | None        # linked entity state
    def input(self, name: str) -> Event | None  # latest event from a declared input (None if stale)
    def advice(self, advisor: str | None = None) -> Advice | None     # latest unexpired advice for this market
    def next_schedule(self, kind: str | None = None) -> Schedule | None   # soonest upcoming for this market/entity
    portfolio: PortfolioView                    # read-only (below)
    def target(self, qty: Qty, *, style: str = "passive", **opts) -> None   # worked by execution/algos.py
    def quote(self, *, bid: tuple[Price, Qty] | None, ask: tuple[Price, Qty] | None) -> None
    def take(self, side: BookSide, limit: Price, qty: Qty) -> None   # immediate-or-cancel
    def cancel_all(self) -> None
    def taker_fee(self, price: Price) -> Price  # per contract, rounded up (core/fees.py)
    def maker_fee(self, price: Price) -> Price
    def log(self, text: str, level: str = "info") -> None
```

`quote` declares the resting orders the slot wants now (post-only GTC); the order manager
diffs against working orders, requoting only on a ≥ 1-tick change. Calling `quote` again
replaces the target; `quote(bid=None, ask=None)` pulls. `take` is one-shot IOC.

The engine side — intents, the order manager's view, and the router:

```python
# strategies/base.py — what contexts buffer; every intent names its slot
Quote(slot, ticker, bid: (Price, Qty) | None, ask: (Price, Qty) | None) · Take(slot, ticker, side, limit, qty)
CancelAll(slot, ticker: MarketId | None = None) · Target(slot, ticker, qty, style="passive", opts={})
type Intent = Quote | Take | CancelAll | Target
class OrderView(Protocol):                      # the order manager (OrderTracker without one)
    def position(self, slot: str, ticker: MarketId) -> Qty
    def working(self, slot: str, ticker: MarketId) -> list[WorkingOrder]
    def throttled(self, slot: str) -> bool
class PortfolioView(Protocol):                  # the portfolio tracker (OrderTracker without one)
    def stats(self, scope: str, mode: str) -> ScopeStats
    def exposures(self, mode: str, strategy: str | None = None) -> list[Exposure]

# engine/slots.py
class MarketSelector(Struct): series, tickers, events: list[str] = []; linker: str | None = None
                              status: ContractStatus | None = "open"
class ByLinker(Struct): linker: str             # slot_by = { linker = "<name>" }
class StrategySpec(Struct, kw_only):            # one [[strategy]] from config
    id: str; kind: str; params: dict = {}; mode: Mode = "paper"; model: str | None = None
    markets: MarketSelector; slot_by: "market" | "event" | "entity" | "all" | ByLinker = "market"
    inputs: list[str] = []; advisors: list[str] = []          # inputs add to the class's
    cadence_ms: int | None = None; warmup_s: float | None = None   # None = the class's
    cpu_budget_ms: float = 50; risk: RiskLimits | dict | None = None   # read here only for stale_input_ms
class Slot:                                     # id "<strategy>/<ticker | group key>"; mode, model, key, tickers,
    risk_limits; warming; error; halted; stopped; last_action   # read by risk and the order manager
class Router:                                   # Component: dispatch step 4
    def __init__(self, specs, *, orders: OrderView | None = None, portfolio: PortfolioView | None = None,
                 linkers=LINKERS, outcomes: Callable[[str], dict[str, dict[MarketId, str]]] | None = None,
                 status_ms=1_000, cpu_clock=time.perf_counter_ns)
    slots: dict[str, Slot]
    def on_event(self, ev, state, now_ms) -> list[Event]   # LogEvent, SlotStatus, Halt(slot, "slow")
    def take_intents(self) -> list[Intent]      # the order manager drains these on the same event
    def stop_all(self, now_ms) -> list[Event]   # shutdown: on_stop + CancelAll per slot
    def checkpoint(self) -> dict[str, bytes]; def restore(self, blobs: Mapping[str, bytes]) -> None
class OrderTracker                              # OrderView + stub PortfolioView from SlotFill/OrderUpdate events
```

- **Selection.** A contract is selected when its series, event, or ticker is listed (nothing listed
  selects nothing) and, with `linker`, that linker links it. Slots start only on contracts in
  `status` (default `open`), including ones that appear mid-run; `paused` keeps them. A market that
  closes (`closed` / `determined` / `settled`) or leaves the selection gets a `CancelAll`; a slot with
  no markets left gets `on_stop` and is dropped once its working orders are gone.
- **Hooks.** The router calls `on_event` for everything it routes to a slot except declared inputs
  (`on_input`); the default `on_event` dispatches to the typed hooks. Contract updates, schedules,
  order updates, and rejects reach `on_event` only. Strategies that don't override `on_timer` (or
  `on_event`) aren't called on `Timer`s.
- **Cadence.** The first book/trade/input change opens a `cadence_ms` window; when it ends, each
  changed market's and input's hook runs once with the latest state. Nothing else is delayed.
- **CPU.** Each hook's wall time is charged to the engine second it ran in; `cpu_warn_s` (10)
  consecutive seconds over budget → warning `LogEvent`, `cpu_halt_s` (60) → halted `slow` (a `Halt`
  event and a `CancelAll`).
- **Failure.** A hook, `ready`, `on_load`, or the constructor raising → slot `error`: its buffered
  intents are dropped, `CancelAll`, an error `LogEvent`, no more hooks. `Halt` events (slot id,
  strategy id, or "") stop and resume hooks; canceling on a halt is risk's and the order manager's.
- **Warm-up** ends (latched) once `warmup_s` of engine time has passed since the slot started and
  `ready(m)` is true.
- **Status.** A `SlotStatus` at once on a state change, otherwise at most every `status_ms` when
  something changed (intents, own orders/fills/rejects, the PnL of a held position). A group slot
  publishes one status with `ticker` = its group key and position/PnL summed over its markets.
- **Orders are checked in the context.** Integer units, positive qty, on the contract's price grid,
  and a quote that doesn't cross itself; anything else raises (and the slot errors). `taker_fee` /
  `maker_fee` are per contract, rounded up.

**Inputs.** A strategy names the sources it reads (`inputs = ["coinbase_btc", "deribit_iv"]`);
every event from those sources reaches all of its slots via `on_input`, and `m.input(name)`
returns the latest one, or None once it's older than the slot's `stale_input_ms`. Combining
several feeds into a probability is usually cleaner as a model (`m.forecast()`); inputs are
for strategies that act on the raw signals themselves.

**Cadence and CPU budget.** With `cadence_ms`, book/trade/input hooks run at most that
often with the latest state (heavy math across many markets); fills, order updates, advice,
and timers are never delayed. The router times every hook: a slot over `cpu_budget_ms`
(per second of engine time, default 50) for 10 s gets a warning `LogEvent`, for 60 s it is
halted with reason `slow`.

**Warm-up.** A slot is `warming` until `warmup_s` of engine time has passed since it started
and `ready(m)` is true; risk rejects its orders with rule `warming`. After a restart,
`[engine] warm_from_recording` replays the last `warmup_s` of recorded data through the slot
with trading disabled.

**Group slots (`slot_by`).** `slot_by = "market"` (default) is one slot per market, as
above. `slot_by = "event"` (one slot per Kalshi event), `"entity"` (per linked entity),
`{ linker = "<name>" }` (per key from a strategy-specific grouping linker — e.g. a Fed-path
group across several events, or one question listed on two venues), or `"all"` (one slot over
everything the strategy selects — basket allocation, hedging overlays) gives one slot all its
markets at once — for math across related contracts (both
teams' YES should sum to ~1, bracket ladders, nested thresholds). Its context is a
`GroupContext`: `tickers`, `market(ticker) -> MarketView` (contract, book, position,
working, forecast, data), `quote(ticker, …)`, `take(ticker, …)`, `cancel_all(ticker=None)`,
`outcomes` (the domain's outcome map: `{outcome: {ticker: "yes" | "no" | YES settlement price}}`). Slot ids are `<strategy_id>/<event, entity, or group
key>` (`<strategy_id>/all` for `"all"`); risk
limits apply per ticker and across the group; portfolio scenarios already net within an
entity. Cross-venue arbitrage uses the same mechanism with a second venue. Grouping
linkers only shape slots; portfolio risk still nets by the domain's entities.

**Portfolio view.** `m.portfolio` is a read-only view from the portfolio tracker:
`stats(scope, mode)`, `exposures(mode)`. Overlays read it to decide; they act only through
their own slots (a hedge is the overlay's own position), so slot isolation holds.

**Target positions (`execution/algos.py`).** `m.target(qty, style=…, **opts)` (in a group,
`m.target(ticker, qty, …)`) declares the position the slot wants on a market; a new target replaces the old
(its clock restarts), `m.target(0)` flattens. The router keeps a `TargetWorker` per slot and market and steps it
— at most once per event, never while the slot is warming, halted, or failed — on that market's book changes and
prints, the slot's own fills, order updates, and rejects, stream resets, and timers. The worker sends ordinary
`quote` / `take` intents, so risk, the order scheduler, and the ledger see nothing new (the `Target` intent
itself passes through for viewers and the test kit; the order manager ignores it):
- `style="passive"` rests what's left at others' best price on our side (in live mode our own resting size is
  taken out of the venue's book first, so the worker never chases itself); `style="improve"` sits
  `improve_ticks` (default 1) inside that, never touching the other side. No requote while price and size hold.
- `cross_after_s`: from then on, take what's left by IOC at the best opposite price — or, with `max_cross_c`, at
  the worst price whose all-in cost (price ± the series' taker fee) is within that many cents of the mid; at most
  one take a second, none while one is in flight. `expires_s`: pull and stop.
- It waits, pulling resting orders where that's safer, while the slot is throttled, its market has no book, one
  of the slot's own risk `windows` is open, and for 5 s after a risk reject (so a blocked target isn't resent on
  every event). Progress is the slot's `last_action`: `target +20, have +12, resting 8 @ 54¢`.
- Don't also quote or take by hand on a market the slot targets.

**Checkpoints.** Learned or adaptive strategies implement `on_save() -> bytes` and
`on_load(blob)`; the engine persists each slot's blob with the run id every 60 s and at
shutdown and restores it on restart (warm-up still applies). Checkpoints are tagged with the
strategy version and ignored if the version changed.

### Advisors — `advisors/`

Slow or non-deterministic reasoning — LLM agents, external model services, anything that
takes more than a millisecond or can't be replayed — lives outside the engine loop.

```python
class Advisor(Protocol):                        # @advisor("kind"), built as cls(name, params, env)
    name: str                                   # instance name from config
    async def run(self, view: StateView, sink: AdviceSink, stop: asyncio.Event) -> None
    # backtestable only with: deterministic = True; trained_until = "YYYY-MM-DD"   (backtest_refusal)

class StateView(Protocol):                      # read-only, eventually consistent, may lag the engine
    def book(self, ticker: str) -> OrderBook | None
    def data(self, entity: str) -> EntityState | None    # as the domain renders it (EntityView: summary, fields)
    def forecast(self, ticker: str, model: str) -> Forecast | None
    def position(self, slot: str) -> Qty
    def tickers(self, *, series: str | None = None) -> list[str]   # open markets

class AdviceSink(Protocol):
    def advise(self, a: Advice) -> None         # recorded raw first, then queued into the engine

class AdvisorParams(Struct, kw_only):           # budgets an advisor's Params inherit, enforced by the worker
    timeout_s: float = 30; max_calls_per_min: int | None = None; max_cost_per_day: str | None = None  # "5.00", UTC day
class AdvisorEnv:                               # what the worker hands an advisor
    async def call(self, aw, *, what="call")    # its outside calls: time out, count, wait out a pause
    def charge(self, cost: str | Micros)        # report a call's cost; past max_cost_per_day: paused to the next day
    def now_ms(self) -> int; log; note(level, text)    # engine time (wall live, the engine clock in a backtest)
class AdvisorSpec(Struct): name; kind; params: dict = {}             # one [[advisor]]
def build_advisor(spec, env_for) -> (advisor, env); def backtest_refusal(cls, start: date) -> str | None

# advisors/worker.py
class AdvisorRunner(specs, view, out)           # each advisor its own task; raises → logged, restarted (1 s doubling
                                                #   to 60 s); returns → done; an advisor speaks only under its own name
class AdvisorLink(specs, sink, feed: Path, *, clock)   # the engine side; `sink` = engine.source("advisors")
    async def run(stop)                         # start/supervise `python -m pmx.advisors.worker` over a socketpair
    def handle(line: bytes)                     # `advice` → sink.record("advisors", "e", …) then sink.emit;
                                                #   `log` → a LogEvent; anything else → dropped + logged
```

- The worker runs at lower OS priority (`nice 10`), builds a `FeedView` from the viewer feed, and starts its
  advisors once the feed's full state has arrived (`synced`); it exits when the engine goes away.
- **Validation is the engine's**, as it dispatches (so live and replay agree): `State` refuses advice whose
  target is no known contract or entity, with neither `p` nor a direction, `p` or `confidence` outside
  [0, 1], an unknown direction, or a rationale over 200 characters (`events.py:advice_problem`). A refused
  event (`Refused`) goes no further — not routed, not published — and becomes a warning `LogEvent` from
  `advisor:<name>`. Advice already lapsed on arrival withdraws that advisor's advice for the target.
- Budget pauses and advisor failures are `LogEvent`s and raise alerts.

- Advisors run in a worker process (`pmx run` starts it), fed by the same one-way stream as
  the TUI; `advise` sends `Advice` back to the engine over a socket.
- **Advisors never place orders** (invariant 14). A strategy lists the advisors it uses
  (`advisors = ["news"]`), receives their advice via `on_advice` / `m.advice()`, and decides;
  risk applies as to any order. Expired advice is ignored; no advice = no signal.
- An advisor's own I/O (web, news, model APIs) has timeouts, retries, and cost/rate budgets
  in its `Params`. Outside text is untrusted (prompt injection): advice is bounded to `p`,
  `direction`, and `confidence` — never instructions the engine follows.
- **Backtests replay recorded advice only.** Running a hosted LLM over historical events isn't
  a valid backtest — its training data may contain the outcome, and its weights change — so
  hosted advisors are judged on forward paper trading. A
  *local* model with pinned weights, temperature 0, and a declared `trained_until` may run
  in backtests on events after that date only (`deterministic = true`); the backtest refuses
  earlier windows.
- **You can be an advisor too.** `pmx advise TICKER --p 0.62 --expires 30m` sends
  `Advice(advisor="manual", …)`: you set the fair, a strategy listing `"manual"` executes it.
- Advisor workers run at lower OS priority than the engine; a local LLM must not starve it.

### Portfolio — `portfolio/`

```python
class PortfolioTracker:                         # portfolio/tracker.py: the engine's `portfolio` step (2b)
    def __init__(self, *, slots=router.slots, outcomes: OutcomeMap | None, ledger: Ledger | None, risk=None,
                 tz="America/New_York")
    def start(self, now_ms) -> [(scope, mode, [(ts_ms, equity)])]   # restart: positions + today's opening
                                                #   equity from the ledger; the last day of equity for viewers
    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]   # PortfolioSnapshot ≤ 1/s on Timers
    def stats(self, scope: str, mode: str) -> ScopeStats   # scope: "portfolio" | "strategy:<id>" | "slot:<id>"
    def exposures(self, mode: str, strategy: str | None = None) -> list[Exposure]   # entities held, worst first
    def position(self, slot: str) -> Qty
    def what_if(self, slot: str, req: OrderRequest, state: State) -> ScopeStats  # risk pre-check: the portfolio
                                                #   if `req` filled in full at its limit (fees aside)
def snapshot(now_ms) -> PortfolioSnapshot       # every scope, exposure, and limit usage (portfolio/metrics.py)

# portfolio/scenarios.py
class OutcomeMap(state)                         # entity -> {outcome: {ticker: leg}}; also the router's `outcomes`
    def of(ticker) -> (entity, outcomes)        # the entity it nets in; a market no domain covers: itself, yes/no
# plugins.py: @outcomes("<linker kind>") fn(entity, contracts) -> {outcome: {ticker: leg}}
#   leg = "yes" ($1) | "no" ($0) | the YES settlement price (an NFL tie pays $0.50 on both teams)
# sports/nfl/outcomes.py: outcomes named by team code plus "tie"
```

Exact definitions (all integer micros; `q` signed YES qty, prices in `Price`). Marks are read lazily from
`State` books when a number is asked for — the tracker does nothing per market-data event:
- **Average-cost accounting per slot** (`core/position.py:AvgCost`, the order manager's arithmetic): buys/sells
  that increase |q| update the average YES cost; ones that reduce |q| realize `(price − avg) × reduced_qty ×
  sign`. Settlement realizes at the contract's settlement price (1.00, 0.00, 0.50 for a tie). Fees are tracked
  separately and always subtracted.
- **Unrealized** = `q × (mid − avg)`. With no live book, the last mid seen is used (else the average price) and
  the scope is flagged stale.
- **Capital at risk** for a position = the most it can still lose at settlement from here: long YES `q × mid`,
  long NO `|q| × (1 − mid)` (bounded binary payoffs). Summed per scope.
- **Scenario PnL** per entity with an open position: for each of the domain's outcomes, realized − fees + Σ (q ×
  the leg's settlement price − cost) over the entity's positions in scope. Tickers no domain covers are their own
  entity with outcomes yes/no (every Kalshi market here settles binary; a domain that settles otherwise
  declares it in its legs).
- **Worst case** = Σ over entities of the minimum scenario (entities treated as independent).
- **EV under model / market** = Σ over entities of Σ_outcome P(outcome) × scenario PnL. P(outcome) from per-market
  P(YES) — the model's forecasts, or book mids: an outcome with legs settling at $1 gets the smallest of their
  P(YES), outcomes without one share the rest, then normalized. The model is the scope's strategy's; at portfolio
  scope, per entity, the model of the slot holding the most of it. Entities without that model's forecasts count
  at the market's odds; `ev_model` is None when no entity has any. **Stdev under model** likewise, entities
  independent.
- **Equity** = realized + unrealized − fees since inception for that mode; **today** = equity change since local
  midnight (the opening equity survives restarts via the ledger's `equity` rows); **hwm** = today's high-water
  mark; **drawdown** = hwm − equity. Daily, so a `max_drawdown` halt clears with the day.
- **Working exposure** = extra capital at risk if the working orders in scope filled: per market, whichever of
  all its bids or all its asks filling would add more. Risk's `max_capital_at_risk` counts it (working orders
  included).
- **Ledger** (`equity`, `fill_meta`): an equity row per scope and mode every 10 s of engine time when it changed,
  and for the fill's slot, strategy, and portfolio on every fill. A `fill_meta` row per fill: slot, entity, the
  slot's model and its forecast's `version`, fair, mid, spread, the entity state's `phase` (a domain attribute;
  NFL: pre / Q1–Q4 / half / OT / final); markouts = signed qty × (mid − fill price) at +10 s / +1 m / +5 m of
  engine time, filled in as that time passes (identical in backtests).
- **Risk** (`PortfolioRisk`): `max_capital_at_risk` and `max_worst_case` against `what_if` at global scope,
  `max_entity_risk` against `exposures`; `max_drawdown` rejects new risk and, on `Timer`s, halts its scope.
- `pmx risk [--mode paper|live]` prints the running engine's latest snapshot from its feed: scopes, exposures with
  scenario PnL, limit usage (slots only when breached).

### Execution — `execution/`

```python
class Broker(Protocol):                         # live: talks to the venue asynchronously
    mode: Literal["paper", "live"]
    async def place(self, req: OrderRequest) -> OrderAck
    async def cancel(self, order_id: str, ticker: str) -> Qty
    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]
class SyncBroker(Protocol):                     # paper: executes on engine time, deterministic
    def place_now(self, req, slot, state, now_ms) -> list[Event]   # fills + order updates as events
    def cancel_now(self, client_order_id, now_ms) -> list[Event]
    def queue_ahead(self, client_order_id) -> Qty | None
    def on_event(self, ev, state, now_ms) -> list[Event]           # the engine's broker step: resting fills

class OrderManager:                             # execution/orders.py: the engine's order_manager step
    def __init__(self, *, intents=router.take_intents, slots=router.slots, risk: RiskManager | None,
                 brokers: Mapping[mode, SyncBroker], scheduler: OrderScheduler, ledger: Ledger | None,
                 instance: str, paper_cash: Micros, attribution: Mapping[strategy_id, Attribution])
    def start(self, now_ms) -> list[Event]     # restart: cancel open paper orders, rebuild positions from fills
    def cancel_scope(self, scope, ticker, why) # risk's on_cancel hook
    # OrderView (router), Positions (risk), Balance: position(scope, ticker, mode=None) · working(scope,
    # ticker=None, mode=None) · throttled(slot) · pnl(scope, mode, state) · available(mode)
class OrderScheduler(rate=100, burst=100, cost=10, take_ttl_ms=1_000)   # execution/scheduler.py
    submit(Write) · drop(client_order_id) · due(now_ms) -> (send, expired) · rate_limited(now_ms) · throttled(slot)
class PaperBroker                               # execution/paper.py
class LiveBroker(venue, *, instance, emit, clock, ledger=None, live=(), positions=…)   # execution/live.py
    # a SyncBroker: place_now / cancel_now start the venue call and return []; results arrive as events via emit
    async def reconcile(why="") -> None         # start, every account-stream reconnect, unknown outcomes
    def reconcile_soon() · working() -> [LiveOrder] · async drain(timeout)
class Brokers(*brokers)                         # execution/broker.py: the engine's broker step over paper + live
ADOPTED = "adopted from an earlier run"         # reason on an `open` live update for an earlier run's order

class RiskLimits(msgspec.Struct, frozen=True):  # all optional; tightest of slot/strategy/global wins
    max_position: Qty | None = None             # |signed YES| per slot, counting working orders
    max_order_qty: Qty | None = None
    max_open_orders: int | None = None
    max_loss: Micros | None = None              # realized + marked, per day
    price_band: Price | None = None             # max distance from fair (or mid)
    stale_book_ms: int | None = None
    stale_forecast_ms: int | None = None
    stale_input_ms: int | None = None           # any declared input older than this blocks new risk
    windows: list[Window] = []                  # schedule windows: no new risk, quotes pulled
    # portfolio-level (meaningful at strategy and global scope; from the portfolio tracker)
    max_capital_at_risk: Micros | None = None   # including working orders
    max_worst_case: Micros | None = None        # most negative scenario sum allowed
    max_entity_risk: Micros | None = None       # capital at risk in any single entity
    max_drawdown: Micros | None = None          # from the high-water mark, per day

class Window(msgspec.Struct, frozen=True):      # e.g. { kind = "release", before_s = 60, after_s = 10 }
    kind: str                                   # Schedule.kind; "resolution" uses each contract's resolution time
    before_s: int
    after_s: int
    action: Literal["pull", "block_new"] = "pull"   # pull = cancel resting quotes too

class RiskManager:                              # execution/risk.py
    def __init__(self, *, positions: Positions, slots: Mapping[str, SlotInfo] = {}, global_limits=None,
                 strategy_limits: Mapping[str, RiskLimits | dict] = {}, balance: Balance | None = None,
                 portfolio: PortfolioRisk | None = None, venue_limits: Mapping[MarketId, Qty] = {},
                 halt_file: Path | None = None, tz="America/New_York")
    def check(self, slot: str, req: OrderRequest, state: State, now_ms: int, *, mode=None) -> RiskReject | None
    def halt(self, reason: str, scope: str | None = None, now_ms=0) -> None   # slot id, strategy id, or None = all
    def resume(self, scope: str | None = None, now_ms=0) -> None
    def on_event(self, ev, state, now_ms) -> list[Event]   # Timers: kill switch, loss watch, day roll, window pulls;
                                                           # Halt events from elsewhere; returns Halt + LogEvents
    on_cancel: list[Callable[[scope, ticker | None, reason], None]]   # the order manager cancels in scope
    halts: dict[str, str]                                  # scope ("" = everything) -> reason
RiskLimits.of(table) -> RiskLimits                       # config: money/qty as strings ("25.00"; whole ints ok)
class Positions(Protocol):                       # the order manager; scope = slot id, strategy id, or ""
    def position(self, scope, ticker, mode=None) -> Qty; def working(self, scope, ticker=None, mode=None) -> list[WorkingOrder]
    def pnl(self, scope, mode, state) -> Micros  # realized + marked to mid − fees, since inception
class Balance(Protocol): def available(self, mode) -> Micros          # cash not tied up as collateral
class PortfolioRisk(Protocol): what_if(slot, req, state) · stats(scope, mode) · exposures(mode, strategy=None)   # the portfolio tracker
class SlotInfo(Protocol): id; mode; model; inputs; risk_limits; tickers; warming   # engine/slots.py:Slot
```

Rules, in order (the first broken one rejects): `halted` (slot, its strategy, or everything) →
`market_not_open` → then, for orders that add risk only: `warming` → `window` → `stale_book` (no book,
or none newer than `stale_book_ms`) → `stale_forecast` (the slot's model) → `stale_input` (any declared
input) → for every order: `price_band` (vs the slot model's fair, else the mid) → `max_order_qty` → for
orders that add risk: `max_open_orders` → `max_position` → `max_loss` (each at slot, strategy, and global
scope, against that scope's own numbers; a loss breach also halts the scope) → `venue_limit` →
portfolio limits (`max_capital_at_risk`, `max_worst_case` via `what_if` at global scope; `max_drawdown`;
`max_entity_risk`) → `insufficient_balance` (buying YES ties up price × qty, selling YES (1 − price) × qty).
An order *reduces* risk when it shrinks |position| net of same-side working orders without flipping it.
Per-order limits merge to the tightest of slot / strategy / global; windows from all three apply.
Today's loss is the portfolio tracker's `today` when there is one, else the change since risk first looked that local day.

Order requests carry `strategy = "<strategy_id>/<ticker>"` (the slot) so the ledger
attributes every order and fill to its slot. Client order ids are `pmx-<instance>-<slot hash>-<seq>`.

The order manager drains the router's intents on every event: a `Quote` side is diffed against the
slot's working quote (same price and remaining size → nothing; else cancel + place post-only GTC; a
still-queued order is just dropped, which is how a newer target replaces an unsent one); a `Take` is
one IOC order; `CancelAll` cancels. A new order that would cross one of our resting orders on that
market in any slot is rejected (`self_cross`, never sent; IOC takes don't count, they never rest), then
risk checks it, then the scheduler queues it. Writes are released on engine time as the token bucket
allows — every event, so a quiet session releases them on the next `Timer`. Fills and order updates
come back as events and are recorded in the ledger with attribution. A market that pauses or closes
has its resting orders canceled; one that *settles* (not merely determines) settles every position in
it at the contract's settlement price. The paper balance is starting cash + trade and settlement
cash − fees − $1 per short contract held − what resting orders tie up.

**Live.** The live broker has the paper broker's interface, so the order manager doesn't know which it talks
to. A write released by the scheduler starts a venue call; the order stays `pending` until the venue answers, and
acks, cancels, the account stream's `VenueOrder` updates and our `Fill`s come back as engine events (`OrderUpdate`,
`SlotFill`), attributed by client order id, else the venue's order id (this run's, or an earlier run's from the
ledger); a fill nobody here placed is logged loudly and not booked. Two conventions: a broker's `pending` update means
the outcome is unknown (5xx, timeout) — that slot gets no new orders (`unresolved`) until reconciliation resolves it —
and an `open` update with reason `ADOPTED` is an earlier run's resting order, which the order manager takes on as a
working quote. Reconciliation: the venue's resting orders (this instance's synced, adopted when their strategy is live
here, else canceled; other pmx instances' warned about and left; anyone else's never touched), unknown or unobserved
orders resolved, fills since the ledger's last, then balance + positions. Positions are cross-checked on every
`AccountSnapshot`; the live balance is the venue's minus the collateral of orders sent since it. Kalshi offers no
cancel-on-disconnect, so every live run starts `pmx guard` (`ops/guard.py`: the engine's
heartbeat file + control socket; it cancels this instance's resting orders when the engine dies or hangs); orders
are sent with `cancel_order_on_pause` and `taker_at_cross` self-trade prevention.

#### Order scheduler — `execution/scheduler.py`

Venue write limits are shared by every slot — Kalshi's basic tier is ~10 writes/s (100
tokens/s, 10 per request; `connectors/kalshi/rest.py`). Every write goes through one
scheduler between risk and the broker:

- **Priority:** cancels → risk-reducing orders (shrink |position|) → new risk.
- **Fairness:** round robin across slots within a priority, so one busy slot can't starve
  the rest.
- **Coalescing:** a slot's newer `quote` target replaces its queued, unsent one; IOC takes are
  never merged and expire after `take_ttl_ms` (default 1,000) if still queued.
- **Feedback:** queued writes emit `OrderUpdate(status="queued")`; `m.throttled` is true while
  a slot has any; a 429 shrinks the bucket and backs off. Sustained throttling is logged
  and alerted.
- **Paper parity:** the paper broker sits behind the same scheduler with the same limits, so
  paper results include rate-limit effects.

### Backtests — `backtest/`

```python
class Runner(cfg: RunConfig, *, start: date, end: date, run_dir: Path, root=data/, sources=None, run_id=None,
             env="prod")
    def step(self, n=1) -> int                  # dispatch up to n recorded inputs (and the Timers before each)
    def run(self) -> Summary; def finish(self) -> Summary; def reset(self) -> None
def run(cfg, *, start, end, run_dir, **kw) -> Summary
def walk_forward(configs: {label: RunConfig}, *, train: (date, date), evaluate: (date, date), out_dir) -> rows
class Summary: run_id, start, end, inputs, timers, sources {raw: events}, first_ms, last_ms, clamped, orders, fills,
               strategies {id: StrategyResult(equity, realized, unrealized, fees, orders, fills)}, wall_s, inputs_per_s
def sources_for(cfg, root, start, end, *, only=None) -> [RawLogSource]   # kalshi (+ kalshi_rest), [[source]]s, advisors
```

- **The same core.** `engine/run.py:Core` — state, models, slots, risk, order manager, paper broker, scheduler,
  portfolio tracker — is what `pmx run` builds; the runner builds it on a `SimClock` with models inline (their
  `latency_ms` as delay), slots' CPU budgets off (wall time would make results machine-dependent), no kill-switch
  file, instance `bt`, and its own ledger `run_dir/ledger.sqlite` (plus `config.toml`, `summary.json`).
- **Order of inputs.** Sources merge by `(receive ms, source order, record order)`; before each input, a `Timer` for
  every whole second of simulated time since the last one; inputs go straight to `Engine.dispatch` with their
  engine source name (`kalshi`, `espn`, `advisors`). A record whose receive time is before the engine's (a wall
  clock step while recording) is dispatched at the engine's time and counted (`clamped`).
- **Paper only.** Live strategies run as paper (logged). Advisors aren't run again — their recorded advice replays;
  models run again, so their recorded outputs aren't replayed.
- **Determinism (invariant 6).** Same raw logs + config + code ⇒ a byte-identical ledger: run ids default to
  `bt-<from>-<to>-<config hash>`, client order ids are `pmx-bt-…`, every timestamp is engine time, and nothing
  depends on hash-set order (tested with another `PYTHONHASHSEED`).
- **Candle fidelity.** `NflHistory` rebuilds NFL games from Kalshi's 1-minute candles and nflverse plays
  — synthetic prints at each minute's low and high, a one-level book at the closing bid/ask, game states `latency_s`
  after nflverse says they were known — through the same runner (`history=`), labeled `fidelity = "candle"`.
- **Reports.** `backtest/report.py` formats the analytics stats per strategy and slot (`report.md`, `report.json`),
  compares runs (or a run and the paper ledger over days), and `pmx backtest view` serves a run to the TUI.
- **Contracts from before the first day.** Venue market data starts with each ticker's newest metadata from the days
  before `start` that is still `open` (newest day first, back to a day with a whole-universe listing — the recorder
  writes one at start and each new UTC day — or 14 days), at the first record's time.
- **Starting mid-day.** `Runner(start_ms=, end_ms=)` (`--from` / `--to` given as instants) replays a live session
  that started inside a day: records before `start_ms` only build state (books, trades, entity states — no
  strategies, models, advice, or orders) and their contracts are held, then announced at `start_ms`, as a live
  engine started then would discover its markets; Timers run until `end_ms` even after the last record.
- `pmx backtest --config … --from YYYY-MM-DD[THH:MM:SSZ] [--to …] [--sources kalshi,espn] [--run-id …]` →
  `data/backtests/<run id>/`.

### Analytics — `analytics/`

```python
# analytics/stats.py — any ledger with .db and .env (Ledger, or ReadOnly(path, env) beside a running engine)
def load(ledger, *, mode, strategy=None, slot=None, start_ms=None, end_ms=None) -> Data   # episodes, totals, equity
def compute(data: Data) -> PerfStats          # strategy_stats(...) = compute(load(...))
def bootstrap(episodes) -> (pnl_ci, cpc_ci, p_nonpositive) | None   # entities resampled, 2,000×, seeded
# analytics/breakdowns.py, verdict.py, publish.py
def breakdowns(data: Data) -> dict[str, list[BucketRow]]   # market, series, entity, phase, edge, day
def verdict(stats, rows) -> Verdict           # profitable | unprofitable | inconclusive (< 10 entities) + why + flags
class PerfPublisher(ledger_path, env, strategies, send, every_s=30)   # `perf` messages, off the dispatch path
```

`PerfStats` fields: net/gross PnL, fees, contracts, fills, episodes (flat →
flat or settlement), win rate, avg win/loss, profit factor, expectancy ¢/contract with a
95% CI (bootstrap clustered by entity), edge at entry vs realized edge, markouts at
+10 s/+1 m/+5 m, maker share, fill rate, max drawdown, time under water, daily PnL series,
per-day Sharpe (not annualized) with the day count, Brier of fair-at-entry vs market-at-entry.

### Ledger tables

```sql
equity(ts_ms, mode, env, scope, equity, realized, unrealized, fees, capital_at_risk)   -- 10 s + fills
fill_meta(venue, mode, env, fill_id, ts_ms, slot, entity, model, model_version, fair, mid, spread, phase,
          markout_10s, markout_1m, markout_5m)                                    -- markouts filled later
runs(run_id, env, instance, started_ms, ended_ms, git_sha, dirty, config_hash, config_toml)   -- per `pmx run`
checkpoints(env, slot, strategy_version, run_id, ts_ms, blob)                     -- latest on_save per slot
-- orders and fills also carry: env, run_id, strategy_version (git sha, "+dirty" if uncommitted), params_hash
settled(venue, mode, env, ticker, price, ts_ms)                                    -- any settlement price
```

Every order and fill is attributable to the exact strategy code and parameters that made it
(invariant 15), so performance can be split by strategy version as strategies evolve.

### Config — `config/strategies.toml`

```python
# engine/config.py
def load(path=None, *, text=None) -> RunConfig     # ConfigError naming the file and the problem; no network
class RunConfig: engine; watch; risk: RiskLimits; manual: RiskLimits; paper_cash: Micros; sources: [SourceConfig]
    models: [ModelSpec]; advisors: [AdvisorSpec]; strategies: [StrategySpec]   # each `risk` decoded to RiskLimits
    hash: str                       # of the parsed config (formatting and comments don't change it)
    params_hash: {strategy id: str} # of the decoded params (an explicit default hashes like the default)
    live -> [strategy id]; universe() -> (series, tickers)   # every selector ∪ [watch]; events via their series
    without_strategies(); watching(series, tickers); with_models(kinds)      # `pmx serve`, --series, --models
def plan(cfg, *, env=None) -> str                   # `pmx config check`
BUILTIN_SOURCES = kalshi, timer, account, advisors  # names `pmx run` gives its own sources (valid `inputs`)

# engine/run.py
class System(cfg, *, env, sock, venue=None, data=None, live=False, record=None, source_mode="auto", market=None)
    async def run(stop)             # serve until stop, then the shutdown sequence below
class StartRefused(Exception)       # live without both switches, without a key, without pmx guard, or max_loss
class RunLog                        # the engine's publisher: the tap, plus logs/fills/rejects/source changes → logging
```

`load` checks, besides each plugin's params (`build_params`) and risk tables (`RiskLimits.of`): unique source,
model, advisor, and strategy names (a `[[source]]` can't take a built-in name; `manual` is reserved for `pmx advise`);
a strategy's `model` is a `[[model]]`, its `inputs` are sources, its `advisors` are `[[advisor]]`s or `manual`; its
`markets` select something; `mode = "live"` requires a global `max_loss`; and no value looks like a secret (PEM
keys, auth headers, key-id UUIDs, keys named like `api_key` / `token` / `secret`). `[engine]`: `venue`, `env`
(market data; `--env` wins), `record`, `paper_cash`, `checkpoint_s` (60), `warm_from_recording`. A `[[source]]`
takes `queue = { max, policy }`. `[manual] risk` becomes the risk limits of strategy id `manual` (orders placed by hand).

**Wiring** (`System`): state with every linker; `Router(strategies)`; `OrderManager(paper broker, scheduler,
ledger, instance, paper_cash, attribution)` with `RiskManager(global [risk], strategy limits {manual},
halt_file = data/HALT)`; models via `build_model` (process models attached to their own source sink); the
`kalshi` market source (`policy = "reset"`, re-snapshots on a sleep gap), `timer`, `account` with an API key, every
`[[source]]` with its queue, `AdvisorLink` when there are `[[advisor]]`s; `EngineLink` + `Tap` on the socket;
`RecordingSink(data/)` when recording. Each run writes a `runs` row and gets `Attribution(run_id, strategy version,
params hash)` per strategy; the strategy version is the git sha, plus `+dirty.<hash of the strategy's source
file>` with uncommitted changes, so editing a strategy changes its version. The engine instance id lives in
`data/instance`; client order ids carry on from the ledger's highest sequence for that instance, and paper fill
ids extend their client order id, so a restart never reuses either.

**Shutdown** (SIGINT/SIGTERM/SIGHUP, `pmx down`): stop dispatching new inputs → save checkpoints → halt
everything → each slot's `on_stop` + cancel-all → dispatch until no working orders remain (≤ `shutdown_s`, 2 s;
paper orders still queued for write capacity are then canceled in the ledger) → stop sources, the feed process,
and workers (≤ 10 s) → close process models, the recorder, and the ledger (`runs.ended_ms`) → remove the socket.
**Checkpoints**: `router.checkpoint()` every `checkpoint_s` and at shutdown → ledger `checkpoints` (latest per slot,
with its strategy version); at start, blobs whose version matches are handed to `router.restore`.

**Kill-switch file** (`data/HALT`, `execution/risk.py`: `read_halts`, `write_halt`, `clear_halt`): one JSON line per
halt, `{"scope": "<slot id> | <strategy id> | \"\" (everything)", "reason": …}`; other text halts everything (so
`touch data/HALT` and `echo why > data/HALT` work). Risk compares it with what it last read on every `Timer`:
new scopes are halted (orders canceled), removed ones resumed.

**Logs**: `data/logs/run-<date>.log` and stderr, written by a listener thread so the engine never waits on a log
write; engine `LogEvent`s (at most 20 a second per source), fills, rejects (once a minute per slot and rule),
broker-rejected orders, and sources going up or down are in it.

```toml
[engine]
venue = "kalshi"
env = "prod"                        # market-data environment
record = true                       # raw-record every source while running

[watch]                             # markets to stream for the TUI even if nothing trades them
series = ["KXNFLGAME"]

[risk]                              # global (portfolio) limits
max_loss = "250.00"
max_open_orders = 60
max_capital_at_risk = "1000.00"
max_worst_case = "400.00"
max_entity_risk = "150.00"
max_drawdown = "300.00"

[[source]]
name = "espn"
kind = "espn_nfl"
params = { live_poll_s = 5 }

# [[source]]                        # an external feed a strategy can read (example)
# name = "coinbase_btc"             # what strategies list in `inputs`
# kind = "coinbase_trades"
# queue = { max = 10000, policy = "conflate" }
# params = { product = "BTC-USD", emit_ms = 100 }    # aggregates ticks into 100 ms bars

# [[advisor]]                       # an AI agent (example); never places orders
# name = "news"
# kind = "llm_news"
# params = { timeout_s = 20, max_cost_per_day = "5.00", expire_s = 300 }

[[model]]
name = "espn"                       # instance name; strategies and the TUI refer to this
kind = "espn_wp"                    # pass-through of ESPN's win probability

[[model]]
name = "mid"                        # a second model running alongside, for comparison
kind = "market_mid"

# [[model]]                         # a trained model (needs its artifact)
# name = "wp"
# kind = "nfl_wp"
# params = { artifact = "data/models/nfl_wp.pkl" }

[manual]                            # limits for orders placed by hand (`pmx order`)
risk = { max_position = "20", max_order_qty = "10", max_loss = "10.00" }

[[strategy]]                        # reference strategy shipped for system tests
id = "smoke"
kind = "scripted"
mode = "paper"
markets = { series = ["KXNFLGAME"], linker = "nfl_game" }
params = { script = "config/scripts/smoke.toml" }
risk = { max_position = "5", max_loss = "5.00" }

[[strategy]]                        # a custom strategy (docs/GUIDE.md) — example
id = "nfl-edge"
kind = "edge_taker"
mode = "paper"
model = "espn"
markets = { series = ["KXNFLGAME"], linker = "nfl_game" }   # one slot per matching market
params = { min_edge = 0.04, size = "5", max_spread = "0.03" }
risk = { max_position = "50", max_order_qty = "10", max_loss = "25.00",   # per slot
         stale_book_ms = 5000, stale_forecast_ms = 60000 }

# [[strategy]]                      # more options (example)
# id = "btc-threshold"
# kind = "spot_vs_threshold"
# markets = { series = ["KXBTCD"] }
# inputs = ["coinbase_btc"]         # on_input / m.input("coinbase_btc")
# advisors = ["news"]               # on_advice / m.advice("news")
# cadence_ms = 250                  # book/trade/input hooks at most 4×/s
# warmup_s = 300                    # no orders for the first 5 minutes
# slot_by = "market"                # or "event" / "entity" for one slot over related markets
# cpu_budget_ms = 50
# risk = { max_position = "20", stale_input_ms = 2000 }

```

### Strategy test kit — `testing/`

(`edge_taker` is the example strategy from GUIDE.md.)

```python
sc = Scenario("edge_taker", params={"min_edge": 0.04, "size": "5"}, ticker="KXNFLGAME-26OCT18HOUJAC-HOU")
sc.book(bids=[("0.55", "100")], asks=[("0.57", "100")])
sc.forecast(0.63)
assert sc.intents == [Take("bid", limit="0.57", qty="5")]
sc.fill("bid", "0.57", "5")
sc.forecast(0.64)
assert sc.intents == []                        # max position reached / cooldown
```

Runs one slot with the real `MarketContext`, a fake clock, and a recording broker — no
engine, no network. Prices/qty in tests are strings (dollars / contracts) for readability.

### Feed protocol — `feed/proto.py` ⇄ `tui/src/proto.rs` (v3)

NDJSON over `data/pmx.sock` (`data/pmx-demo.sock` for `--demo`), tagged by `t`, from the feed
process. `tests/fixtures/feed/v3.ndjson` is what the real publisher sends over a scripted session
(`make_v3.py` regenerates it; pytest checks it still matches) and cargo test parses it; the v2
fixture must still parse. v3 conflates market data — no `delta` or `forecast` messages; a v2
viewer still gets every `book` (and ignores `top`).

| Tag | Shape | Notes |
|---|---|---|
| `hello` | `venue, env, source, version=3, run` | `source`: live · poll · replay · engine · backtest · demo. A `hello` mid-stream (a resync) resets the viewer |
| `synced` | — | the full state after a `hello` has all been sent; live updates follow |
| `status` | `connected, detail, ts_ms, source, dropped` | one per data source; `source=""` is the venue, `"engine"` the engine's link (`dropped` = events that never reached the feed) |
| `top` | `ticker, bid, ask, last, fairs, position, ts_ms` | conflated with `book`: ≤ 4/s per changed market, ≤ 10/s with slots or working orders; `bid`/`ask` `[price, qty]`, `last` `[price, qty, taker_side]`, `fairs` model → p, `position` mode → qty (nonzero, summed over slots) |
| `book` | `ticker, bids, asks, ts_ms` | top 10 levels per side, sent only when changed, conflated as `top` |
| `contract` `stale` `reset` `trade` `account` | v1 | `stale`/`reset` at once; `delta` is v2 only |
| `fill` | v1 + `slot, mode` | published from `SlotFill`; raw venue fills aren't forwarded |
| `entity` | `key, kind, tickers, summary, fields, ts_ms` | any domain; viewers show `summary`/`fields` verbatim |
| `forecast` | `model, ticker, p, ts_ms, data_ts_ms` | v2 only; v3 sends fairs in `top` |
| `advice` | `Advice` (advisor, target, p, direction, confidence, expires_ms, rationale, ts_ms) | never conflated; unexpired ones kept for late viewers; `expires_ms < ts_ms` withdraws |
| `models` | `names` | configured model instances, in config order |
| `slot` | `SlotStatus` | ≤ 1/s per slot |
| `order` | `OrderUpdate` | working orders kept for late viewers |
| `risk` · `log` · `halt` | `RiskReject` · `LogEvent` · `Halt` | |
| `portfolio` | `PortfolioSnapshot` | ≤ 1/s; each send appends an `equity` point per portfolio/strategy scope |
| `equity` | `scope, mode, points: [[ts_ms, equity, drawdown]], replace` | history (`replace=true`) on connect, ≤ 2,000 points, thinned by age |
| `perf` | `strategy, mode, range, ts_ms, stats: PerfStats, breakdowns, verdict` | `PerfStats` wire shape in `feed/proto.py` |

`feed/publisher.py` maps engine events to messages (dispatch step 6) and conflates by engine
time (deadline heaps: a flush costs nothing when nothing is due); in the feed process a 50 ms
tick flushes on estimated engine time when the engine is quiet. New viewers get full state
first; unknown tags and missing fields are tolerated both ways. `pmx serve --demo`
(`feed/demo.py`) runs an invented session through the tap and the real feed process so the TUI
can be explored without a venue — every number is fake and badged DEMO; with `--markets N
--rate R [--slots S]` it is the load generator instead.

### Control channel — `control/`

NDJSON request/response over `data/pmx-ctl.sock` (0600), separate from the read-only feed.
Requests: `order`, `cancel`, `halt`, `resume`, `status`, `positions`, `advise` (manual
advice: target, p or direction, confidence, expiry). Requests become engine
events and pass through the same queue, order manager, risk, broker, and ledger as strategy
intents. Manual orders run as slots `manual/<ticker>` with `[manual]` risk limits. Live
manual orders need `pmx run --live`, `mode: "live"` in the request, and interactive
confirmation. The TUI never uses this channel.

## Data layout (`data/`, git-ignored)

```
raw/<source>/<YYYY-MM-DD>/<HH>-<start_s>.log.zst    recordings: kalshi (websocket), kalshi_rest (REST poll), espn, …
parquet/<source>/...                                research tables from `pmx compact`
nflverse/pbp_<season>.parquet                       trimmed play-by-play (`pmx data nfl`)
kalshi/history/markets.parquet, candles/<ticker>.parquet   settled markets + 1-minute candles (`pmx data kalshi-history`)
models/<name>.pkl, models/<name>.json               trained model artifact + model card
backtests/<run_id>/                                 config, ledger.sqlite, summary.json, report.json, report.md
ledger.sqlite                                       paper + live ledger
logs/                                               process logs (rotated)
health.json                                         per-source/slot health, queue depths, drops, throttling
instance                                            this engine's instance id (client order ids carry it)
HALT                                                the kill-switch file
pmx.sock, pmx.pid                                   feed socket (read-only) + pidfile of the process serving it
pmx-ctl.sock                                        control socket
```

Raw days are kept `[recorder] keep_days` once compacted to parquet. `pmx ops backup` copies `raw/` and the ledger
to another disk daily.

## Failure handling

| Failure | Behavior |
|---|---|
| Kalshi websocket drop / seq gap | reconnect with backoff; books stale until re-snapshot; slots see `StreamReset`; risk rejects on stale books |
| Outside source failure (ESPN) | `SourceStatus(connected=False)`; entity data ages; models stop emitting past staleness; slots pull quotes |
| Model exception | log; disable that model for that entity/ticker; engine continues |
| Slot exception | log; halt that slot only and cancel its orders; other slots continue |
| Order 5xx | order `pending`; reconcile via REST before any new order in that slot |
| Engine crash | live orders persist at Kalshi; on restart reconcile open orders + fills before trading |
| Loss limit | halt at the limit's scope (slot / strategy / global) and cancel its orders |
| Portfolio limit (capital at risk, worst case, entity risk, drawdown) | reject orders that would increase the breach; drawdown breach halts the scope |
| Feed process slow or down | engine keeps trading; the tap ring drops observability events (counted); viewers resync on reconnect |
| Viewer slow | conflation bounds its traffic; if it still falls behind it's dropped and resyncs on reconnect |
| Source floods its queue | its `queue.policy` applies (`conflate` / `drop_oldest` / `reset` → re-snapshot); overflow counted and alerted |
| Declared input stale or down | `m.input()` returns None; risk rejects new risk past `stale_input_ms`; the strategy pulls quotes |
| Advisor slow, failing, or over budget | its advice expires; strategies see no signal; logged; the engine is unaffected (it's another process) |
| Venue write limit reached | writes queue by priority (cancels first) and coalesce; `m.throttled`; sustained throttling alerted |
| Slot over CPU budget | warned after 10 s, halted (`slow`) after 60 s; other slots continue |
| Restart mid-session | slots start `warming`; optional warm-up from recorded data; live orders reconciled first |
| Engine dies or hangs with live orders resting | the watchdog (`pmx guard`) cancels this instance's live orders after `guard_s`; alert |
| Laptop sleeps | no-sleep assertion while running; a detected wall-clock jump is recorded as `sleep_gap`, books re-snapshot, alert |
| Market paused / closed by the venue | resting orders canceled, new orders rejected (`market_not_open`) until it reopens |
| Order would cross our own working order | rejected (`self_cross`); never sent |
| Insufficient balance | rejected before sending (`insufficient_balance`); paper simulates the same |
| Outside API changes its format | source marked unhealthy, raw still recorded for re-parsing, alert |

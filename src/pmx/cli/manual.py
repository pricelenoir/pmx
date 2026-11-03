"""The manual: `pmx help` (an overview by task), `pmx help GUIDE` (a walkthrough), and the examples shown with
`pmx help COMMAND` and `pmx COMMAND -h`. Plain text, man-page style; `tests/test_manual.py` checks every command
has examples and every `pmx …` written here is a real command."""

from __future__ import annotations

OVERVIEW = """\
PMX(1)                                  pmx manual                                  PMX(1)

NAME
    pmx — trade prediction markets (Kalshi): record data, research ideas, paper trade, trade live

FIRST TIME
    pmx help start            install, try the demo, connect your Kalshi account

EVERYDAY
    pmx tui --env prod        the dashboard: markets, portfolio, strategies (q leaves; pmx keeps running)
    pmx status                is the engine running, what's it doing
    pmx positions             open positions
    pmx risk                  money at risk, worst case, limits
    pmx perf --strategy ID    is a strategy making money (with a confidence interval)
    pmx halt / pmx resume     stop all trading now / start again

WHAT DO YOU WANT TO DO?                                       guide
    look at markets                                           pmx help markets
    record market data (for backtests and research)           pmx help record
    run strategies on paper                                   pmx help run
    place or cancel orders by hand, stop trading              pmx help trade
    test a strategy on history                                pmx help backtest
    find an edge (data, pre-registered experiments)           pmx help research
    run pmx on a server and watch it from your laptop         pmx help server
    get alerts, check health, read logs                       pmx help monitor
    go live with real money                                   pmx help live
    something's wrong                                         pmx help troubleshoot

COMMANDS
    pmx help COMMAND          what a command does, its options, examples (same as pmx COMMAND -h)
    pmx help --all            every command's options

FILES
    config/strategies.toml    what the engine runs: sources, models, strategies, risk limits
    config/recorder.toml      what pmx record records
    .env                      Kalshi key IDs, alert webhook, uptime URL (never in config)
    data/                     recordings, the ledger, backtests, logs
    research/                 your private research workspace
"""

GUIDES: dict[str, tuple[str, str]] = {
    "start": ("install, try the demo, connect your Kalshi account", """\
GETTING STARTED

    1. Install (once). You need uv; Rust only for the dashboard.
           uv sync
           ln -s "$PWD/.venv/bin/pmx" ~/.local/bin/pmx
           pmx doctor                     checks the install, keys, clock, disk

    2. Try it with no account: an invented session, nothing real.
           pmx up --demo
           pmx tui --demo                 q leaves the dashboard
           pmx down --demo

    3. Look at real markets. No key needed.
           pmx markets --series KXHIGHNY --env prod
           pmx book TICKER --env prod
           pmx tui --env prod

    4. Connect your Kalshi account. Kalshi → Account → API Keys → Create; it shows a key ID and downloads a
       private key file. Then:
           pmx setkey --env prod          asks for the key ID and the file
           pmx check --env prod           auth, REST, websocket
           pmx account --env prod         your balance and positions
       Or make the key here (the private half never leaves this machine): pmx keygen --env prod, paste the
       public key into Kalshi, then pmx setkey --env prod KEY_ID.

ENVIRONMENTS
    Every command takes --env demo or --env prod. Demo is Kalshi's play-money exchange (its own sign-up and
    keys); prod is real. The default is $KALSHI_ENV, else demo.

NEXT
    pmx help record      start collecting data now: book data can't be downloaded later
    pmx help run         paper trading
    pmx help research    finding an edge
"""),
    "markets": ("look at markets", """\
LOOKING AT MARKETS

    pmx markets --series KXNFLGAME --env prod     a series' open markets with top of book
    pmx markets --event EVENT_TICKER --env prod   one event's markets
    pmx book TICKER --env prod                    one market's full order book
    pmx watch TICKER --env prod                   live top of book (needs a key)
    pmx tui --env prod                            the dashboard on the series in config [watch]
    pmx tui --env prod --series KXHIGHNY          ... on other series

    Prices are YES prices in dollars; a bid buys YES, an ask sells YES (= buys NO).
    The dashboard starts a market-data feed if nothing runs (never a trading engine); quitting it leaves the
    feed running: pmx down stops it.
"""),
    "record": ("record market data for backtests and research", """\
RECORDING

    Order-book history can't be downloaded afterwards: record the markets you might trade, all the time.

    pmx record --env prod                  every series in config/recorder.toml, plus its [[source]]s
    pmx record --env prod --series X Y     only these series
    pmx record --counts                    what was recorded today
    pmx compact                            turn raw logs into tables (data/parquet/)

    With a key the recorder uses the websocket (every book change); without one it polls REST.
    Raw logs go to data/raw/ and are kept [recorder] keep_days (config/recorder.toml).
    To keep it running for good: pmx ops install --record (see pmx help server).
    Don't run pmx record and an engine that records at the same time; run the engine beside it with
        pmx run --env prod --no-record
    (pmx ops install --record --run does that for you).

    History you can download instead (prices, not order books):
    pmx data kalshi --series KXHIGHNY      settled markets + hourly candles (see pmx help research)
"""),
    "run": ("run strategies on paper", """\
RUNNING THE ENGINE (PAPER TRADING)

    config/strategies.toml says what runs: data sources, models (fair prices), strategies, and risk limits.
    A strategy trades paper unless it says mode = "live" (pmx help live).

    pmx config check              validate the config and print what would run (no network)
    pmx up --env prod             start the engine in the background
    pmx tui --env prod            watch it
    pmx status                    sources, strategies, orders, PnL
    pmx positions                 open positions per strategy and market
    pmx risk                      capital at risk, worst case by outcome, limit usage
    pmx perf --strategy ID        stats, breakdowns, and a verdict: profitable / unprofitable / inconclusive
    pmx down                      stop in order: halt, cancel working orders, save

    pmx run --env prod            the same engine in the foreground (Ctrl-C stops it in order)

    Paper fills are simulated against the real order book; paper money starts at [engine] paper_cash.
    Every order and fill is in data/ledger.sqlite, by strategy and version.

SEE ALSO
    pmx help trade · pmx help backtest · pmx help monitor
"""),
    "trade": ("place or cancel orders by hand, stop trading", """\
TRADING BY HAND, STOPPING TRADING

    The engine must be running (pmx up). Manual orders go through the same risk checks and ledger as
    strategies, as strategy "manual" (limits: [manual] in config/strategies.toml).

    pmx order buy TICKER 5 @ 0.40              rest a paper bid: 5 YES at 40¢
    pmx order sell TICKER 5 @ 0.47 --ioc       sell now or not at all
    pmx order buy TICKER 1 @ 0.40 --mode live  real money: needs pmx run --live, and asks you to confirm
    pmx cancel --all                           cancel every manual order
    pmx cancel ORDER_ID                        one order

    STOPPING
    pmx halt                                   stop all trading now (new orders refused, quotes canceled)
    pmx halt STRATEGY                          one strategy · pmx halt STRATEGY/TICKER: one market
    pmx halt --reason "checking something"     shown in the log and the dashboard
    pmx resume                                 undo a halt
    pmx down                                   stop the engine (cancels its working orders)

    A halt is a file (data/HALT), so it holds across restarts until pmx resume.
"""),
    "backtest": ("test a strategy on history", """\
BACKTESTING

    A backtest runs config/strategies.toml through the same engine, on a simulated clock.

    pmx backtest --from 2026-10-19                     a recorded day (real order books)
    pmx backtest --from 2026-10-18 --to 2026-10-25     several days
    pmx backtest --from 2026-10-19T08:31:10Z           from an instant: replay a live session
    pmx backtest --nfl-history --seasons 2025,2026     NFL seasons from downloaded candles (rougher fills)

    pmx backtest report RUN                            stats per strategy and market, with a verdict
    pmx backtest compare RUN paper:2026-10-19          a backtest against the paper ledger for that day
    pmx backtest view RUN                              open it in the dashboard

    Results go to data/backtests/RUN/. Recorded-day backtests are only possible for days you recorded
    (pmx help record). Before backtesting, find the edge: pmx help research.
"""),
    "research": ("find an edge: data and pre-registered experiments", """\
RESEARCH

    Find an edge before writing a strategy, with a test written down before you see the results.
    The rules and the library: docs/RESEARCH.md.

    DATA
    pmx data kalshi --series KXHIGHNY                    settled markets + hourly candles (years of history)
    pmx data kalshi --series KXHIGHNY --trades           + every trade, with the taker's side
    pmx data kalshi --category "Climate and Weather"     every series in a category

    EXPERIMENTS (in research/, private)
    pmx research init                      create the workspace (once)
    pmx research new weather-longshot      draft: research/experiments/<date>-weather-longshot/
                                           write spec.md (the hypothesis) and run.py (the rule)
    pmx research approve NAME              you, in a terminal: locks spec.md + run.py
    pmx research run NAME                  run the current stage, print the report
    pmx research advance NAME              discovery → validation → holdout (the holdout runs once)
    pmx research close NAME --as dead --note "what it rules out"
    pmx research status                    every experiment and how many variants were tried

    Discovery tries up to variants_max variants; validation tests the one you pick on later data; the
    holdout tests it once more on the newest data. Every number is net of fees.
"""),
    "server": ("run pmx on a server and watch it from your laptop", """\
RUNNING ON A SERVER

    The whole walkthrough is docs/runbooks/linux-vm.md. In short:

    ON THE SERVER (Ubuntu)
    scripts/setup-linux.sh                     packages, uv, the venv, clock sync, start-at-boot
    pmx keygen --env prod; pmx setkey ...      the server's own Kalshi key
    pmx doctor                                 everything set up?
    pmx ops install --record --backup          record 24/7 and back up daily, restarted if they crash
    pmx ops install --run                      + the engine (paper unless a strategy is live)
    pmx ops status                             each service's state
    pmx ops uninstall                          stop and remove them

    FROM YOUR LAPTOP (ssh access as "vm", say, in ~/.ssh/config)
    pmx remote vm tui                          the dashboard, on the server's engine
    pmx remote vm status                       any pmx command runs there: positions, risk, halt, …
    pmx remote vm logs run                     follow a service's log

    On a Mac, pmx ops install uses launchd the same way (a Mac that sleeps stops recording: keep it on AC).
"""),
    "monitor": ("alerts, health checks, logs", """\
MONITORING

    ALERTS (to your phone)
    Put a webhook in .env and pmx sends alerts there: a source down, a halt, a strategy that failed, a live
    fill, live positions that disagree with Kalshi, low disk.
        PMX_ALERT_WEBHOOK=https://ntfy.sh/<a-long-random-topic>     (or a Slack / Discord webhook URL)
        pmx ops alert-test

    THE MACHINE ITSELF
    Alerts can't tell you the machine died. Give pmx an uptime URL; it pings it every minute while running
    and the service (healthchecks.io, Better Stack, …) tells you when the pings stop.
        PMX_UPTIME_URL=https://hc-ping.com/<your-check-id>

    CHECKING
    pmx doctor                  setup: venv, keys, clock, disk, services, alerts, uptime ping
    pmx status                  the engine (or its last health file if it's down)
    pmx ops status              services: active / failed
    logs                        data/logs/ (engine: run-<date>.log); services: journalctl --user -u pmx-run
                                (server) or data/logs/launchd-*.log (Mac); from a laptop: pmx remote vm logs run
"""),
    "live": ("go live with real money", """\
GOING LIVE

    BEFORE
    1. The strategy paper-traded with its current params for [ops] min_paper_days (10) and its paper verdict
       isn't unprofitable: pmx run --live checks this (the promotion gate) and refuses otherwise.
    2. Risk limits sized to the money in the account: [risk] and the strategy's risk = {...}.
    3. Alerts and an uptime URL set (pmx help monitor); pmx doctor clean.
    4. Start small: one contract, a few days, compare with paper (pmx perf --mode live / --mode paper).

    STARTING
    In config/strategies.toml set the strategy's mode = "live", then:
        pmx run --env prod --live              asks you to type prod
        pmx up --env prod --live --confirm prod      the same in the background
        pmx ops install --run --live           as a service (on prod it confirms for you)
    Going live past the gate (logged): --skip-paper-check "why".

    WHAT PROTECTS YOU
    pmx guard              starts with every live engine; cancels its orders within seconds if it dies
    order expiry           live resting orders expire at Kalshi after [ops] live_order_ttl_s (600) and pmx
                           renews them: if the whole machine dies, nothing rests longer than that
    reconciliation         pmx checks its orders, fills, and positions against Kalshi at start, on reconnect,
                           and hourly; a disagreement is an alert
    risk limits            every order: loss, position, capital at risk, worst case, drawdown (halts)

    EMERGENCY
    pmx halt                         stop all trading now (pmx remote vm halt from elsewhere)
    pmx cancel --all --mode live     cancel manual live orders · pmx down: stop the engine
    Kalshi's website                 cancel anything left, if pmx can't
"""),
    "troubleshoot": ("something's wrong", """\
TROUBLESHOOTING

    pmx doctor                                     start here
    "pmx isn't running"                            pmx up --env prod (or pmx ops status on a server)
    "pmx is already running"                       pmx status; pmx down stops it
    the dashboard is empty                         is anything running? pmx status; pmx tui starts a feed
    orders rejected                                pmx status shows the rule (max_loss, stale_book, …); the
                                                   limits are in config/strategies.toml; pmx config check
    "live trading refused"                         the promotion gate: pmx help live
    a service keeps restarting                     pmx ops status; journalctl --user -u pmx-run (server)
    services stop when I log out (Linux)           sudo loginctl enable-linger $USER
    429 retries in the log                         no API key: REST polling shares Kalshi's rate limit
    gaps in recordings                             the machine slept (keep a Mac on AC) or the network dropped
    keys                                           pmx check --env prod; pmx setkey to replace a key
    no colours, or odd symbols                     colour is only on a terminal; NO_COLOR=1 turns it off
"""),
}

EXAMPLES: dict[str, str] = {
    "markets": "pmx markets --series KXNFLGAME --env prod\npmx markets --event KXHIGHNY-26NOV02 --env prod",
    "book": "pmx book KXHIGHNY-26NOV02-B58.5 --env prod",
    "watch": "pmx watch TICKER --env prod                    needs a key",
    "record": "pmx record --env prod                          everything in config/recorder.toml\n"
              "pmx record --env prod --series KXNFLGAME       only these series\n"
              "pmx record --counts                            what was recorded today",
    "compact": "pmx compact                                    every complete recorded day → data/parquet/\n"
               "pmx compact --day 2026-10-19",
    "account": "pmx account --env prod",
    "keygen": "pmx keygen --env prod                          then paste the public key into Kalshi",
    "setkey": "pmx setkey --env prod                          asks for the key ID and the key file",
    "check": "pmx check --env prod\npmx check --env demo --order                   + a 1¢ order round trip (demo)",
    "serve": "pmx serve --env prod                           market data + models, no strategies",
    "up": "pmx up --env prod                              the engine in the background\n"
          "pmx up --env prod --live --confirm prod        live (pmx help live)\n"
          "pmx up --demo                                  an invented session",
    "down": "pmx down\npmx down --demo",
    "tui": "pmx tui --env prod\npmx tui --env prod --series KXHIGHNY --models market_mid",
    "data": "pmx data kalshi --series KXHIGHNY               settled markets + hourly candles\n"
            "pmx data kalshi --series KXHIGHNY --trades      + every trade\n"
            "pmx data kalshi --category \"Climate and Weather\" --since 2025-01-01\n"
            "pmx data nfl --seasons 2025,2026               NFL play-by-play",
    "research": "pmx research new weather-longshot\npmx research run weather-longshot\n"
                "pmx research advance weather-longshot\npmx research status",
    "run": "pmx run --env prod                             paper, in the foreground\n"
           "pmx run --env prod --live                      live strategies too (asks you to type prod)\n"
           "pmx run --env prod --no-record                 beside pmx record",
    "config": "pmx config check",
    "halt": "pmx halt                                       everything\n"
            "pmx halt nfl-edge --reason \"news\"              one strategy\n"
            "pmx halt nfl-edge/KXNFLGAME-26NOV09BUFMIN-BUF  one market",
    "resume": "pmx resume\npmx resume nfl-edge",
    "backtest": "pmx backtest --from 2026-10-19 --to 2026-10-25\npmx backtest report RUN\n"
                "pmx backtest compare RUN paper:2026-10-19",
    "risk": "pmx risk\npmx risk --mode live",
    "perf": "pmx perf --strategy nfl-edge\npmx perf --strategy nfl-edge --mode live --from 2026-11-01",
    "export": "pmx export --year 2026                         fills and settlements as CSV (taxes)",
    "guard": "(started by pmx run --live; you don't run it yourself)",
    "doctor": "pmx doctor",
    "ops": "pmx ops install --record --backup              record 24/7 and back up daily\n"
           "pmx ops install --run                          + the engine\n"
           "pmx ops status\npmx ops uninstall\npmx ops backup --to /Volumes/Backup/pmx\npmx ops alert-test",
    "remote": "pmx remote vm tui\npmx remote vm status\npmx remote vm halt --reason \"checking\"\n"
              "pmx remote vm logs run",
    "order": "pmx order buy TICKER 5 @ 0.40                  paper: rest a bid for 5 YES at 40¢\n"
             "pmx order sell TICKER 5 @ 0.47 --ioc           sell now or not at all\n"
             "pmx order buy TICKER 1 @ 0.40 --mode live      real money (asks to confirm)",
    "cancel": "pmx cancel --all\npmx cancel ORDER_ID",
    "status": "pmx status",
    "positions": "pmx positions\npmx positions --mode live",
    "advise": "pmx advise TICKER --p 0.62 --expires 30m        your own probability, for strategies that use it",
    "bench": "pmx bench feed\npmx bench core",
    "help": "pmx help\npmx help live\npmx help order",
}

SEE_ALSO: dict[str, str] = {
    "markets": "markets", "book": "markets", "watch": "markets", "tui": "markets", "record": "record",
    "compact": "record", "account": "start", "keygen": "start", "setkey": "start", "check": "start",
    "serve": "run", "up": "run", "down": "run", "run": "run", "config": "run", "status": "monitor",
    "positions": "run", "risk": "run", "perf": "run", "order": "trade", "cancel": "trade", "halt": "trade",
    "resume": "trade", "backtest": "backtest", "data": "research", "research": "research", "ops": "server",
    "remote": "server", "doctor": "monitor", "guard": "live", "export": "run", "advise": "trade",
}

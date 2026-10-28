# Runbook — operations

How to run pmx unattended, stop it, recover after a crash, and rotate keys. Recording game days has its own
runbook: `docs/runbooks/recording.md`.

## Before a game day

```
pmx doctor                                # venv = uv.lock, TUI builds, keys present and 0600, clock, disk
pmx config check                          # config/strategies.toml validates; read the plan it prints
pmx check --env prod                      # auth, REST, websocket
```

- Plug the Mac in. `pmx run` and `pmx record` hold a no-sleep assertion (`caffeinate`), but on battery the Mac can
  still sleep (closing the lid, low power). Both warn at start when on battery, and every sleep leaves a
  `sleep_gap` in the raw logs (and an alert).
- Start the recorder (`pmx record --env prod --series KXNFLGAME`), or run the engine with `[engine] record = true`,
  never both (ESPN would be recorded twice).

## Run, watch, stop

```
pmx up --env prod                          # pmx run in the background (data/pmx.pid); paper unless --live
pmx tui                                    # attach the dashboard; q detaches, pmx keeps running
pmx status                                 # sources, slots, orders, halts, PnL — from the engine, or its health file
pmx risk                                   # the portfolio: equity, capital at risk, scenarios, limits
pmx down                                   # orderly stop: halt, cancel working orders, save, close; then pmx guard
```

`data/health.json` (engine) and `data/health-record.json` (recorder) are rewritten every 10 s; `pmx status` reads
them when the engine doesn't answer, and says how old they are.

## Manual orders and halts

```
pmx order buy KXNFLGAME-26OCT19DALGB-GB 5 @ 0.40                     # paper; slot manual/<ticker>, [manual] limits
pmx order sell KXNFLGAME-26OCT19DALGB-GB 1 @ 45c --ioc
pmx cancel --all                                                    # every manual order (or an order id, --ticker)
pmx positions
pmx halt [smoke | smoke/KXNFLGAME-…] --reason "…"                    # applied at once and kept in data/HALT
pmx resume [scope]
pmx advise KXNFLGAME-… --p 0.62 --expires 30m --note "…"  # for strategies that list `manual`
```

`pmx halt` writes the kill-switch file (it survives restarts and works when the engine is hung) and tells the
running engine over the control socket so it applies at once.

## Live trading

- Two switches (invariant 11): `mode = "live"` on the strategy (or `--mode live` on a manual order) **and**
  `pmx run --live`. A live run needs a global `[risk] max_loss` and the venue key. On prod it also asks you to
  type `prod` after showing the plan (`pmx up --live --confirm prod` headless); a live manual order asks you to
  type `yes` (`--yes` is refused on prod).
- **`pmx guard`** starts with every live run, in its own session, logging to `data/logs/guard.log`. If the
  engine dies (`kill -9`, a crash) it cancels every resting order of this engine instance (`pmx-<instance>-…`) at
  once; if the engine hangs for `[ops] guard_s` (10 s) it cancels them too and re-arms when the engine answers
  again. It never touches other instances' or your own orders. Kalshi has no cancel-on-disconnect, so this is the
  only dead-man switch: don't trade live without it (`guard=False` exists for tests).
- On start, and on every reconnect of the account stream, the live broker reconciles with Kalshi: it adopts an
  earlier run's resting orders whose strategy is still live, cancels this instance's orders nobody owns, books
  fills it missed, and cross-checks positions.

## After a crash

1. `pmx status` — is anything still running? `tail data/logs/guard.log` — did the guard cancel?
2. If the engine died with live orders resting and the guard didn't cancel (e.g. the Mac rebooted), check Kalshi's
   site for this instance's orders (client ids `pmx-<data/instance>-…`) and cancel them there.
3. Start again: `pmx up --env … [--live]`. Paper orders from the dead run are canceled in the ledger; live ones are
   reconciled (adopted or canceled) before anything trades. Check the log's `reconciled (start): …` line.
4. If a strategy failed (`slot … failed and stopped` alert), fix it and restart — its slot is dead until then.

## Unattended (launchd)

```
pmx ops install --record --run --backup   # writes and loads ~/Library/LaunchAgents/com.pmx.*.plist
pmx ops uninstall                                    # unloads and removes them
```

The `record` and `run` agents start at login and are restarted when they crash (not after a clean `pmx down`,
which exits 0); `--live` makes the run agent trade live (with `--confirm prod` on prod). The `backup` agent runs
`pmx ops backup` daily at 04:30. Their own output goes to `data/logs/launchd-<name>.log`; the engine's log is
`data/logs/run-<date>.log` (rotated at 50 MB, 5 kept).

## Backups

`pmx ops backup --to /Volumes/Backup/pmx` (or `[ops] backup_dir`): raw logs are copied incrementally and
verified by hash; each ledger is copied with SQLite's online backup API (consistent while the engine writes),
checked with `PRAGMA integrity_check`, and kept `[ops] backup_keep_days` (14) days. Raw logs and the ledger are
the only things that can't be rebuilt.

## Alerts

macOS notifications (and a log line) on: a live fill, any halt, a slot error, a source down > 60 s, a source
queue overflow, the order scheduler holding writes > 30 s, a slot over its CPU budget, a failing advisor, feed
drops, low disk (< 5 GB), a sleep, running on battery. The same alert goes out at most every 5 minutes. Tune in
`[ops]` (`alerts = false` keeps only the log lines).

## Rotating keys

1. Create a new key on Kalshi (prod: kalshi.com → Account → API keys; demo: demo.kalshi.co), download the file.
2. `pmx down` (and stop `pmx record` if it uses the websocket).
3. `pmx setkey --env prod <KEY_ID> --key ~/Downloads/<file>` — moves it to `~/.kalshi/pmx-prod.key` (0600),
   updates `.env`, runs `pmx check`.
4. Delete the old key on Kalshi, and the old file (`~/.kalshi/*.old`).
5. Start again.

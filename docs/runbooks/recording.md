# Runbook — recording game days

Raw recordings are irreplaceable: a day that wasn't recorded can never be backtested at book
fidelity or re-parsed after a parser fix. Record every NFL game day (Thu / Sun / Mon) from before the
first kickoff until after the last final.

## What gets recorded

`pmx record` records the venue (Kalshi) plus every `[[source]]` plugin listed in `config/recorder.toml`,
running each exactly as `pmx run` would but keeping only its raw records. Today that's one domain source:

| Source | Without an API key | With an API key |
|---|---|---|
| `espn` (`kind = "espn_nfl"`, params in the config) | scoreboard every 60 s (5 s while a game is on); each live game's summary every 15 s, at once on a new play, and once at the final | same |
| `kalshi` | — (the websocket needs a key) | every book delta and print for the universe in `config/recorder.toml`, verbatim websocket frames |
| `kalshi_rest` | every book in the universe polled round-robin at ≤ 15 requests/s (each of ~54 NFL markets every ~3–4 s) plus the public tape every 2 s, verbatim REST bodies | not used |

Each response is one line in `data/raw/<source>/<UTC day>/<HH>-<start>.log.zst`; HTTP sources wrap it
as `{"url", "status", "ms", "body"}`. A response byte-identical to the previous one for the same URL
isn't written again. Payloads that look like credentials are refused (a `redacted` control record is
written instead), and a laptop sleep writes a `sleep_gap` control record to every log.

## Run it

```
pmx record --env prod --series KXNFLGAME                     # everything configured; --sources espn | kalshi
```

Narrow `--series` on game days without a key: REST polling time is split across every market in the
universe, so the default four series (~340 markets) would poll each book only every ~25 s.

**tmux** (simplest):

```
tmux new -s rec 'pmx record --env prod --series KXNFLGAME 2>&1 | tee -a data/logs/record.log'
# detach: ctrl-b d · reattach: tmux attach -t rec · stop: ctrl-c in the session
```

**Background, no terminal:**

```
nohup pmx record --env prod --series KXNFLGAME >> data/logs/record.log 2>&1 < /dev/null &
echo $! > data/record.pid          # stop: kill -INT $(cat data/record.pid)  (flushes and closes cleanly)
```

**launchd user agent** (starts at login, restarts if it dies): save as
`~/Library/LaunchAgents/com.pmx.record.plist`, then `launchctl load ~/Library/LaunchAgents/com.pmx.record.plist`
(`launchctl unload …` to stop).

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.pmx.record</string>
  <key>WorkingDirectory</key><string>/Users/YOU/dev/pmx</string>
  <key>ProgramArguments</key><array>
    <string>/Users/YOU/dev/pmx/.venv/bin/pmx</string><string>record</string>
    <string>--env</string><string>prod</string><string>--series</string><string>KXNFLGAME</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/YOU/dev/pmx/data/logs/record.log</string>
  <key>StandardErrorPath</key><string>/Users/YOU/dev/pmx/data/logs/record.log</string>
</dict></plist>
```

**Keep the Mac awake** — a sleeping laptop records nothing. On AC power:
`caffeinate -s -w $(cat data/record.pid) &` holds a no-sleep assertion for as long as the recorder runs.

## Check that it worked

```
pmx record --counts                        # today (UTC): records per source and kind, time span, MB
pmx record --counts DAY                    # another UTC day (YYYY-MM-DD)
tail data/logs/record.log                  # "recorded so far: …" every minute
pmx compact --day DAY                      # Kalshi websocket logs → data/parquet/kalshi/{bbo,trades}
```

Healthy on a game day: `espn` grows every few seconds while games are on; `kalshi_rest` (or `kalshi`)
grows continuously; `sleep_gaps` from `pmx compact` is 0. A few `429` retries in the log are fine; a
steady stream means a lower `rps` for the `kalshi_market` source.

## Disk use

REST polling (no key, `--series KXNFLGAME`, 54 markets, one game live): `kalshi_rest` ~13 MB/hour (deep NFL
books, ~4 written per second after skipping unchanged ones); `espn` ~1.8 MB/hour per live game (summaries are
~600 KB raw each). The websocket recording (`kalshi`, with a key) is far smaller: it stores deltas.

Retention: raw days older than `[recorder] keep_days` (default 30, `config/recorder.toml`) are deleted
by the running recorder once a day — but only days that `pmx compact` has turned into parquet. ESPN and
`kalshi_rest` have no compactor, so their raw logs are kept. `pmx record --dry-run-retention` lists
what would go. Back raw logs up to a second disk (`pmx ops backup`).

## After a crash

1. Restart the recorder at once — every minute it's down is lost. Nothing needs repairing first: each
   process writes its own hourly files, and a file cut off mid-write reads back up to its last flush
   (every 5 s).
2. Check the gap: `pmx record --counts` (time span per source) and the log's last lines.
3. Note the gap in the day's notes if it overlapped a game; backtests over it should skip that window
   (a `sleep_gap` control record marks laptop sleeps automatically, crashes are visible as missing time).

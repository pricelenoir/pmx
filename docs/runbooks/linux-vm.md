# Running pmx on a Linux VM

The VM runs pmx unattended as systemd user services: the recorder all the time, the engine (paper, later live), and
a daily backup. You watch and control it from your own machine over SSH (`pmx remote`) and get alerts on your phone.
Nothing listens on the network: the dashboard's feed is a Unix socket that SSH forwards.

```
 your laptop                                   the VM
 pmx remote vm tui   ──── ssh (socket fwd) ──► data/pmx.sock ◄── pmx run      (pmx-run.service)
 pmx remote vm status ─── ssh ──────────────►  pmx status          pmx record   (pmx-record.service)
 phone ◄── webhook (ntfy / Slack / Discord) ── alerts              pmx ops backup (pmx-backup.timer, 04:30)
```

## 1. The machine

- **Size:** 2 vCPU, 4 GB RAM, 80–100 GB disk (raw recordings grow a few GB a month; keep them all). Any provider:
  AWS EC2 or Lightsail, DigitalOcean, Hetzner.
- **Image:** Ubuntu 24.04 LTS. **Region:** the US (Kalshi is a US exchange; latency doesn't matter much here).
- **Access:** SSH keys only. Open nothing but SSH: `sudo ufw allow OpenSSH && sudo ufw enable`. Ubuntu installs
  security updates on its own (`unattended-upgrades`); a reboot brings every service back (lingering, below).
- Run pmx as an ordinary user (the image's default user, or a dedicated one), never as root.

On your laptop, give it a name in `~/.ssh/config`, so every command below can say `vm`:

```
Host vm
  HostName 203.0.113.10
  User ubuntu
  IdentityFile ~/.ssh/id_ed25519
```

## 2. The code

The repo is private, so give the VM a read-only deploy key:

```sh
ssh vm
ssh-keygen -t ed25519 -f ~/.ssh/pmx_deploy -N "" -C "pmx deploy"
cat ~/.ssh/pmx_deploy.pub          # GitHub → the repo → Settings → Deploy keys → Add (read-only)
printf 'Host github.com\n  IdentityFile ~/.ssh/pmx_deploy\n' >> ~/.ssh/config
git clone git@github.com:<you>/pmx.git ~/pmx
cd ~/pmx && scripts/setup-linux.sh
```

`scripts/setup-linux.sh` installs git, curl, sqlite3, tmux, and uv; builds the locked venv; puts `pmx` on the
PATH; turns on NTP; and enables **lingering** (`loginctl enable-linger`), which lets user services start at boot and
keep running after you log out. It's safe to run again. Add `--with-tui` to also build the dashboard on the VM.

## 3. Keys and alerts

Give the VM **its own** Kalshi key, so either machine's key can be revoked alone. Make it on the VM; the private
half never leaves it. Without a key everything still runs, but the recorder and the engine both poll REST and share
the public rate limit (the logs show `429, retry`); with one they use the websocket.

```sh
pmx keygen --env prod              # paste the public key into Kalshi → API Keys → Create
pmx setkey --env prod KEY_ID       # then: pmx check --env prod
```

Alerts (a source down, a halt, a failed slot, a live fill, low disk) go to a webhook set in `.env` — never in the
config, since the URL works as a password:

```sh
echo 'PMX_ALERT_WEBHOOK=https://ntfy.sh/<a-long-random-topic>' >> ~/pmx/.env
pmx ops alert-test
```

[ntfy](https://ntfy.sh) is the simplest: install its phone app and subscribe to the topic (pick a long random
name: anyone who knows it can read it). A Slack or Discord webhook URL works too; pmx formats for each.

Alerts come from pmx, so they can't tell you the VM itself died. For that, create a check on a dead-man service
([healthchecks.io](https://healthchecks.io) is free) with a period of a few minutes, and give pmx its ping URL; the
engine and the recorder ping it every minute, and the service alerts you when the pings stop:

```sh
echo 'PMX_UPTIME_URL=https://hc-ping.com/<check-uuid>' >> ~/pmx/.env
```

If the VM dies with live orders resting, they still come off: live resting orders expire at Kalshi after
`[ops] live_order_ttl_s` (10 minutes) unless pmx renews them.

## 4. Backups off the VM

`pmx ops backup` copies raw recordings, the ledger, and `research/` to `[ops] backup_dir`, then runs
`backup_sync` to ship that copy elsewhere. In `config/strategies.toml`:

```toml
[ops]
backup_dir = "~/pmx-backup"
backup_sync = ["aws", "s3", "sync", "{dir}", "s3://my-pmx-backups/vm"]   # or ["rclone", "sync", "{dir}", "remote:pmx"]
```

Give the sync tool its own credentials, limited to that one bucket. Check that `pmx ops backup` runs clean by hand
once.

## 5. Start it

```sh
pmx doctor                                  # venv, keys, clock, disk, services, lingering, alerts
pmx ops install --record --backup           # record every series in config/recorder.toml, 24/7; back up daily
pmx ops install --run                       # + the engine: config/strategies.toml (paper unless a strategy is live)
pmx ops status                              # record active · run active · backup active
```

Services restart on a crash (after 10 s) and at boot; a clean stop (`pmx down`) stays stopped. Each gets 90 s to
stop in order. With the recorder service installed, the engine service runs with `--no-record`, so only one process
writes raw logs (two would double every frame in replays): keep `config/recorder.toml`'s series a superset of
what your strategies trade, so their days can be backtested. Install `--run --live` only for a strategy you've signed off for live trading: it starts
`pmx guard`, and on prod it is the `--confirm prod` you would otherwise type.

## 6. Watch and control it from your laptop

```sh
pmx remote vm tui                 # the dashboard here, on the VM's engine (q leaves; pmx keeps running there)
pmx remote vm status              # any pmx command runs on the VM:
pmx remote vm positions --mode paper
pmx remote vm risk
pmx remote vm perf --strategy ID
pmx remote vm halt --reason "checking something"      # the kill switch; pmx remote vm resume
pmx remote vm logs [record|run|backup]                # follow the service logs (journalctl)
```

`pmx remote` runs `ssh vm`, so whatever works for `ssh vm` works here; pmx lives in `~/pmx` on the VM unless you
pass `--dir` (or set `PMX_REMOTE_DIR`). The dashboard is built on your laptop (`pmx tui` once builds it).

To research on recordings made on the VM, copy them down: `rsync -a vm:pmx/data/raw/ data/raw/`.

## 7. Updating

```sh
ssh vm
cd ~/pmx && git pull --ff-only && uv sync --locked
pmx ops install --record --backup --run     # rewrites the units and restarts each service on the new code
```

Restarting the engine stops it in order first (halt, cancel working orders, save); live orders are reconciled when
it starts. To restart one service only: `systemctl --user restart pmx-run`.

## Troubleshooting

- **Services stopped when I logged out / didn't come back after a reboot:** lingering is off —
  `sudo loginctl enable-linger $USER` (`pmx doctor` checks it).
- **`systemctl --user` says "Failed to connect to bus":** you're in a shell without a user session (e.g. `sudo -u`);
  log in as the user over SSH instead.
- **A service keeps restarting:** `systemctl --user status pmx-run` and `pmx remote vm logs run`.
- **`pmx remote vm tui` says pmx isn't running:** the engine (or `pmx serve`) isn't up on the VM:
  `pmx remote vm ops status`.
- **Stop everything:** `pmx ops uninstall` (stops and removes the services), or `pmx down` for the engine alone.

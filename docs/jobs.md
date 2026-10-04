# The jobs file

A jobs file is TOML. `~/.config/takt/jobs.toml` holds the jobs of this machine. `~/.config/takt/jobs.<host>.toml` holds the jobs of another host. [examples/](../examples) has three complete files.

```toml
[settings]
python = "/opt/homebrew/bin/python3"
path = "/Users/me/.local/bin:/opt/homebrew/bin:/usr/bin:/bin"

[job.backup]
schedule = "30 3 * * *"
lock = ["disk"]
command = ["/usr/local/bin/backup", "--quiet"]
stderr = "~/.local/state/takt/backup.err"
```

## Settings

| Key | Meaning |
|---|---|
| `python` | The interpreter that the scheduler uses to run takt. Use a stable path. On macOS, use `/opt/homebrew/bin/python3`, because the real path of Homebrew Python changes with each upgrade. On Windows, use the full path to `python.exe`, because `python3` is often the Microsoft Store stub. |
| `status` | Where job statuses show: `master` (default), `all`, `client` or `none`. See [Status across devices](cli.md#status-across-devices). Set it on the controller. |
| `master` | The device that shows all statuses in `master` mode. Default: the controller. |
| `name` | The name of this device in the takt-net. Default: its short host name. |
| `path` | The `PATH` that launchd and systemd give to the job. Write it out in full. A copy of your shell `PATH` can hold temporary directories. |

## Job keys

A job is a `[job.<id>]` table. The id can contain letters, digits, `-` and `_`. Two ids that differ only by case are an error, and Windows device names (`CON`, `NUL`, `COM1` and the like) are not allowed, because the id becomes a file name.

| Key | Default | Meaning |
|---|---|---|
| `schedule` | none | A cron expression. Without a schedule (and without `run_at_load`), the job is manual-only: it runs only when you start it with `takt start` or `takt run`. On systemd it installs as a service with no timer, and `status` shows `SCHED` as `-`. |
| `command` | | One command as an argv list. A string runs through `/bin/sh -c`. |
| `stdout`, `stderr` | none | Append the output of the command to these files. `~` is expanded, and a missing directory is created. If a file cannot be opened, the step is `failed` with exit code 127 and a note, and the next steps run. |
| `step` | | Several commands, as `[[job.<id>.step]]` tables. Use `step` or `command`, not both. |
| `lock` | `[]` | Lock names: letters, digits, `-` and `_`, no Windows device name. Names are not case-sensitive. Two jobs with the same lock name never run at the same time. |
| `lock_timeout` | `900` | Seconds to wait for a lock. After this, the run ends as `lock-timeout`. |
| `after` | `[]` | Job ids that must run first when they are due in the same slot. |
| `needs` | `[]` | Preflight checks. If one fails, takt skips the job and records why. |
| `wants` | `[]` | Preflight checks. If one fails, takt records a warning and runs the job. |
| `catch_up` | `"run-once"` | After a sleep, `run-once` runs a missed slot once. `skip` drops a start that is more than 5 minutes late. |
| `run_at_load` | `false` | Also run the job when the scheduler loads it (at login). |
| `on_event` | none | Windows only. An event query (the XML of a Task Scheduler event trigger) that also starts the job, for example a Remote Desktop session event. launchd and systemd refuse a job with `on_event`. |
| `bundle` | none | macOS only. The `AssociatedBundleIdentifiers` value of the plist. |
| `replaces` | `[]` | Old schedules that `install` retires. See [replaces](#replaces). |

### Steps

```toml
[[job.ingest.step]]
id = "index"
command = ["/usr/local/bin/ingest", "--reindex"]

[[job.ingest.step]]
id = "ingest"
command = ["/usr/local/bin/ingest", "--json"]
report = "json-failed-sources"
stdout = "~/.local/state/takt/ingest.log"
```

Steps run in order. A failed step does not stop the next step. Each step has `id`, `command`, `stdout`, `stderr` and `report`.

`report = "json-failed-sources"` reads the last JSON line of the step output. If `error.failed_sources` holds names, the step is `partial` and the names show in the status, also when the step exits with 0. If `error` is not an object (`{"error": "connection refused"}`), its text becomes the note of the step.

## Schedules

takt reads five cron fields: minute, hour, day of month, month, day of week. Each field can be `*`, a number, a range (`1-5`), a list (`0,30`) or a step (`*/15`). `@hourly`, `@daily`, `@weekly` and `@monthly` also work.

Limits:

- Month names and day names (`JAN`, `MON`) are not supported.
- A day of month and a day of week in the same expression are not supported. Cron runs on either day, but launchd and systemd need both.
- Task Scheduler cannot run every minute inside a range of hours (`* 9-17 * * *`).
- Task Scheduler takes at most 48 triggers per task. takt writes one trigger for each time of day, unless the minutes repeat evenly across all hours (`*/15 * * * *` is 1 trigger). `*/5 9-17 * * 1-5` needs 108, so `install` refuses it on Windows before it changes anything. Use fewer times, or split the job.

A schedule that never matches a date (`0 0 30 2 *`) is an error when takt loads the file. Sparse schedules work: takt searches for slots across 8 years, so a yearly job or a job on 29 February finds its slot.

Schedules use the clock of the host that runs the job.

## Preflight checks

`needs` and `wants` take the same check names. A check can also be a command as an argv list, which passes when it exits with 0:

```toml
needs = [["C:\\Program Files\\PowerShell\\7\\pwsh.exe", "-NoProfile", "-File", "C:\\Users\\me\\bin\\wsl-running.ps1"]]
```

| Check | Passes when |
|---|---|
| `exe:<name>` | `<name>` is on `PATH`. |
| `fda` | This process can read `~/Library/Messages/chat.db`. This tests macOS Full Disk Access. If it fails, the message names the binary that needs the grant. |
| `fda:<path>` | This process can read `<path>`. |
| `owa:<profile>` | [owa-piggy](https://github.com/damsleth/owa-piggy) reports a valid token for the profile, and its reseed does not need a sign-in (`reseed.state` from `owa-piggy status --json`). An older owa-piggy without `--json` is checked on token expiry only. |

`takt preflight` runs all checks now.

## Ordering with `after`

If `ingest` has `after = ["token-refresh"]`, and both jobs are due in the same slot, the refresh runs first. The ingest runs the refresh itself, under the locks that it holds. When the scheduler starts the refresh a moment later, the refresh sees the record and exits. The refresh runs once, in each order of start. A dependency with `catch_up = "skip"` is not pulled in when its slot is more than 5 minutes old.

The `needs` and `wants` checks of the ingest run after the refresh, so the refresh can make a check pass. See [design.md](design.md#ordering).

## replaces

`replaces` names old schedules that `install` takes out of service before it registers the new job.

| Value | What `install` does |
|---|---|
| `launchd:<label>` | Runs `launchctl bootout`, then moves the plist to `~/.local/state/takt/retired/`. |
| `cron:<text>` | Saves the crontab to `~/.local/state/takt/crontab.bak`, then comments out each live line that contains `<text>`. |
| `schtasks:<task>` | Windows. Ends the task, then disables it (`schtasks /Change /DISABLE`). The task stays, so `/ENABLE` brings it back. |

`uninstall` does not restore these. The retired plist and the crontab backup stay in the state directory.

## Status values

| Status | Meaning |
|---|---|
| `ok` | Every step exited with 0 and reported no failed sources. |
| `partial` | Some steps failed or reported failed sources. |
| `failed` | Every step failed. |
| `skipped` | A `needs` check failed. The record holds the reason. |
| `lock-timeout` | The job waited longer than `lock_timeout`. |
| `never` | No record exists. |

The record (`~/.local/state/takt/<id>.json`) also holds the slot, the trigger, the wait for locks, the duration and the result of each step and preflight check.

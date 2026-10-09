# Design

## The problem

takt started from seven scheduling failures on one Mac in five days (2026-09-25 to 09-30). Each one became a requirement:

| Failure | Requirement |
|---|---|
| A cron job took its name from the commented-out line above it. | Each job has a stable id in the file. |
| An edit in a cron TUI made a second copy of the job. | Edits use the id. Enabled is job state, not a comment. |
| The Mac slept for 2 days and cron ran nothing. | Each job has a catch-up rule. |
| Two jobs used one browser profile at the same minute. The fix was a `:10` offset. | Jobs can share a lock and can run in order. |
| A job failed on every run because Full Disk Access belonged to `/usr/sbin/cron`. | A preflight check names the binary that needs the grant. |
| 3 sources failed on every run, and the only record was a JSON line in a log. | Each run records the status of each sub-step. |
| A token expired because one profile was missing from a list. | A dependency can be a health check, not only a job. |

## Shape

One TOML file declares the jobs. The scheduler of the OS is the clock: launchd, systemd or Task Scheduler. Each scheduled entry runs `takt run <id>`, and the wrapper does the rest: locks, order, preflight, steps and the record.

takt has no daemon. A daemon can stop without notice, and that was one of the failures. launchd runs a missed calendar slot once after wake, which cron does not do.

## Locks

Each run first takes `<state>/running/<id>.lock`, which says "this job is running" (`takt start` reads it). It has its own directory, so no lock name can collide with it. Only the job's own wrapper takes it, and it holds nothing else while it waits for it, so it cannot deadlock. Then the run takes an exclusive lock on `<state>/locks/<name>.lock` for each name in its `lock` list. It also takes one lock for its own id, and the same locks for each `after` dependency. It takes the locks in sorted order, so two jobs cannot deadlock.

If a lock is held, the job waits. The record holds `waited_s` and `blocked_on`. After `lock_timeout` seconds the run ends as `lock-timeout` with exit code 75.

On macOS and Linux the lock is `flock`. Each step inherits the open lock files, and `flock` belongs to the open file, so the lock stays held until the wrapper and the step have both exited. If something kills the wrapper alone, its running step keeps the lock, and the next job waits for the step. When the last process exits, the OS releases the lock, so a crashed job cannot hold it.

A process that the step starts keeps the lock only if it inherits the lock files. A shell `&` keeps them open. Python's `subprocess` closes them by default (`close_fds=True`), so a Python step that starts a background process does not protect that process with the lock. A background process that keeps the lock files holds the lock until it exits.

On Windows the lock is `msvcrt.locking`, and it ends with the wrapper process. The wrapper runs in a job object that ends its steps when the wrapper exits or is killed, so a step never runs without its lock. Before the wrapper releases its locks, it ends the other processes in its job and waits for each one to exit, so a helper that a step left running cannot run without the lock either. If takt cannot create the job object, it does not run the job, and the record says why.

Processes that a step leaves running end with the job on every scheduler: systemd stops the service's cgroup, launchd ends the job's process group, and on Windows the job object closes.

## Ordering

`after` pulls a dependency in, as `make` does. Job B has `after = ["A"]`. When B starts, it looks at the latest slot of A. If that slot is at or after the slot of B, and A has no record for it, B runs A first, under the locks that B holds. A is recorded under its own slot with `pulled_by = "B"`. When the scheduler starts A, A takes the lock, reads the record and exits.

So A runs once and before B, in each order of start, with no timing assumptions.

A job that another job pulled in for its slot exits at once when the scheduler starts it, before it waits for a lock. If it times out on a lock while the other job pulls it in, it keeps the record of the pull-in and does not write `lock-timeout`.

B checks its own `needs` and `wants` after A has run and after B holds its locks. A can make a check of B pass, for example by refreshing a token that B needs. A dependency with `catch_up = "skip"` is not pulled in when its slot is more than 5 minutes old.

The lock alone stops the overlap. `after` adds the order: in the original case, the ingest must use the tokens that the refresh just wrote.

## Catch-up

`catch_up = "run-once"` is the behavior of the schedulers: after a sleep, one start for the missed slots. `catch_up = "skip"` is done by the wrapper: a scheduled start more than 5 minutes after its slot exits with 0 and runs nothing.

The slot of a run is the latest scheduled time at or before the start. takt searches day by day across 8 years, so a yearly job woken months late finds its real slot. A schedule that never matches a date is rejected when the file loads.

After a long sleep, a pulled-in dependency is recorded under its own latest slot. Example: the Mac wakes at 15:33. The ingest (every 2 hours) has the 14:00 slot. The refresh (hourly) has the 15:00 slot. The ingest pulls in the refresh and records it under 15:00. The refresh then starts for 15:00, finds the record, and does not run a second time.

## Status

Each run writes one JSON record: slot, trigger, start time, duration, wait, status, and for each step the exit code, the duration and the failed sources. Steps run in order, and a failed step does not stop the next one, the same as `;` in a crontab line.

`report = "json-failed-sources"` reads `error.failed_sources` from the last JSON line of a step. A step that exits with 0 and reports failed sources is `partial`.

`report = "watch"` compares the output of a step with the value that takt told last time. It is edge-triggered: a value that stays changed sends one notice. A broken read (an error, a timeout, no output, or output that does not match `expect`) is a failed step, and takt does not compare it. An expired token or an error page is then a failure alert, not a change. Cooldown and the flap digest come from kikar, a private watcher prototype. The scheduler gives a watch its schedule, locks, preflight and status, so a watch is one more kind of step.

## Notifications

A job with `notify` sends a failure alert after `notify_after` bad runs in a row, one recovery notice, and the notices of its watch steps. A notice that takt cannot send stays unsent, and the next run sends it. The state lives in `<state>/notify/<id>.json`, under its own lock, because a `lock-timeout` run also writes it.

## Preflight

`needs` skips the job and records the reason. `wants` records a warning and runs the job. The Full Disk Access check opens the protected file and names the binary that needs the grant: the interpreter in `[settings] python`, not the terminal.

## Backends

| Spec | launchd | systemd | Task Scheduler |
|---|---|---|---|
| `schedule` | `StartCalendarInterval`, one dict for each combination (`0 */2` gives 12 dicts). `* * * * *` gives `StartInterval 60`. | `OnCalendar` | `CalendarTrigger` with a `Repetition` for `*/n` minutes and hourly jobs, otherwise one trigger for each time. |
| `catch_up = "run-once"` | built in | `Persistent=true` | `StartWhenAvailable` |
| `run_at_load` | `RunAtLoad` | `OnActiveSec=5s` | `LogonTrigger` |
| overlap | the wrapper locks | the wrapper locks | the wrapper locks, and `MultipleInstancesPolicy IgnoreNew` |
| `PATH` | `EnvironmentVariables` | `Environment=PATH=` | inherited |

`import-plist` reads a plist back into a job. `import-cron` reads a crontab. A commented-out cron line is never read as a job.

## Measurements

macOS (Python 3.14), Ubuntu 24.04 aarch64 (Python 3.12, systemd user instance with lingering), Windows 11 (OpenSSH into PowerShell 7.4, Python 3.11).

- **Self-test**: 392 checks pass on macOS. A copy run on Linux passes 386 of 387: the one failure, a `push` check of the takt-net settings, fails in the same way on `main` when run from a copy outside the install. Windows has not run this version (an earlier version passed 175 of 181 there). The copies skip the checks of `examples/`. Windows skips the checks that need `chmod 000`, a shell script, or inherited lock files, and runs a job-object check of its own.
- **Mutation tests**: 112 breaks of the logic, each made on a copy. 9 are for the fixes of a codex review of the watch branch: the notify lock that ran the Windows leftover cleanup, an unbounded read after a timeout, a failure alert lost when the next run was ok, a recovery count read after it was reset, a flap digest inside the cooldown, a flap count without the moves that started it, an http URL with no host, `deliver` that raised on a bad URL, and `nan` taken as a number. 13 are for watches and notifications: new items never pending, no cooldown, the told value not moved, a broken read accepted, a timeout that ends only the shell, an alert for each bad run, no recovery notice, a failed delivery taken as sent, no flap collapse, a threshold that never re-arms, a skipped job not counted as bad, `notify_after` ignored, and state that is never saved. The other 90: 16, 12, 13, 8, 10, 7, 5, 4, 2 and 3 for the fixes of ten external reviews, and 9 for ordering, systemd install and uninstall, Task Scheduler XML, ssh arguments and scheduler state. The self-test fails on all 90. The two job-object breaks ran on Windows. 2 breaks of `init` and 9 earlier breaks of the core (locks, `after`, status, preflight, catch-up) failed it in earlier versions.
- **Leftover processes on Windows**: a step started a background helper and returned while another job waited for the same lock. The helper's last write came 0.054 s before the waiting job started, and no helper was left running.
- **Task Scheduler trigger limit**: a task with 48 triggers registered on Windows 11, and one with 49 was refused ("The task XML contains too many nodes of the same type").
- **`takt start` off its slot**: a `catch_up = "skip"` job started at another time ran and recorded the trigger `start` on launchd, systemd and Task Scheduler. On Linux, removing its schedule and reinstalling retired the timer.
- **Manual-only job on Linux**: a job with no schedule installed as a systemd service with no timer, ran from `takt start`, and uninstalled.
- **Uninstall of a running job**: a job that was running at uninstall was gone afterwards on launchd, systemd and Task Scheduler (1 process before, 0 after). On Linux with no user bus, `uninstall` refused, exited with 1 and kept the unit files.
- **Reinstall**: on macOS, a throwaway job installed twice with a custom state directory loaded, ran from `launchctl kickstart` with its record in that directory, and uninstalled. On Linux, a second install restarted the timer.
- **Locks**: two real processes in the same minute with a shared lock never overlap, and the second waits and succeeds. The same pair without the lock overlaps, so the test can see the failure. This passes on all three OSes.
- **Live runs on Linux and Windows**: a probe job installed, fired at its slot from systemd and from Task Scheduler, was disabled, enabled, fired again at the next slot, was started by hand and was uninstalled. `status -A` over three hosts took 3.6 seconds.
- **Real data on macOS**: `import-plist` read the real token-refresh plist, and `import-cron` read 12 crontab jobs. The rendered plists put every slot at minute 0.
- **A watch on a live source**: a `new-items` watch read the Bærum planning archive (two `curl` calls and `jq`) into a local HTTP sink. The first read stored 10 cases and sent nothing. The second read found nothing new. With 2 cases removed from the state, it sent exactly those 2. Without the session cookie, `jq` failed (exit 4) twice, and one alert came after the second run (`notify_after = 2`). With the cookie back, it sent one recovery notice. Each read took 1.3 to 2.2 seconds.

Found by the runs:

- Windows appends to a file as a seek and a write. Two processes that append to one file at the same time overwrite lines. This broke a marker file in the self-test, not the lock.
- `schtasks /Run` refuses a disabled task, and systemd starts it.
- Rendered on the development shell, a plist got a temporary `fnm` directory in `PATH` and a versioned Homebrew path for Python. `[settings]` and `takt init` now pin both.
- A token-expiry check called 2 profiles healthy while their refresh job needed an interactive sign-in. The expiry of a token does not show the health of its refresh.

Not measured yet: a week of real jobs under launchd, a sleep and wake under launchd, and Full Disk Access when launchd starts the interpreter.

## Limits

- A step without `timeout` that hangs holds its lock. Each job that waits for that lock ends as `lock-timeout`. Set `timeout` on a step that reads the network.
- On Linux and macOS, a timeout ends the processes that the step started, found with `ps`. A process that starts between the `ps` and the kill continues to run. So does a child whose parent exited before the timeout: it is not in the tree. After a timeout, takt reads the rest of the output for at most 5 seconds and then closes the pipe, so such a child cannot hang the wrapper.
- `--check` does not run the installers. They were tested by hand on macOS, Linux and Windows.
- The Windows installer writes paths under the user profile as `%USERPROFILE%` in `takt.cmd`. A Python path with non-ASCII characters outside the profile stops the installer with an error. No account with a non-ASCII name has been tested.
- A failed step is recorded. takt does not retry it.
- `status` shows only the jobs in the jobs file. For a read-only view of every scheduled job on a machine, use the tools of the OS.
- A host updates itself only from GitHub. Between two runs of `takt-update` (one hour), a host can run an older version. `takt update -A` updates every host now.
- Windows tasks run only while the user is logged on.

## Not built

Each item has the condition that would justify it.

| Item | Build it when |
|---|---|
| A daemon or a runner that is always on | A job must run while logged out on macOS, or more often than each minute. |
| Control of jobs that takt did not install | A job outside the jobs file must be started or toggled from the TUI. |
| Retries | The first transient failure that a retry would fix. |
| Notices by mail, Teams or a desktop notification | A reader that cannot take an HTTP POST. |
| A filter on watch items after a good read | A `new-items` watch must ignore some lines. A filter in the command (`grep`) makes "no match" an empty read, which is a failure. |
| A curses TUI | The TUI needs panes that fzf cannot show. |
| Windows tasks that run while logged out | A Windows job must run at the login screen. This needs S4U or a stored password. |

## Prior art

[skdlr](https://github.com/byteowlz/skdlr) (Rust) schedules jobs on the same three backends, with a SQLite store, a TUI, an MCP server and an HTTP API. In its source there are no locks between jobs, no ordering, no preflight per job and no status per step. Those four are the requirements above. Its schtasks backend builds `/CREATE` arguments, which cannot express `0 */2`. takt writes Task Scheduler XML with repetition triggers. No code is copied: skdlr has no license.

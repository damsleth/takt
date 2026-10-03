# Commands

takt reads `~/.config/takt/jobs.toml` unless you give `--spec <file>` after the command. `--state <dir>` sets the state directory the same way.

`install`, `uninstall`, `push`, `start`, `enable` and `disable` change the scheduler or another host. Without `--allow-writes`, they show a plan and stop. `run` executes a job at once, `init` writes a new jobs file, and `render` writes files to a folder.

## Daily use

| Command | What it does |
|---|---|
| `takt` | Opens the TUI. The same as `takt ui`. |
| `takt status` | Shows the scheduler state, the last run and the next slot of every job. |
| `takt status -A` | The same for this machine and every host in `~/.config/takt/`. Hosts run in parallel. |
| `takt status --json` | The same rows as JSON, for other tools. |
| `takt show <id>` | Shows the last record of a job: steps, exit codes, preflight, and the end of its logs. |
| `takt start <id>` | Starts the job now, through the scheduler. The run records the trigger `start`, and `catch_up = "skip"` does not drop it. Needs `--allow-writes`. |
| `takt enable <id>` | Arms the schedule of an installed job. Needs `--allow-writes`. |
| `takt disable <id>` | Disarms the schedule. The job stays installed. Needs `--allow-writes`. |

`SCHED` in the status table is `on` (installed and armed), `off` (installed, disarmed) or `-` (not installed). `NEXT` uses the clock of the host.

## Setup

| Command | What it does |
|---|---|
| `takt init` | Writes a starter `jobs.toml` with one example job. It never overwrites a file. |
| `takt install` | Registers every job with the scheduler of this OS. |
| `takt uninstall [id ...]` | Stops and removes jobs. Without ids, it removes every job in the file and every takt job that the scheduler still has. |
| `takt preflight` | Runs the `needs` and `wants` checks of every job and prints the result. |
| `takt run <id>` | Runs a job through the wrapper now, in this terminal. The scheduler calls this command. |
| `takt run <id> --dry-run` | Shows the locks, the `after` order, the preflight result and the steps. Runs nothing. |
| `takt render [--out DIR]` | Writes the launchd, systemd and Task Scheduler files to `DIR` (default `./staging`). |

`install` on each OS:

- macOS: writes `~/Library/LaunchAgents/dev.takt.<id>.plist` and runs `launchctl bootstrap`.
- Linux: writes `~/.config/systemd/user/takt-<id>.{service,timer}`, then runs `daemon-reload` and `enable --now`.
- Windows: writes the task XML to the state directory and runs `schtasks /Create /TN \takt\<id>`.

Run `install` again after you edit the jobs file. It replaces each registration: launchd boots out the loaded copy first, and systemd restarts the timer. If launchd refuses to boot out a loaded job (a takt job or a `replaces` target), `install` stops before it moves or writes a plist. Each label is booted out once, also when two jobs replace the same label or a `replaces` names a job that `install` retires. On systemd, a restart runs a missed slot once (`Persistent=true`). If you removed or renamed a job, `install` first retires the takt job that the scheduler still has under the old id. If you removed a job's schedule, `install` disables and removes its old systemd timer.

Before `install` and `uninstall` plan anything, takt reads the job list of the scheduler. It also finds a loaded takt job whose plist is gone, and it looks up each id that you give to `uninstall`. If it cannot read it (for example, no systemd user bus), the command prints the error, exits with 1 and changes nothing.

`uninstall` stops only the jobs that the scheduler has registered, including a run that is in progress: launchd boots out the job, systemd stops the timer and the service, and Task Scheduler ends the task before it deletes it. If the scheduler refuses a stop, takt prints the error, exits with 1 and keeps the files of that job.

The scheduled command includes the `--spec` and `--state` paths that `install` used, as absolute paths. A job installed with `--state some/dir` keeps its records and locks there.

`install` also retires what a job names in `replaces` (see [jobs.md](jobs.md#replaces)).

## Other hosts

| Command | What it does |
|---|---|
| `takt --host <h> <command>` | Runs the command on host `h` over ssh. `--host` must be the first argument. |
| `takt --host <h> push` | Copies `takt.py` and `jobs.<h>.toml` to the host. Needs `--allow-writes`. |
| `takt --host <h> install` | Pushes, then installs on the host. Needs `--allow-writes`. |

See [hosts.md](hosts.md).

## Migration from cron and launchd

| Command | What it does |
|---|---|
| `takt import-cron [--file F]` | Reads `crontab -l` (or `F`) and prints the jobs as TOML. |
| `takt import-plist <file>` | Reads a LaunchAgent plist and prints it as a job in TOML. |

Both commands only read. Copy the output into your jobs file and edit it.

`import-cron` splits a line into steps only when the line is plain arguments, `;`, `>>` and `2>>`. These lines become one `/bin/sh -c` step with the text unchanged:

- shell expansion: `$`, backticks, `*`, `?`, `[`, `{`, `~`, and any backslash escape (`echo \;`)
- an environment prefix (`NAME=value cmd`), or a `2` before `>>` that is quoted or spaced (`echo "2">> f`, `echo 2 >> f`): the `2` is an argument
- a `#` inside the command (`echo abc#def`), and a shell builtin such as `cd`, `export` or `source` (`cd /tmp; ./run`), because the next command depends on it
- a quote next to an operator (`echo ";"`), because the quotes make the operator an argument
- a truncating `>`, pipes and other operators

A line with an unescaped `%` is not imported, because cron sends the text after `%` to the command as input, and a takt job has no input. `import-cron` prints a warning for it. An escaped `\%` becomes `%`. Environment lines such as `PATH=...` and `MAILTO=...` are not imported either. Each one gets a warning, so you can move the values to `[settings] path` or into the commands.

## TUI

The TUI is [fzf](https://github.com/junegunn/fzf) over `takt status -A`. The bottom pane shows `takt show` for the selected job.

| Key | Action |
|---|---|
| Type | Filter the rows. |
| `enter` | Open `takt show` in a pager. |
| `ctrl-r` | Refresh. |
| `ctrl-s` | Start the job. Only with `takt ui --allow-writes`. |
| `ctrl-e` | Enable the job. Only with `takt ui --allow-writes`. |
| `ctrl-x` | Disable the job. Only with `takt ui --allow-writes`. |

The result of a key shows in the footer.

## Checks

| Command | What it does |
|---|---|
| `takt --check` | Runs the self-test. It needs no network and no credentials, and takes about 6 seconds. |
| `takt --version` | Prints the version. |

## Files

| Path | Contents |
|---|---|
| `~/.config/takt/jobs.toml` | The jobs of this machine. `TAKT_CONFIG` changes the directory. |
| `~/.config/takt/jobs.<host>.toml` | The jobs of another host. |
| `~/.local/state/takt/<id>.json` | The last run record of a job. `TAKT_STATE` changes the directory. |
| `~/.local/state/takt/locks/` | One lock file for each lock name. |
| `~/.local/state/takt/log/` | The output of the wrapper under launchd. |
| `~/.local/state/takt/retired/` | Plists that `install` took out of service. |
| `~/.local/state/takt/crontab.bak` | The crontab before `install` changed it. |

## Exit codes of `takt run`

| Code | Meaning |
|---|---|
| 0 | `ok`, or a deliberate skip (missed slot with `catch_up = "skip"`, or already run in this slot). |
| 1 | `partial` or `failed`. |
| 2 | Skipped, because a `needs` check failed. |
| 75 | `lock-timeout`. |

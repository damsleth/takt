# takt

Declare scheduled jobs once. takt runs every job through a wrapper that takes turns with other jobs, records why a job skipped, and reports the sub-step that failed.

One file drives launchd on macOS, systemd on Linux and Task Scheduler on Windows. It does this on your machine and, over ssh, on your other hosts.

## Install

macOS and Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/damsleth/takt/main/install.sh | sh
```

Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/damsleth/takt/main/install.ps1 | iex
```

takt is one Python file with no dependencies. It needs Python 3.11 or later. The installer puts `takt.py` in `~/.local/share/takt/` and the `takt` command in `~/.local/bin/`.

## Start

```sh
takt init                     # write ~/.config/takt/jobs.toml with one example job
takt install                  # show what takt will register; changes nothing
takt install --allow-writes   # register the jobs with the scheduler of this OS
takt                          # open the TUI (needs fzf)
```

To serve a dashboard on a device of your tailnet, add `web = ["myvps"]` to `[settings]` and install on that device. Setup starts the server there. See [Web dashboard](docs/web.md).

Edit `~/.config/takt/jobs.toml` between `init` and `install`. The commands that change the scheduler or another host (`install`, `uninstall`, `push`, `start`, `enable`, `disable`) need `--allow-writes`. Without it, they show the plan and stop. `run` executes a job at once, `init` writes a new jobs file, and `render` writes files to a folder.

## Examples

Two jobs that use the same resource in the same minute. The lock makes the second job wait. `after` makes the refresh run first:

```toml
[job.token-refresh]
schedule = "0 * * * *"
lock = ["browser-profile"]
command = ["/usr/local/bin/refresh-tokens"]

[job.ingest]
schedule = "0 */2 * * *"
lock = ["browser-profile"]
after = ["token-refresh"]
command = ["/usr/local/bin/ingest", "--json"]
```

Install and start a job on another host. `~/.config/takt/jobs.myvps.toml` declares the jobs for `ssh myvps`:

```sh
takt --host myvps install --allow-writes
takt --host myvps start backup --allow-writes
```

See all hosts in one table:

```text
$ takt status -A
HOST   ID            SCHED  STATUS  LAST         NEXT         DETAIL
local  owa-reseed    -      never   -            -
local  yaams-ingest  -      never   -            -
kvps   probe         on     ok      10-02 20:10  10-02 20:15
kwin   probe         on     ok      10-02 22:10  10-02 22:15
```

## Docs

- [Commands](docs/cli.md): every command, the TUI keys, environment variables.
- [Jobs file](docs/jobs.md): all keys, schedules, preflight checks, status values.
- [Web dashboard](docs/web.md): `takt web`, which devices serve it, ssh setup.
- [Hosts](docs/hosts.md): other hosts over ssh, Linux and Windows setup.
- [Design](docs/design.md): how locks, ordering, catch-up and status work, and the measurements.
- [Security](SECURITY.md): what takt trusts, and how to report a problem.

## License

[MIT](LICENSE)

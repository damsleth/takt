# takt: agent guide

takt declares scheduled jobs once and runs them through a wrapper on launchd, systemd and Task Scheduler, locally and on other hosts over ssh. Users read [README.md](README.md). The reference is in [docs/](docs).

## Layout

```
takt.py          the whole tool: one file, Python 3.11+ stdlib only
install.sh       curl | sh installer (macOS, Linux)
install.ps1      irm | iex installer (Windows)
examples/        jobs files; the self-test loads and checks them
docs/            cli.md, jobs.md, hosts.md, web.md, design.md
```

## Contract

- **One file, stdlib only.** The installers and `--host` copy `takt.py` alone. A second module or a dependency breaks both.
- **Python 3.11 or later**, on macOS, Linux and Windows. Guard OS-specific code (`fcntl`, `os.getuid`, `chmod`) and test it on Windows.
- **Scheduler and host changes need `--allow-writes`.** `install`, `uninstall`, `push`, `start`, `enable` and `disable` print their plan without the flag. The TUI action keys exist only under `takt ui --allow-writes`.
- **Nothing is quoted over ssh.** Forwarded arguments must match `SAFE_ARG`. Do not add quoting: the login shell can be `sh` or PowerShell.

## Gate

Run before each commit:

```sh
./takt.py --check
```

It must print `0 failed`. It runs offline in about 6 seconds and spawns real processes for the lock and order tests. A change to logic needs a check that fails when that logic breaks. Make a one-line break on a copy and confirm that the check fails.

For changes to Linux or Windows behavior, also run the check on a real host:

```sh
takt --host <host> push --allow-writes
takt --host <host> --check
```

When you change behavior, update the doc page for it in `docs/`. Keep the README short: pitch, install, start, examples, links.

## Writing

Docs follow ASD-STE100 writing rules: short sentences, active voice, one instruction per sentence, the condition first. No em dashes.

## Git

- Commit in logical groups. The body says what was verified, with numbers.
- No `Co-Authored-By` or other attribution trailers.
- Push only when asked.
- Agent instructions go in this file. Do not add a `CLAUDE.md`.

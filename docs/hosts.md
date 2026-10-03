# Other hosts

takt manages jobs on other machines over ssh. The same `takt.py` runs on each host and drives the scheduler of that host. takt has no agent, no daemon and no network protocol of its own.

## Add a host

1. Make sure that `ssh <host>` connects without a password prompt. takt uses your ssh config, including `ControlMaster` if you set it.
2. Make sure that the host has Python 3.11 or later.
3. Write `~/.config/takt/jobs.<host>.toml`. Use the ssh alias as `<host>`. Set `[settings] python` to the interpreter on that host.
4. Show the plan: `takt --host <host> install`.
5. Install: `takt --host <host> install --allow-writes`.

```sh
takt --host myvps install --allow-writes
takt status -A
```

## How `--host` works

`takt --host <host> <command>` does this:

1. For `push` and `install`: one `scp -r` copies `takt.py` to `~/.local/share/takt/takt.py` and `jobs.<host>.toml` to `~/.config/takt/jobs.toml` on the host.
2. It runs `ssh <host> '<python> .local/share/takt/takt.py <command>'`.
3. It prints the output of the host and returns its exit code.

The host renders its own units with its own paths. The records and the scheduler state come from the host.

The installers put `takt.py` in the same place as `push`. A host where you ran the installer and a host that you push to are the same. A push replaces the copy on the host with the copy on your machine.

Only `push` and `install` copy files. Other commands run the copy that is on the host. After you upgrade takt on your machine, push again to keep the hosts current.

## Argument rules

The host name must be an ssh alias: letters, digits and `. _ @ -`, with no `-` at the start.

The login shell on the host can be `sh` or PowerShell, and ssh joins arguments with spaces. So takt sends no quotes. Every argument after `--host <host>` must be a plain word: letters, digits and `. / : \ = , @ + - _`. takt refuses other arguments before it starts ssh.

## Linux hosts

- Jobs install as systemd user units: `~/.config/systemd/user/takt-<id>.service` and `.timer`.
- Enable lingering, so that the user units run when you are logged out: `loginctl enable-linger $USER`.
- `catch_up = "run-once"` sets `Persistent=true` on the timer.

## Windows hosts

- Install the OpenSSH server and allow key authentication.
- Set `[settings] python` to the full path of `python.exe`. `python3` is often the Microsoft Store stub, which does not run scripts. The installer prints the correct path.
- Jobs install as Task Scheduler tasks in the `\takt\` folder.
- Tasks run as the ssh user, only while that user is logged on. takt stores no password.
- Tasks run with `pythonw.exe`, and each step starts without a window, so no console window opens.
- Use argv lists for commands. A string command runs through `/bin/sh -c`, which Windows does not have.

## Differences between schedulers

| Topic | Behavior |
|---|---|
| Time zone | Each host uses its own clock. `NEXT` in `status -A` shows host time, so two hosts can show different times for the same slot. |
| Start a disabled job | systemd starts the service. Task Scheduler refuses with "could not run because it is disabled". takt shows the error from the scheduler. |
| Trigger in the record | A run from `takt start` shows the trigger `scheduled`, because the scheduler starts the same command as a timed run. |
| Missed slots | launchd and systemd (`Persistent=true`) run a missed slot once after wake. Task Scheduler does the same with `StartWhenAvailable`. |

## Remove a host

```sh
takt --host myvps uninstall --allow-writes
```

This removes the units or tasks. The copy of `takt.py`, the jobs file and the records stay on the host. Delete `~/.local/share/takt`, `~/.config/takt` and `~/.local/state/takt` on the host to remove them.

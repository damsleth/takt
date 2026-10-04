# Web dashboard

`takt web` serves a dashboard of every device in the takt-net. It shows the same rows as `takt status -A`. A click on a row shows the output of `takt show` for that job. The page refreshes itself every 15 seconds.

The server is part of `takt.py`. It has no dependencies, no CDN and no build step.

## Choose the devices

Name the devices that serve the page in the jobs file of the controller:

```toml
[settings]
web = ["myvps"]          # device names. "local" means the controller itself.
# web_port = 8787        # default 8787
# web_writes = false     # true adds start, enable and disable buttons
# [settings.web_bind]    # optional: the address to listen on, for each device
# myvps = "100.64.0.7"
```

Use more than one name to serve the page from more than one device. `push` and `install` copy these settings to each host in `net.toml`, in the same way as `settings.status`. After you change them, run `takt --host <h> install --allow-writes` for each host that serves the page or that stops serving it.

## Install

Setup starts the server. You do not run it by hand.

```sh
takt --host myvps install --allow-writes
```

On a device named in `web`, `install` also installs the web service and starts it:

| OS | Service | Restart |
|---|---|---|
| Linux | systemd user service `takt.web.service`, enabled with `WantedBy=default.target` | `Restart=always`, 10 seconds apart |
| macOS | launchd agent `dev.takt-web`, with `RunAtLoad` | `KeepAlive`, 10 seconds apart |
| Windows | Task Scheduler task `\takt-web\serve`, with a logon trigger | `RestartOnFailure`, every minute, no time limit; runs with `pythonw.exe` |

`takt install` on the controller does the same when `local` or the controller name is in `web`. The plan shows the web service before you add `--allow-writes`.

A device that is not in `web` gets the service retired by the next `install`: the plan stops and removes it. `takt uninstall` without job ids removes the web service too. `takt uninstall <id>` leaves it. The service names lie outside the job id rules, so a job named `web` does not clash with it.

On Linux, enable lingering so that the service runs when you are logged out: `loginctl enable-linger $USER`.

The service writes its output to `~/.local/state/takt/log/web.log` on macOS. On Linux, use `journalctl --user -u takt.web`.

## Address

The server listens on the Tailscale IPv4 address of the device and nowhere else. takt finds it with `tailscale ip -4`. It looks for `tailscale` on `PATH`, then in the macOS app and the Windows install folder.

takt takes the first of these:

1. `--bind <address>` of `takt web`.
2. `[settings.web_bind]` for this device.
3. The Tailscale address.

If none gives an address, the server prints the reason and exits. The service manager starts it again 10 seconds later, so the server comes up when Tailscale does. takt never listens on all interfaces unless you pass `--bind 0.0.0.0`, and it prints a warning then.

The page is at `http://<address>:<port>/`. The MagicDNS name of the device works too.

## Which devices the page shows

A web device is a viewer. It shows every device in the takt-net, also when `settings.status` is `master` and the web device is not the master, or when it is `client`. Only `status = "none"` turns the page off.

`settings.status` still controls `takt status -A` and the TUI on that device. A web device in `master` mode shows all devices on its page and a pointer to the master on the command line.

Each row has a host, a job, `SCHED`, `STATUS`, the last run, the next run and a detail. A device that does not answer shows as one row with the status `unreachable` and the error. The rest of the page works.

## Ssh from the web device

The web device pulls the rows of the other devices over ssh, in the same way as `takt status -A`. It uses the names in `net.toml`. Each name must work as `ssh <name>` from the web device, without a password prompt:

1. Put the ssh public key of the web device in `authorized_keys` on each other device.
2. Add a `Host <name>` entry with `HostName` and `User` to the ssh config of the web device, if the name does not resolve to the right user and address.
3. Connect once from the web device (`ssh <name> true`), so that it knows the host key.
4. Make sure that the other device has `takt.py` and Python 3.11 or later. `takt --host <name> push --allow-writes` does that.

The server runs ssh with `BatchMode=yes` and `ConnectTimeout=8`. It never asks for a password and never waits for a prompt. The controller is a member of the net with the path of its `takt.py`. If you do not run an ssh server on the controller, it shows as unreachable on the other web devices.

takt does not create keys or change the ssh config.

## Write buttons

The server is read-only. Start it with `--allow-writes`, or set `web_writes = true`, to get the buttons `start`, `enable` and `disable` on the job detail. They run the same commands as `takt ui --allow-writes`, on the device of the row. Anyone who can reach the page can press them. Read [SECURITY.md](../SECURITY.md) first.

## Checks that the server makes

- The `Host` header must be an IP address, `localhost`, a name without a dot, or a name that ends in `.ts.net`. This stops a web page on another domain that resolves to the address of the server.
- A write request needs the header `X-Takt: 1`, and an `Origin` header, if present, must be the server. A web page on another origin cannot send this header without a preflight, and the server does not answer preflights.
- The device must be this device or a member of the net. The job id must be a plain id. The action must be one of the three.

## Command

```sh
takt web [--bind ADDRESS] [--port N] [--allow-writes] [--spec FILE] [--state DIR]
```

Run it by hand to try it. Use `--bind 127.0.0.1` to test on one machine. Press `ctrl-c` to stop it.

## Not built

- Login, TLS and users. The tailnet is the boundary. Add them if the page must be open to more than one person.
- Live logs and run history. The page shows the last record, as `takt show` does.

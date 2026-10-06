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

Use more than one name to serve the page from more than one device. `push` and `install` copy these settings to each host in `net.toml`, in the same way as `settings.status`. After you change them, run `takt --host <h> install --allow-writes` for each host that serves the page or that stops serving it, and `takt --host <h> push --allow-writes` for every other host. A device reads the list of web devices from its own `net.toml`, so a device that you do not update sends its rows nowhere.

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

If none gives an address, the server prints the reason and exits. The service manager starts it again 10 seconds later, so the server comes up when Tailscale does. takt never listens on all interfaces unless you pass `--bind 0.0.0.0`, and it prints a warning then. The address must be IPv4: takt refuses an IPv6 address.

The page is at `http://<address>:<port>/`. The MagicDNS name of the device works too.

## Which devices the page shows

A web device is a viewer. It shows every device in the takt-net, also when `settings.status` is `master` and the web device is not the master, or when it is `client`. Only `status = "none"` turns the page off.

`settings.status` still controls `takt status -A` and the TUI on that device. A web device in `master` mode shows all devices on its page and a pointer to the master on the command line.

Each row has a host, a job, `SCHED`, `STATUS`, the last run, the next run and a detail. The rows of this device are read when the page asks. The rows of the other devices come from their last report. The header shows the age of each report, for example `kmbp reported 3 min ago · kwin no report yet`.

## Reports from the other devices

The web device does not log in to other devices. Each device sends its own rows to the web devices:

- After each run of a job from its own jobs file (`~/.config/takt/jobs.toml`), the wrapper sends `POST /api/report` to each web device in `settings.web`. A test run or a run from another `--spec` file does not report.
- `takt report` sends the rows now.
- All sends of one run share a budget of 8 seconds, address lookups included. A web device that is left when the budget is spent is skipped. If the web device does not answer, the job is not affected, and the error goes to the log of the job. The next run sends the full rows again.
- The device finds the web device with `tailscale ip -4 <name>`, so MagicDNS is not necessary. `[settings.web_bind]` overrides the address.
- On macOS with the Tailscale app, set `[settings.web_bind]` for each web device. Under launchd the app binary prints "The Tailscale GUI failed to start" instead of an address, so the lookup fails and every report goes to the bare name, which does not resolve. The job log shows `report to <name>: URLError: ... nodename nor servname provided`.

The web device accepts a report only if Tailscale confirms the sender. It runs `tailscale whois` on the address of the caller. The first part of the machine name (`kmbp` in `kmbp.example.ts.net`) must be the device that the report names, and that device must be a member of the takt-net. If Tailscale does not know the address, or if the `tailscale` command is missing, the report is refused. A report holds rows only. It cannot run anything.

The web device keeps the last report of each device in `<state>/reports/<device>.json`, so a restart keeps them. When a device is asleep or offline, the page shows its last report and the age of that report.

Use the Tailscale machine names of your devices as their names in takt (`jobs.<name>.toml`), so that the names in `tailscale whois` and in takt agree.

The detail pane of a job on another device shows the reported record: status, steps, notes and preflight. The log tails stay on that device. Use `takt --host <name> show <id>` from the controller to see them.

The start, enable and disable buttons for another device still use ssh from the web device (see "Write buttons"). The page itself does not need ssh.

## Write buttons

The server is read-only. Start it with `--allow-writes`, or set `web_writes = true`, to get the buttons `start`, `enable` and `disable` on the job detail. They run the same commands as `takt ui --allow-writes`, on the device of the row. Anyone who can reach the page can press them. Read [SECURITY.md](../SECURITY.md) first.

## Checks that the server makes

- The `Host` header must be an IP address, `localhost`, a name without a dot, or a name that ends in `.ts.net`. This stops a web page on another domain that resolves to the address of the server.
- A write request needs the header `X-Takt: 1`, and an `Origin` header, if present, must be the server. A web page on another origin cannot send this header without a preflight, and the server does not answer preflights.
- The device must be this device or a member of the net. The job id must be a plain id. The action must be one of the three.

## Behind a reverse proxy

To serve the page with TLS on a name of your own, put a reverse proxy on the web device and keep the name inside the tailnet:

- A DNS record for the name points to the Tailscale address of the web device, with no proxying by a CDN.
- The proxy allows only tailnet addresses (`100.64.0.0/10` and `fd7a:115c:a1e0::/48`) and denies all other clients.
- The proxy sends the upstream address as `Host` (in nginx, the default `$proxy_host`), so the `Host` check of takt passes. Do not forward the public name: takt refuses a `Host` with a dot that is not `.ts.net`.
- If `web_writes` is on, the proxy translates the `Origin` of the page to the upstream address. Otherwise takt refuses the buttons, because `Origin` and `Host` differ.

An nginx example, for `takt.example.com` and a web device at `100.64.0.7`:

```nginx
map $http_origin $takt_origin {
    "https://takt.example.com" "http://100.64.0.7:8787";
    default                    $http_origin;
}
server {
    listen 443 ssl;
    server_name takt.example.com;
    allow 100.64.0.0/10;
    allow fd7a:115c:a1e0::/48;
    deny  all;
    location / {
        proxy_pass       http://100.64.0.7:8787;
        proxy_set_header Origin $takt_origin;
    }
}
```

If the proxy trusts a real-IP header (for example from a CDN), make sure that it trusts the header only from the CDN addresses. Otherwise a client can send a tailnet address in the header.

Reports do not go through the proxy. The devices send them to the address and port of `takt web`.

## Command

```sh
takt web [--bind ADDRESS] [--port N] [--allow-writes] [--spec FILE] [--state DIR]
```

Run it by hand to try it. Use `--bind 127.0.0.1` to test on one machine. Press `ctrl-c` to stop it.

## Not built

- Login, TLS and users. The tailnet is the boundary. For TLS, see "Behind a reverse proxy". Add them if the page must be open to more than one person.
- Live logs and run history. The page shows the last record, as `takt show` does.

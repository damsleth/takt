# Security

## Report a problem

Use GitHub private vulnerability reporting: open the **Security** tab of this repository and select **Report a vulnerability**. Do not open a public issue for a security problem.

Include the version (`takt --version`), the OS, and the steps that show the problem.

## What takt trusts

- **The jobs file is code.** Each command in it runs as your user, from your scheduler. Give the jobs files the same protection as your shell profile. Do not install a jobs file that you have not read.
- **ssh access is host access.** `takt --host <host>` runs commands and copies files with your ssh credentials. takt adds no access of its own and stores no passwords.
- **Changes to the scheduler need `--allow-writes`.** Without the flag, `install`, `uninstall`, `push`, `start`, `enable` and `disable` print a plan and change nothing. `run` executes a job at once, `init` writes a new jobs file, and `render` writes files to a folder.
- **A host is an ssh alias.** takt refuses a host name that starts with `-` or contains characters outside letters, digits and `. _ @ -`, so a host name cannot become an ssh option.
- **Arguments to ssh are plain words.** takt refuses an argument that contains characters outside letters, digits and `. / : \ = , @ + - _`, before it starts ssh. This stops shell syntax in a job id or option from running on the host.
- **No network access of its own, except `takt web`, its reports, and `notify`.** takt starts `ssh` and `scp`, and the commands in your jobs files. It calls no other service unless a job sets `notify`.
- **`notify` sends job data out.** A notice holds the device name, the job id, the status and detail of a run, and for a watch the values that it saw. takt sends it only to the URL in `notify`. A topic on ntfy.sh is public: anyone who knows its name can read it. Use a topic name that is hard to guess, or your own ntfy server.
- **`takt web` has no login.** It listens only on the Tailscale address of the device (never on all interfaces, unless you pass `--bind 0.0.0.0`). Every device and user that Tailscale lets reach that address can read the page: all job names, status, error text, and the tails of the logs of every device in the takt-net. The tailnet is the only access control, so check your Tailscale ACLs. The page is plain HTTP, and the traffic is encrypted by Tailscale only.
- **`takt web` is read-only by default.** The start, enable and disable buttons exist only when you start it with `--allow-writes` or set `web_writes = true`. Then anyone who can reach the page can start a job on any device in the net. The server checks the `Host` and `Origin` headers and needs a custom header on writes, so a web page in your browser cannot make those requests. This does not protect against another user on the tailnet.
- **Devices report to the web device; the web device logs in nowhere.** Each device sends its own rows over HTTP to the web devices. The web device accepts a report only when `tailscale whois` names the sending machine as the device in the report, and that device is in the takt-net. A report holds rows only, so it cannot run anything. Only the optional start, enable and disable buttons for another device use ssh from the web device.

## The installers

The installers download `takt.py` from the `main` branch of this repository. To install a fixed version, set `TAKT_REF` to a tag or a commit:

```sh
curl -fsSL https://raw.githubusercontent.com/damsleth/takt/main/install.sh | TAKT_REF=v0.1.0 sh
```

```powershell
$env:TAKT_REF = 'v0.1.0'; irm https://raw.githubusercontent.com/damsleth/takt/main/install.ps1 | iex
```

To read the installer before it runs, download it first and open it.

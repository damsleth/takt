# Security

## Report a problem

Use GitHub private vulnerability reporting: open the **Security** tab of this repository and select **Report a vulnerability**. Do not open a public issue for a security problem.

Include the version (`takt --version`), the OS, and the steps that show the problem.

## What takt trusts

- **The jobs file is code.** Each command in it runs as your user, from your scheduler. Give the jobs files the same protection as your shell profile. Do not install a jobs file that you have not read.
- **ssh access is host access.** `takt --host <host>` runs commands and copies files with your ssh credentials. takt adds no access of its own and stores no passwords.
- **Writes need `--allow-writes`.** Without the flag, `install`, `uninstall`, `push`, `start`, `enable` and `disable` print a plan and change nothing.
- **Arguments to ssh are plain words.** takt refuses an argument that contains characters outside letters, digits and `. / : \ = , @ + - _`, before it starts ssh. This stops shell syntax in a job id or option from running on the host.
- **No network access of its own.** takt starts `ssh` and `scp`, and the commands in your jobs files. It does not call other services.

## The installers

The installers download `takt.py` from the `main` branch of this repository. To install a fixed version, set `TAKT_REF` to a tag or a commit:

```sh
curl -fsSL https://raw.githubusercontent.com/damsleth/takt/main/install.sh | TAKT_REF=v0.1.0 sh
```

```powershell
$env:TAKT_REF = 'v0.1.0'; irm https://raw.githubusercontent.com/damsleth/takt/main/install.ps1 | iex
```

To read the installer before it runs, download it first and open it.

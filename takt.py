#!/usr/bin/env python3
"""takt: declare scheduled jobs once, run them through a wrapper, on every host you have.

One TOML file per host declares jobs. Every job executes via `takt run <id>`, which takes
named locks, pulls in `after` dependencies, runs preflight checks, runs the steps, and
records per-job status including failing sub-steps. The same spec installs as launchd
plists (macOS), systemd user units (Linux) or Task Scheduler tasks (Windows).

Jobs live in ~/.config/takt/jobs.toml ($TAKT_CONFIG overrides the directory). Another
host is ~/.config/takt/jobs.<host>.toml: `takt --host <host> ...` copies this file and
that spec to the host over ssh and runs the command there.

    takt init                                  write a starter jobs.toml
    takt                                       TUI over every host (= takt ui)
    takt status [-A] [--json]                  scheduler state + last run; -A = every host
    takt install [--allow-writes]              plan, then register with the scheduler
    takt start|enable|disable <id>             through the native scheduler (--allow-writes)
    takt --host myvps install --allow-writes   the same, on another host
    takt web [--allow-writes]                  dashboard on the tailnet (install runs it where settings.web says)
    takt --check                               offline self-test, no auth, no network

Docs: https://github.com/damsleth/takt/tree/main/docs
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import math
import os
import plistlib
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import product
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

try:
    import fcntl
except ImportError:  # Windows: msvcrt byte-range lock, same "dies with the process" semantics
    fcntl = None
    import msvcrt

__version__ = "0.1.0"
HERE = Path(__file__).resolve().parent
CONFIG = Path(os.environ.get("TAKT_CONFIG") or Path.home() / ".config/takt")
PREFIX = "dev.takt"
DEFAULT_PATH = "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"
SKIP_AFTER = 300  # catch_up="skip": a scheduled start later than this past its slot is dropped
RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]
ALIASES = {"@hourly": "0 * * * *", "@daily": "0 0 * * *", "@weekly": "0 0 * * 0",
           "@monthly": "0 0 1 * *"}
ID_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")  # no leading "-": an id is an argv word
# Windows device names: `NUL.lock` is the null device, not a file. Rejected everywhere, so a jobs
# file stays valid on every OS.
RESERVED = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}


def file_name_ok(name):
    """A job id or lock name becomes a file name: plain characters, no Windows device name."""
    return isinstance(name, str) and bool(ID_RE.match(name)) and name.lower() not in RESERVED
NO_WINDOW = {"creationflags": 0x08000000} if os.name == "nt" else {}  # CREATE_NO_WINDOW


# ---------------------------------------------------------------- cron fields

def parse_cron(expr: str) -> list:
    """Five fields, each None (= every value) or a sorted list of ints.
    ponytail: numbers, ranges, lists and steps only (no MON/JAN names); dom and
    dow may not both be restricted (cron ORs them, launchd/systemd would AND)."""
    expr = ALIASES.get(expr.strip(), expr)
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron needs 5 fields: {expr!r}")
    out = []
    for i, (p, (lo, hi)) in enumerate(zip(parts, RANGES)):
        dow = i == 4
        top = 7 if dow else hi
        vals = set()
        for item in p.split(","):
            m = re.fullmatch(r"(\*|\d+(?:-\d+)?)(?:/(\d+))?", item)
            if not m:
                raise ValueError(f"bad cron field {p!r} in {expr!r}")
            base, step = m.groups()
            if base == "*":
                a, b = lo, hi
            elif "-" in base:
                a, b = map(int, base.split("-"))
            else:
                a = b = int(base)
                if step:
                    b = top
            s = int(step or 1)
            if s < 1 or a < lo or b > top or a > b:
                raise ValueError(f"cron value out of range in {p!r}")
            vals.update(range(a, b + 1, s))
        if dow:
            vals = {v % 7 for v in vals}
        out.append(None if vals == set(range(lo, hi + 1)) else sorted(vals))
    if out[2] is not None and out[4] is not None:
        raise ValueError("dom and dow both restricted is not supported")
    return out


def compress(vals, lo, hi) -> str:
    if vals is None:
        return "*"
    if len(vals) > 1:
        s = vals[1] - vals[0]
        if s > 1 and vals[0] == lo and vals == list(range(lo, hi + 1, s)):
            return f"*/{s}"
    return ",".join(map(str, vals))


def fields_to_cron(f) -> str:
    return " ".join(compress(v, lo, hi) for v, (lo, hi) in zip(f, RANGES))


def matches(f, t: datetime) -> bool:
    dow = (t.weekday() + 1) % 7
    want = [t.minute, t.hour, t.day, t.month, dow]
    return all(v is None or w in v for v, w in zip(f, want))


# 29 February is the sparsest date a valid schedule can name. 8 years and a few days cover
# a skipped leap year (2100), so a slot that exists is always found.
SEARCH_DAYS = 366 * 8 + 2


def _day_ok(f, d):
    return ((f[2] is None or d.day in f[2]) and (f[3] is None or d.month in f[3])
            and (f[4] is None or (d.weekday() + 1) % 7 in f[4]))


def _times(f):
    return [(h, m) for h in (f[1] or range(24)) for m in (f[0] or range(60))]


def prev_slot(f, now: datetime, days=SEARCH_DAYS):
    """The latest slot at or before `now`, searched day by day."""
    t = now.replace(second=0, microsecond=0)
    times = _times(f)[::-1]
    for i in range(days):
        d = t - timedelta(days=i)
        if _day_ok(f, d):
            for h, m in times:
                c = d.replace(hour=h, minute=m)
                if c <= t:
                    return c
    return None


def next_slot(f, now: datetime, days=SEARCH_DAYS):
    """The first slot after `now`."""
    t = now.replace(second=0, microsecond=0)
    times = _times(f)
    for i in range(days):
        d = t + timedelta(days=i)
        if _day_ok(f, d):
            for h, m in times:
                c = d.replace(hour=h, minute=m)
                if c > t:
                    return c
    return None


# ------------------------------------------------------------------- the spec

WATCH_KEYS = ("compare", "target", "expect", "cooldown", "flap_max", "flap_window")
STEP_KEYS = ("command", "stdout", "stderr", "report", "timeout") + WATCH_KEYS
REPORTS = ("json-failed-sources", "watch")
COMPARE = ("changed", "above", "below", "equals", "new-items")
NOTIFY_RE = re.compile(r"ntfy://[\w-]+|https?://\S+")


def _argv(c):
    return ["/bin/sh", "-c", c] if isinstance(c, str) else list(c)


def notify_ok(n):
    if not (isinstance(n, str) and NOTIFY_RE.fullmatch(n)):
        return False
    if n.startswith("ntfy://"):
        return True
    try:
        return bool(urlsplit(n).hostname)
    except ValueError:  # "https://[" and other URLs that urllib cannot build a request for
        return False


def seconds(v, what):
    """90, "90s", "10m", "2h" or "1d" -> seconds."""
    if isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0:
        return float(v)
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", str(v).strip())
    if not m:
        raise ValueError(f"{what}: {v!r} is not a duration (90, \"90s\", \"10m\", \"2h\", \"1d\")")
    return float(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]


def _norm_step(s, sid, jid="?"):
    sid = s.get("id", sid)
    where = f"job {jid} step {sid}"
    step = {"id": sid, "command": _argv(s["command"]), "stdout": s.get("stdout"), "stderr": s.get("stderr"),
            "report": s.get("report"), "timeout": seconds(s["timeout"], f"{where}: timeout") if "timeout" in s else None}
    if step["report"] not in (None,) + REPORTS:
        raise ValueError(f"{where}: report must be one of {', '.join(REPORTS)}")
    if step["report"] != "watch":
        if extra := [k for k in WATCH_KEYS if k in s]:
            raise ValueError(f"{where}: {', '.join(extra)} need report = \"watch\"")
        return step
    cmp, target = s.get("compare", "changed"), s.get("target")
    if cmp not in COMPARE:
        raise ValueError(f"{where}: compare must be one of {', '.join(COMPARE)}")
    if cmp in ("above", "below") and (not isinstance(target, (int, float)) or isinstance(target, bool)):
        raise ValueError(f"{where}: compare = \"{cmp}\" needs a number as target")
    if cmp == "equals" and target is None:
        raise ValueError(f"{where}: compare = \"equals\" needs a target")
    try:
        re.compile(s.get("expect") or "")
    except re.error as e:
        raise ValueError(f"{where}: expect is not a regular expression: {e}") from None
    step.update(compare=cmp, target=str(target) if cmp == "equals" else target, expect=s.get("expect"),
                cooldown=seconds(s.get("cooldown", 0), f"{where}: cooldown"), flap_max=int(s.get("flap_max", 0)),
                flap_window=seconds(s.get("flap_window", 3600), f"{where}: flap_window"))
    if step["timeout"] is None:
        step["timeout"] = 60.0  # a watch reads a network source: a hung curl must not hold the locks
    return step


def normalize(jid, j) -> dict:
    if not file_name_ok(jid):
        raise ValueError(f"job id {jid!r}: use letters, digits, - and _, and no Windows device name (CON, NUL, COM1)")
    for name in j.get("lock", []):  # a lock name is a file name under <state>/locks
        if not file_name_ok(name):
            raise ValueError(f"job {jid}: lock name {name!r}: use letters, digits, - and _, and no Windows device name")
    steps = [_norm_step(s, f"step{i + 1}", jid) for i, s in enumerate(j.get("step", []))]
    if "command" in j:
        steps = [_norm_step({**j, "id": "main"}, "main", jid)]
    if len({s["id"] for s in steps}) != len(steps):  # a watch keeps its state under the step id
        raise ValueError(f"job {jid}: two steps have the same id")
    if not steps:
        raise ValueError(f"job {jid}: no command or step")
    job = {"id": jid, "schedule": j.get("schedule"), "run_at_load": bool(j.get("run_at_load")),
           "catch_up": j.get("catch_up", "run-once"), "lock": list(j.get("lock", [])),
           "after": list(j.get("after", [])), "needs": list(j.get("needs", [])),
           "wants": list(j.get("wants", [])), "bundle": j.get("bundle"),
           "lock_timeout": int(j.get("lock_timeout", 900)), "replaces": list(j.get("replaces", [])),
           "on_event": j.get("on_event"), "notify": j.get("notify"),
           "notify_after": int(j.get("notify_after", 1)), "steps": steps}
    if job["notify"] is not None and not notify_ok(job["notify"]):
        raise ValueError(f"job {jid}: notify is ntfy://<topic> or an http(s):// URL with a host")
    if job["notify_after"] < 1:
        raise ValueError(f"job {jid}: notify_after must be 1 or more")
    for n in job["needs"] + job["wants"]:  # "kind:arg", or a command as an argv list that passes on exit 0
        if not (isinstance(n, str) or (isinstance(n, list) and n and all(isinstance(x, str) for x in n))):
            raise ValueError(f"job {jid}: a need is \"kind:arg\" or an argv list, not {n!r}")
    if job["on_event"] is not None and not isinstance(job["on_event"], str):
        raise ValueError(f"job {jid}: on_event is an event query (XML text)")
    if job["catch_up"] == "skip" and (job["run_at_load"] or job["on_event"]):
        # the scheduler starts every trigger with the same command, so a logon or event start would
        # look like a late scheduled start and be dropped
        raise ValueError(f"job {jid}: catch_up = \"skip\" cannot tell a missed slot from a logon or event start; "
                         "use run-once with run_at_load or on_event")
    if job["schedule"] and next_slot(parse_cron(job["schedule"]), datetime(2000, 1, 1)) is None:
        raise ValueError(f"job {jid}: schedule {job['schedule']!r} never matches a date")
    if job["catch_up"] not in ("run-once", "skip"):
        raise ValueError(f"job {jid}: catch_up must be run-once or skip")
    return job


def read_toml(path) -> dict:
    return tomllib.loads(Path(path).read_text(encoding="utf-8"))  # TOML is UTF-8; a Windows locale default is not


UPDATE_ID = "takt-update"
UPDATE_SCHEDULE = "7 * * * *"  # hourly, clear of minute 0
UPDATE_URL = "https://raw.githubusercontent.com/damsleth/takt/{ref}/takt.py"


def update_job(cfg, spec_path):
    """The job that every jobs file gets unless `[settings] update = false`: `takt update`, which
    installs the takt.py on GitHub (settings.update_ref, default main) when it differs."""
    sched = cfg.get("update", True)
    sched = UPDATE_SCHEDULE if sched is True else sched
    if not isinstance(sched, str):
        raise ValueError("settings.update: a cron schedule, true, or false")
    py = cfg.get("python") or (sys.executable if os.name == "nt" else shutil.which("python3") or sys.executable)
    return normalize(UPDATE_ID, {"schedule": sched, "lock": ["takt-update"],
                                 "command": [py, str(Path(__file__).resolve()), "update", "--spec",
                                             str(Path(spec_path).resolve())]})


def load_spec(path) -> dict:
    raw = read_toml(path)
    cfg = raw.get("settings", {})
    spec = {jid: normalize(jid, j) for jid, j in raw.get("job", {}).items()}
    if UPDATE_ID not in spec and cfg.get("update", True) is not False and not os.environ.get("TAKT_NO_UPDATE_JOB"):
        spec[UPDATE_ID] = update_job(cfg, path)
    if (tok := cfg.get("notify_token")) is not None:
        if not isinstance(tok, str):
            raise ValueError("settings.notify_token: the path of a file that holds the token")
        for j in spec.values():
            j["notify_token"] = tok
    if len({j.lower() for j in spec}) != len(spec):  # records, markers and units would share a file
        raise ValueError("two job ids differ only by case: " + ", ".join(sorted(spec)))
    for j in spec.values():
        for a in j["after"]:
            if a not in spec:
                raise ValueError(f"job {j['id']}: after unknown job {a!r}")
    for jid in spec:
        chain_of(spec, jid)  # raises on a cycle
    return spec


def chain_of(spec, jid, stack=()):
    """`after` dependencies of jid, dependencies first."""
    if jid in stack:
        raise ValueError(f"after cycle: {' -> '.join(stack + (jid,))}")
    out = []
    for a in spec[jid]["after"]:
        for d in chain_of(spec, a, stack + (jid,)) + [a]:
            if d not in out:
                out.append(d)
    return out


def toml_val(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, list):
        return "[" + ", ".join(toml_val(x) for x in v) + "]"
    return json.dumps(v)


def toml_job(j) -> str:
    base = f"job.{j['id']}"
    out = [f"[{base}]"]
    if j["schedule"]:
        out.append(f"schedule = {toml_val(j['schedule'])}")
    if j["run_at_load"]:
        out.append("run_at_load = true")
    if j["catch_up"] != "run-once":
        out.append(f"catch_up = {toml_val(j['catch_up'])}")
    for k in ("lock", "after", "needs", "wants", "replaces"):
        if j[k]:
            out.append(f"{k} = {toml_val(j[k])}")
    if j["bundle"]:
        out.append(f"bundle = {toml_val(j['bundle'])}")
    if j["lock_timeout"] != 900:
        out.append(f"lock_timeout = {j['lock_timeout']}")
    if j.get("notify"):
        out.append(f"notify = {toml_val(j['notify'])}")
    if j.get("notify_after", 1) != 1:
        out.append(f"notify_after = {j['notify_after']}")
    single = len(j["steps"]) == 1 and j["steps"][0]["id"] == "main"
    for s in j["steps"]:
        if not single:
            out += ["", f"[[{base}.step]]", f"id = {toml_val(s['id'])}"]
        for k in STEP_KEYS:
            if s.get(k) not in (None, "", []):  # a target of 0 is a value
                out.append(f"{k} = {toml_val(s[k])}")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------- state, locks

def state_dir(a=None) -> Path:
    """Absolute: a relative --state or TAKT_STATE would resolve against the scheduler's
    working directory, and scheduled and manual runs would then use different locks."""
    return Path(getattr(a, "state", None) or os.environ.get("TAKT_STATE")
                or Path.home() / ".local/state/takt").expanduser().absolute()


def read_record(state: Path, jid):
    try:
        return json.loads((state / f"{jid}.json").read_text())
    except (OSError, ValueError):
        return None


def write_record(state: Path, rec):
    state.mkdir(parents=True, exist_ok=True)
    tmp = state / f".{rec['id']}.{os.getpid()}.json.tmp"  # per writer: two writers never share it
    tmp.write_text(json.dumps(rec, indent=1) + "\n")
    os.replace(tmp, state / f"{rec['id']}.json")


def _trylock(f):
    if fcntl:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)


LOCK_FDS = []  # fds of the locks this process holds; each step inherits them (POSIX)
CONTAIN = os.name == "nt"  # Windows: run steps in a job object (end_steps_with_wrapper)


def inherit_locks():
    """subprocess kwargs. A step inherits the lock fds, and flock belongs to the open file, so
    the lock stays held until the wrapper and the step have both exited. Killing the wrapper
    alone cannot free a lock that its still-running step uses. ponytail: POSIX only; on Windows
    the lock ends with the wrapper process."""
    return {"pass_fds": list(LOCK_FDS)} if fcntl and LOCK_FDS else {}


class Locks:
    """Named flocks, taken in sorted order so two jobs can never deadlock.
    flock dies with the last process holding it, so a crashed job cannot wedge the lock."""

    def __init__(self, state: Path, names, timeout, sub="locks", cleanup=True):
        # lowercase: on macOS and Windows "JOB-j" and "job-j" are one file, and taking it twice would
        # wait on itself. cleanup=False: a short inner lock (the notify state) that must not end the
        # job's leftovers in the middle of a run, before the caller of a pulled-in dependency runs.
        self.dir, self.names, self.timeout, self.fds, self.blocked_on, self.cleanup = (
            state / sub, sorted({n.lower() for n in names}), timeout, [], None, cleanup)

    def __enter__(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        for n in self.names:
            f = open(self.dir / f"{n}.lock", "a+")
            while True:
                try:
                    _trylock(f)
                    break
                except OSError:  # BlockingIOError (flock) or PermissionError (msvcrt)
                    self.blocked_on = self.blocked_on or n
                    if time.time() - t0 > self.timeout:
                        f.close()
                        self.__exit__()
                        raise TimeoutError(n)
                    time.sleep(0.05)
            self.fds.append(f)
            LOCK_FDS.append(f.fileno())
        self.waited = round(time.time() - t0, 2)
        return self

    def __exit__(self, *a):
        if self.fds and self.cleanup:
            for hook in BEFORE_UNLOCK:  # Windows: end the job's leftovers first
                hook(self)
        for f in self.fds:
            if f.fileno() in LOCK_FDS:
                LOCK_FDS.remove(f.fileno())
            if not fcntl:
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
            f.close()  # closing releases the flock
        self.fds = []


def lock_names(spec, jid):
    """Own locks + an implicit per-job lock + the same for every `after` dep, so a
    pulled-in dependency cannot run twice at once."""
    names = set()
    for j in [jid] + chain_of(spec, jid):
        names |= set(spec[j]["lock"]) | {f"job-{j}"}
    return names


# ------------------------------------------------------------------- preflight

def parse_owa_status(text):
    """`owa-piggy status` blocks: profile:, authtoken: expires <ISO> (..), refreshtoken: ..."""
    out, cur = {}, None
    for line in text.splitlines():
        k, _, v = line.partition(":")
        k, v = k.strip(), v.strip()
        if k == "profile":
            cur = out[v] = {"auth": None, "refresh": None, "disabled": False}
        elif cur is not None and k in ("authtoken", "refreshtoken"):
            m = re.search(r"expires (\S+)", v)
            if m:
                cur["auth" if k == "authtoken" else "refresh"] = datetime.fromisoformat(
                    m.group(1).replace("Z", "+00:00"))
        elif cur is not None and k == "status" and v == "disabled":
            cur["disabled"] = True
    return out


def parse_owa_json(text):
    """`owa-piggy status --json` -> {profile: {"ok": token valid, "reseed": state or None,
    "fails": n, "max": n, "minutes": refresh-token minutes left}}. None if it is not that JSON."""
    try:
        profiles = json.loads(text)["profiles"]
        out = {}
        for p in profiles:
            rs, rt = p.get("reseed") or {}, p.get("refresh_token") or {}
            out[p["profile"]] = {"ok": p.get("state") == "ok", "reseed": rs.get("state"), "fails": rs.get("fails"),
                                 "max": rs.get("max_fails"), "minutes": rt.get("minutes_remaining")}
        return out
    except (ValueError, KeyError, TypeError):
        return None


def owa_status():
    """Reseed health from `owa-piggy status --json` (owa-piggy with reseed health), else the
    token expiry from the text status of an older owa-piggy."""
    cmd = os.environ.get("TAKT_OWA_PIGGY", "owa-piggy")
    try:
        r = subprocess.run([cmd, "status", "--json"], capture_output=True, text=True, timeout=60)
        js = parse_owa_json(r.stdout)
        if js is not None:
            return {"json": js}
        r = subprocess.run([cmd, "status"], capture_output=True, text=True, timeout=60)
        return parse_owa_status(r.stdout)
    except (OSError, subprocess.SubprocessError):
        return None


def need_label(need):
    return need if isinstance(need, str) else ("cmd " + " ".join([Path(need[0]).name] + need[1:]))[:72]


def check_need(need, cache):
    if isinstance(need, list):  # a command: passes on exit 0
        try:
            r = subprocess.run(need, capture_output=True, text=True, timeout=60, **NO_WINDOW)
        except (OSError, subprocess.SubprocessError) as e:
            return False, f"{need[0]}: {e}"
        last = ((r.stdout or "") + (r.stderr or "")).strip().splitlines()
        return r.returncode == 0, f"exit {r.returncode}" + (f": {last[-1][:120]}" if last else "")
    kind, _, arg = need.partition(":")
    if kind == "fda":
        path = Path(arg or "~/Library/Messages/chat.db").expanduser()
        try:
            with open(path, "rb") as f:
                f.read(1)
            return True, f"can read {path}"
        except PermissionError:
            return False, (f"no Full Disk Access: grant it to {os.path.realpath(sys.executable)} "
                           f"(the binary launchd/cron runs, not the terminal) to read {path}")
        except OSError as e:
            return False, f"cannot read {path}: {e.strerror}"
    if kind == "exe":
        p = shutil.which(arg)
        return (True, p) if p else (False, f"{arg} not on PATH")
    if kind == "owa":
        if "owa" not in cache:
            cache["owa"] = owa_status()
        st = cache["owa"]
        if st is None:
            return False, "owa-piggy status unavailable"
        if "json" in st:  # reseed health: a token can look valid while its reseed needs a sign-in
            p = st["json"].get(arg)
            if p is None:
                return False, f"profile {arg} unknown"
            if not p["ok"]:
                return False, f"needs sign-in: no valid token for {arg} (owa-piggy setup --profile {arg})"
            if p["reseed"] in ("needs_signin", "backed_off"):
                return False, (f"needs sign-in: reseed {p['reseed'].replace('_', ' ')} ({p['fails']}/{p['max']}) "
                               f"for {arg} (owa-piggy setup --profile {arg})")
            left = f"token valid for {p['minutes']} min" if p["minutes"] is not None else "token valid"
            return True, left + ("" if p["reseed"] == "ok" else f", reseed {p['reseed'] or 'not reported'}")
        p = st.get(arg)
        if p is None or p["disabled"]:
            return False, f"profile {arg} unknown or disabled"
        now = datetime.now(timezone.utc)
        exp = max([t for t in (p["auth"], p["refresh"]) if t] or [now - timedelta(1)])
        if exp <= now:
            return False, f"needs sign-in: no valid token for {arg} (owa-piggy setup --profile {arg})"
        return True, f"token valid for {int((exp - now).total_seconds() // 60)} min"
    return False, f"unknown need kind {need!r}"


def preflight(job, cache=None):
    cache = {} if cache is None else cache
    rows = []
    for hard, key in ((True, "needs"), (False, "wants")):
        for n in job[key]:
            ok, detail = check_need(n, cache)
            rows.append({"need": need_label(n), "hard": hard, "ok": ok, "detail": detail})
    return rows


# ------------------------------------------------------------------------ run

def yaams_failed(out: str):
    """Last JSON line of a step's stdout -> (failed sources, error code)."""
    for line in reversed(out.strip().splitlines()):
        if line.startswith("{"):
            try:
                obj = json.loads(line)
            except ValueError:
                return [], None
            err = obj.get("error") if isinstance(obj, dict) else None
            if not isinstance(err, dict):  # {"error": "connection refused"}: report it, do not crash the wrapper
                return [], (f"report: {str(err)[:200]}" if err else None)
            fs = err.get("failed_sources") or []
            return [str(x) for x in (fs if isinstance(fs, list) else [fs])], err.get("code")
    return [], None


def _open_log(path):
    if not path:
        return None
    p = Path(os.path.expanduser(path))
    p.parent.mkdir(parents=True, exist_ok=True)
    return open(p, "ab")


def watch_value(step, text):
    """A watch step's stdout -> (value, None), or (None, why it is not a reading). A broken read
    is a failed step and is never compared, so an expired token or an error page is not a change."""
    out = text.strip()
    if not out:
        return None, "empty output"
    if step["expect"] and not re.search(step["expect"], out):
        return None, f"output does not match expect: {out[:60]!r}"
    if step["compare"] in ("above", "below"):
        try:
            if not math.isfinite(float(out)):  # nan and inf parse, and would re-arm a threshold
                raise ValueError
        except ValueError:
            return None, f"not a finite number: {out[:60]!r}"
    if step["compare"] == "new-items":
        return "\n".join(dict.fromkeys(ln.strip() for ln in out.splitlines() if ln.strip())), None
    return out, None


def kill_tree(pid):
    """End a timed-out step and everything it started (a hung curl under sh). The step stays in
    the wrapper's process group, so launchd still ends it with the wrapper; this walks the
    children instead. ponytail: a child started between the ps and the kill survives."""
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True, **NO_WINDOW)
        return
    kids = {}
    try:
        for ln in subprocess.run(["ps", "-A", "-o", "pid=", "-o", "ppid="], capture_output=True, text=True).stdout.splitlines():
            c, par = map(int, ln.split())
            kids.setdefault(par, []).append(c)
    except (OSError, ValueError):
        pass
    todo = [pid]
    while todo:
        q = todo.pop()
        todo += kids.get(q, [])
        try:
            os.kill(q, 9)  # SIGKILL; signal.SIGKILL does not exist on Windows
        except OSError:
            pass


DRAIN_S = 5  # seconds to read what a killed step left in its pipe


def run_step(step):
    t0 = time.time()
    rec = {"id": step["id"], "exit": None, "failed": [], "note": None}
    out_f = err_f = None
    cap, broken = step["report"] is not None, False
    try:
        out_f = _open_log(step["stdout"])
        err_f = _open_log(step["stderr"])
        p = subprocess.Popen(step["command"], stdout=subprocess.PIPE if cap else (out_f or None),
                             stderr=err_f or None, **NO_WINDOW, **inherit_locks())
        try:
            out, _ = p.communicate(timeout=step.get("timeout"))
        except subprocess.TimeoutExpired:
            kill_tree(p.pid)
            try:
                out, _ = p.communicate(timeout=DRAIN_S)
            except subprocess.TimeoutExpired:  # a child that left the tree (its parent exited) holds the pipe
                if p.stdout:
                    p.stdout.close()
                p.wait()
                out = b""
            broken, rec["note"] = True, f"timed out after {step['timeout']:g}s"
        rec["exit"] = p.returncode
        if cap:
            if out_f:
                out_f.write(out)
            text = out.decode(errors="replace")
            if broken:
                pass
            elif step["report"] == "json-failed-sources":
                rec["failed"], rec["note"] = yaams_failed(text)
            elif rec["exit"] == 0:
                rec["_value"], why = watch_value(step, text)
                broken, rec["note"] = why is not None, why
    except OSError as e:
        rec["exit"], rec["note"] = 127, f"{e.filename or step['command'][0]}: {e.strerror}"
    finally:
        for f in (out_f, err_f):
            if f:
                f.close()
    rec["duration_s"] = round(time.time() - t0, 2)
    rec["status"] = "partial" if rec["failed"] else ("ok" if rec["exit"] == 0 and not broken else "failed")
    return rec


def job_status(steps):
    bad = [s for s in steps if s["status"] != "ok"]
    if not bad:
        return "ok"
    return "failed" if all(s["status"] == "failed" for s in steps) else "partial"


def _execute(job, slot, trigger, pulled_by, pre):
    t0 = time.time()
    started = datetime.now().isoformat(timespec="seconds")
    steps = [run_step(s) for s in job["steps"]]  # `;` semantics: a failed step does not stop the next
    st = job_status(steps)
    return {"id": job["id"], "slot": slot.isoformat(), "trigger": trigger, "pulled_by": pulled_by,
            "started": started, "duration_s": round(time.time() - t0, 2), "status": st,
            "exit": 0 if st == "ok" else 1, "steps": steps, "preflight": pre}


# -------------------------------------------------------------- watch, notify

BAD = ("failed", "partial", "skipped", "lock-timeout")
# new-items forgets the oldest told items past this. ponytail: an item that is still printed after
# ITEMS_MAX newer ones is told again; keep a last-seen time per item if a source ever prints that many
ITEMS_MAX = 10000


def short(v, n=80):
    v = (v or "").replace("\n", " | ")
    return v if len(v) <= n else v[:n - 1] + "…"


def in_state(step, value):
    if value is None:
        return False
    if step["compare"] == "above":
        return float(value) > step["target"]
    if step["compare"] == "below":
        return float(value) < step["target"]
    return value == step["target"]  # equals


def watch(step, st, value, now, send):
    """Compare one good reading with what the user was last told. st is this step's saved state,
    changed in place. -> the step note. Edge-triggered: a value that stays changed, or stays over a
    threshold, is told once. Cooldown defers a notice without losing it. More than flap_max moves
    inside flap_window become one digest at the end of the window."""
    first = "last" not in st
    cooled = st.get("told_at") is None or now - st["told_at"] >= step["cooldown"]
    if step["compare"] == "new-items":
        items = st.setdefault("items", {})  # item -> told, in the order first seen; a new watch seeds silently
        for i in value.splitlines():
            items.setdefault(i, first)
        st["last"] = "seen"
        pending = [i for i, told in items.items() if not told]
        for i in [i for i, told in items.items() if told][:max(0, len(items) - ITEMS_MAX)]:
            del items[i]
        if not pending:
            return "baseline" if first else "no new items"
        if not cooled:
            return f"{len(pending)} new, cooldown"
        more = f"\n… and {len(pending) - 10} more" if len(pending) > 10 else ""
        if not send(f"{len(pending)} new:\n" + "\n".join(pending[:10]) + more):
            return f"{len(pending)} new, notify failed"
        for i in pending:
            items[i] = True
        st["told_at"] = now
        return f"{len(pending)} new"
    changed = step["compare"] == "changed"
    if first:
        # `changed` seeds silently: a new watch must not fire on install. A threshold seeds as "not
        # in state", so a value already over the line on the first read is told once.
        st.update(last=value, told=value, told_in=False, moves=[])
        if changed:
            return "baseline"
    last = None if first else st["last"]
    moved = not first and (value != last if changed else in_state(step, value) != in_state(step, last))
    st["last"] = value
    st["moves"] = [t for t in st.get("moves", []) + ([now] if moved else []) if t > now - step["flap_window"]]
    if st.get("flap_until") is None and moved and step["flap_max"] and len(st["moves"]) > step["flap_max"]:
        # entering needs a real move; the digest counts every move inside the window from here
        st.update(flap_until=now + step["flap_window"], flap_from=st["told"], flap_moves=len(st["moves"]))
        return "flapping"
    if st.get("flap_until") is not None:
        st["flap_moves"] = st.get("flap_moves", 0) + moved
        if now < st["flap_until"] or not cooled:  # the digest is a notice too: it waits for the cooldown
            return "flapping"
        if not send(f"moved {st['flap_moves']} times while flapping; was {short(st['flap_from'])}, now {short(value)}"):
            return "flapping, notify failed"
        st.update(told_at=now, told=value, told_in=not changed and in_state(step, value), flap_until=None,
                  flap_from=None, flap_moves=0)
        return "flap digest"
    if changed:
        want, text = value != st["told"], f"{short(st['told'])} -> {short(value)}"
    else:
        now_in = in_state(step, value)
        if not now_in:
            st["told_in"] = False  # re-arm silently
        want = now_in and not st["told_in"]
        text = (f"{short(value)} is {step['compare']} {step['target']:g}" if step["compare"] != "equals"
                else f"value is now {short(value)}")
    if not want:
        return "no change" if changed else ("in state" if st["told_in"] else "not in state")
    if not cooled:
        return f"{text}, cooldown"
    if not send(text):
        return f"{text}, notify failed"
    st.update(told_at=now, told=value, told_in=not changed)
    return text


def deliver(url, title, text, token=None):
    """POST one notice. ntfy://topic is https://ntfy.sh/topic. token: the path of a file that holds
    a bearer token (settings.notify_token); it is read at each send. -> None, or why it failed."""
    if url.startswith("ntfy://"):
        url = "https://ntfy.sh/" + url[len("ntfy://"):]
    headers = {"Title": title.encode("ascii", "replace").decode()}
    try:
        if token:
            headers["Authorization"] = "Bearer " + Path(os.path.expanduser(token)).read_text(encoding="utf-8").strip()
        req = urllib.request.Request(url, data=text.encode("utf-8"), method="POST", headers=headers)
        urllib.request.urlopen(req, timeout=15).read()
        return None
    except (OSError, ValueError) as e:
        return str(e)[:200]


DELIVER = [deliver]  # the self-check swaps this; nothing else does


def finish(job, rec, state, now=None, say=print):
    """Write the record of a run, after the watch steps and the failure streak have had their say.
    Nothing is sent without `notify`; the notes still show what would have been told. A notice that
    fails to send is kept and sent by a later run."""
    now = (now or datetime.now()).timestamp()
    sent, errors = [], []

    def send(text):
        if not job.get("notify"):
            return True  # told in the note only
        if err := DELIVER[0](job["notify"], f"takt {self_name()}: {job['id']}", text, job.get("notify_token")):
            errors.append(err)
            return False
        sent.append(short(text, 200))
        return True

    try:
        with Locks(state, [job["id"]], 30, sub="notify", cleanup=False):  # a lock-timeout run writes it too
            f = state / "notify" / f"{job['id'].lower()}.json"
            try:
                ns = json.loads(f.read_text())
            except (OSError, ValueError):
                ns = {}
            bad, after = rec["status"] in BAD, job.get("notify_after", 1)
            fails = ns.get("fails", 0) + 1 if bad else 0
            if bad:
                # streak: the bad runs that the user must hear about. It stays until the recovery is told.
                ns["streak"] = max(fails, ns.get("streak", 0)) if ns.get("alerted") else fails
                ns["last_bad"] = f"{rec['status']}: {detail_of(rec) or rec.get('note') or 'no detail'}"
                if fails >= after and not ns.get("alerted"):
                    ns["alerted"] = send(ns["last_bad"])
            elif ns.get("streak", 0) >= after:
                # an alert that was never delivered is told now, with the recovery
                back = f"ok again after {ns['streak']} bad runs"
                if send(back if ns.get("alerted") else f"{ns['last_bad']}; {back}"):
                    ns.update(alerted=False, streak=0)
            else:
                ns["streak"] = 0
            ns["fails"] = fails
            for s, js in zip(rec["steps"], job["steps"]):
                if "_value" in s and s["status"] == "ok":
                    s["note"] = watch(js, ns.setdefault("steps", {}).setdefault(js["id"], {}), s["_value"], now, send)
            tmp = f.with_name(f".{f.name}.{os.getpid()}.tmp")
            tmp.write_text(json.dumps(ns) + "\n")
            os.replace(tmp, f)
    except TimeoutError:
        errors.append("notify state is locked by another run")
    for s in rec["steps"]:
        s.pop("_value", None)
    if sent:
        rec["sent"] = sent
    if errors:
        rec["notify_error"] = errors[0]
        say(f"{job['id']}: notify failed, a later run retries: {errors[0]}")
    write_record(state, rec)


def slot_of(job, now):
    """The latest scheduled slot at or before now. normalize() rejects a schedule with no
    slot, so this is never invented. A job without a schedule uses the current minute."""
    if not job["schedule"]:
        return now.replace(second=0, microsecond=0)
    return prev_slot(parse_cron(job["schedule"]), now)


_JOB = None  # Windows: the job object; closing it (when this process ends) ends the steps


def end_steps_with_wrapper():
    """Windows: put this wrapper in a job object with KILL_ON_JOB_CLOSE. Its steps inherit the
    job, so when the wrapper exits or is killed (`schtasks /End`, uninstall), the steps end with
    it, as systemd (cgroup) and launchd (process group) already do. Without it, /End kills only
    pythonw and the step runs on, unlocked. ponytail: best effort; a failure leaves the old
    behavior."""
    global _JOB
    import ctypes
    from ctypes import wintypes
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.CreateJobObjectW.restype = wintypes.HANDLE
    k32.GetCurrentProcess.restype = wintypes.HANDLE
    k32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]

    class Basic(ctypes.Structure):
        _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64), ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", wintypes.DWORD), ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t), ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t), ("PriorityClass", wintypes.DWORD), ("SchedulingClass", wintypes.DWORD)]

    class Extended(ctypes.Structure):
        _fields_ = [("Basic", Basic), ("Io", ctypes.c_uint64 * 6), ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t), ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t)]
    job = k32.CreateJobObjectW(None, None)
    info = Extended()
    info.Basic.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if job and k32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)) \
            and k32.AssignProcessToJobObject(job, k32.GetCurrentProcess()):
        _JOB = job  # kept open for the life of the process
        return None
    err = ctypes.get_last_error()
    if job:
        k32.CloseHandle(wintypes.HANDLE(job))
    return f"Windows error {err}"


def end_job_leftovers(_locks=None):
    """Windows: end every other process in this wrapper's job (a background helper that a step
    left running) before the locks are released, so none of them runs without the lock."""
    if _JOB is None:
        return
    import ctypes
    from ctypes import wintypes
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.OpenProcess.restype = wintypes.HANDLE

    class PidList(ctypes.Structure):
        _fields_ = [("Assigned", wintypes.DWORD), ("InList", wintypes.DWORD), ("Pids", ctypes.c_size_t * 4096)]
    pl = PidList()
    if k32.QueryInformationJobObject(wintypes.HANDLE(_JOB), 3, ctypes.byref(pl), ctypes.sizeof(pl), None):
        for pid in pl.Pids[:pl.InList]:
            if pid != os.getpid():
                h = k32.OpenProcess(0x00100001, False, wintypes.DWORD(pid))  # TERMINATE | SYNCHRONIZE
                if h:
                    k32.TerminateProcess(wintypes.HANDLE(h), 1)
                    k32.WaitForSingleObject(wintypes.HANDLE(h), 5000)  # termination is asynchronous
                    k32.CloseHandle(wintypes.HANDLE(h))


BEFORE_UNLOCK = [end_job_leftovers]  # run while the locks are still held


def pulled_for(state: Path, jid, slot):
    """The job already ran for this slot or a later one, pulled in by another job: a run for an
    older slot that waited on the lock would overwrite the newer record."""
    last = read_record(state, jid)
    return bool(last and last.get("pulled_by") and (last.get("slot") or "") >= slot.isoformat()) and last


START_TTL = 600  # seconds a `takt start` request waits for the scheduler to run the job


def request_start(state: Path, jid):
    """`takt start` goes through the scheduler, which runs the same --scheduled command as a timed
    run. This file tells that run it was asked for, so catch_up = "skip" and pull-in dedup do
    not drop it."""
    (state / "start").mkdir(parents=True, exist_ok=True)
    (state / "start" / jid).write_text(repr(time.time()))


def take_start(state: Path, jid):
    """Consume a start request. One older than START_TTL is dropped: a scheduled run long after
    a failed start must not be mistaken for it."""
    p = state / "start" / jid
    try:
        t = float(p.read_text())
        p.unlink()
    except (OSError, ValueError):
        return False
    return time.time() - t <= START_TTL


def job_running(state: Path, jid):
    """A wrapper for this job is running: it holds `running/<id>` from its start, also while it
    waits for a shared lock (`job-<id>` is taken only with the other locks, after the wait)."""
    try:
        Locks(state, [jid], 0, sub="running").__enter__().__exit__()
        return False
    except TimeoutError:
        return True


def run_admin(cmds):
    """Native start/enable/disable commands. -> None, or the scheduler's error text."""
    for c in cmds:
        r = subprocess.run(c, capture_output=True, text=True, **NO_WINDOW)
        if r.returncode:
            return (r.stderr or r.stdout).strip() or str(r.returncode)
    return None


def run_job(spec, jid, state: Path, now=None, scheduled=False, dry=False, say=print, trigger=None):
    """Returns the process exit code: 0 ok or deliberately skipped, 1 partial/failed,
    2 preflight skip, 75 lock timeout."""
    now = now or datetime.now()
    job = spec[jid]
    slot = slot_of(job, now)
    trig = trigger or ("scheduled" if scheduled else "manual")
    if scheduled and job["catch_up"] == "skip" and job["schedule"] and (now - slot).total_seconds() > SKIP_AFTER:
        say(f"{jid}: skipped, missed slot {slot:%F %R} and catch_up=skip")
        return 0
    deps = chain_of(spec, jid)
    names = lock_names(spec, jid)
    if dry:
        pre = preflight(job)
        say(f"{jid} slot {slot:%F %R} trigger {trig}")
        say(f"  locks: {', '.join(sorted(names))}")
        say(f"  after: {', '.join(deps) or '-'} (pulled in only if due this slot and not yet run)")
        for r in pre:
            say(f"  {'needs' if r['hard'] else 'wants'} {r['need']}: {'ok' if r['ok'] else 'MISSING'}: {r['detail']}")
        for s in job["steps"]:
            say(f"  step {s['id']}: {shlex.join(s['command'])[:110]}")
        return 0
    if scheduled and (last := pulled_for(state, jid, slot)):  # before the wait: nothing to do
        say(f"{jid}: already ran for slot {slot:%F %R} (pulled in by {last['pulled_by']})")
        return 0
    marker = None
    try:
        # running/<id> first: only this job's own wrapper takes it, and it holds nothing else while
        # it waits for it, so it cannot deadlock. `takt start` reads it to see a waiting run.
        marker = Locks(state, [jid], job["lock_timeout"], sub="running").__enter__()  # own dir: no lock name collides
        locks = Locks(state, names, job["lock_timeout"]).__enter__()
    except TimeoutError as e:
        if marker:
            marker.__exit__()
        if scheduled and pulled_for(state, jid, slot):  # pulled in while we waited: keep that record
            say(f"{jid}: already ran for slot {slot:%F %R} (pulled in while waiting)")
            return 0
        rec = {"id": jid, "slot": slot.isoformat(), "trigger": trig, "pulled_by": None,
               "started": datetime.now().isoformat(timespec="seconds"), "duration_s": 0,
               "status": "lock-timeout", "exit": 75, "steps": [], "preflight": preflight(job),
               "note": f"lock {e} held for more than {job['lock_timeout']}s"}
        finish(job, rec, state, now, say)
        say(f"{jid}: lock-timeout ({rec['note']})")
        return 75
    try:
        if scheduled and (last := pulled_for(state, jid, slot)):  # again: pulled in while we waited
            say(f"{jid}: already ran for slot {slot:%F %R} (pulled in by {last['pulled_by']})")
            return 0
        order = []
        for d in deps:
            dj = spec[d]
            # due = the dependency has a slot at or after ours that has no record. It is recorded
            # under its own latest slot, so after a wake spanning two of its slots its own
            # catch-up start dedups instead of running a second time.
            dslot = slot_of(dj, now) if dj["schedule"] else None
            lr = read_record(state, d)
            late = dj["catch_up"] == "skip" and dslot and (now - dslot).total_seconds() > SKIP_AFTER
            if dslot and dslot >= slot and not late and not (lr and lr.get("slot") == dslot.isoformat()):
                order.append((d, dslot))
        for d, dslot in order:
            code = _run_locked(spec[d], dslot, "pulled", jid, state, say, now=now)
            say(f"{jid}: pulled in {d} first (exit {code})")
        # preflight now, after the locks and the dependencies: a dependency may be what
        # makes a `needs` check pass (a token refresh before the job that needs the token)
        rec_code = _run_locked(job, slot, trig, None, state, say, waited=locks.waited,
                               blocked_on=locks.blocked_on, pulled=[d for d, _ in order], now=now)
    finally:
        locks.__exit__()
        marker.__exit__()
    return rec_code


def _run_locked(job, slot, trig, pulled_by, state, say, waited=0, blocked_on=None, pulled=(), pre=None, now=None):
    pre = preflight(job) if pre is None else pre
    failed = [r for r in pre if r["hard"] and not r["ok"]]
    if failed:
        rec = {"id": job["id"], "slot": slot.isoformat(), "trigger": trig, "pulled_by": pulled_by,
               "started": datetime.now().isoformat(timespec="seconds"), "duration_s": 0,
               "status": "skipped", "exit": 2, "steps": [], "preflight": pre,
               "note": "skipped: " + "; ".join(f"needs {r['need']} ({r['detail']})" for r in failed)}
        finish(job, rec, state, now, say)
        say(f"{job['id']}: {rec['note']}")
        return 2
    rec = _execute(job, slot, trig, pulled_by, pre)
    rec["waited_s"], rec["blocked_on"], rec["pulled_in"] = waited, blocked_on, list(pulled)
    finish(job, rec, state, now, say)
    say(f"{job['id']}: {rec['status']} in {rec['duration_s']}s (waited {waited}s)")
    return rec["exit"]


# -------------------------------------------------------------------- backends

def sd_quote(a):
    return f'"{a}"' if re.search(r"\s", a) else a


def wrapper_argv(ctx, jid):
    return [ctx["python"], ctx["script"], "run", jid, "--spec", ctx["spec"], "--state", ctx["state"], "--scheduled"]


def launchd_schedule(f):
    """cron fields -> ('interval', 60) | ('cal', [dicts])."""
    if all(v is None for v in f):
        return ("interval", 60)
    keys = [("Month", f[3]), ("Day", f[2]), ("Weekday", f[4]), ("Hour", f[1]), ("Minute", f[0])]
    keys = [(k, v) for k, v in keys if v is not None]
    return ("cal", [dict(zip([k for k, _ in keys], combo)) for combo in product(*[v for _, v in keys])])


def windows_only(job, backend):
    if job.get("on_event"):
        raise ValueError(f"job {job['id']}: on_event is Windows-only; {backend} has no event triggers")


def render_launchd(job, ctx) -> bytes:
    windows_only(job, "launchd")
    d = {"Label": f"{PREFIX}.{job['id']}", "ProgramArguments": wrapper_argv(ctx, job["id"]),
         "EnvironmentVariables": {"PATH": ctx["path"]},
         "StandardOutPath": f"{ctx['state']}/log/{job['id']}.log",
         "StandardErrorPath": f"{ctx['state']}/log/{job['id']}.log", "ProcessType": "Background"}
    if job["bundle"]:
        d["AssociatedBundleIdentifiers"] = [job["bundle"]]
    if job["schedule"]:
        kind, v = launchd_schedule(parse_cron(job["schedule"]))
        if kind == "interval":
            d["StartInterval"] = v
        else:
            d["StartCalendarInterval"] = v[0] if len(v) == 1 else v
    if job["run_at_load"]:
        d["RunAtLoad"] = True
    return plistlib.dumps(d, sort_keys=False)


def parse_launchd(data: bytes) -> dict:
    """A plist -> job dict. Reads both foreign plists and ones render_launchd wrote."""
    p = plistlib.loads(data)
    label = p["Label"]
    pa = p.get("ProgramArguments") or ["/bin/sh", "-c", p.get("Program", "")]
    if p.get("Program") and p.get("ProgramArguments"):  # Program runs; ProgramArguments[0] is only argv[0]
        pa = [p["Program"], *pa[1:]]
    # reverse-DNS label: drop the two owner parts (com.example.backup.daily -> backup-daily)
    parts = label.split(".")
    jid = re.sub(r"[^A-Za-z0-9_-]", "-", ".".join(parts[2:]) if len(parts) > 2 else label)
    sched = None
    if "StartInterval" in p:
        if p["StartInterval"] != 60:
            raise ValueError("only StartInterval 60 maps to cron")
        sched = "* * * * *"
    elif "StartCalendarInterval" in p:
        dicts = p["StartCalendarInterval"]
        dicts = [dicts] if isinstance(dicts, dict) else dicts
        keys = ["Month", "Day", "Weekday", "Hour", "Minute"]
        sets = {k: sorted({d[k] for d in dicts if k in d}) for k in keys}
        if any(set(d) != set(dicts[0]) for d in dicts):
            raise ValueError("calendar dicts with differing keys do not map to one cron line")
        sets = {k: v for k, v in sets.items() if v}
        if len({tuple(sorted(d.items())) for d in dicts}) != len(list(product(*sets.values()))):
            raise ValueError("calendar dicts are not a cartesian product; cannot map to one cron line")
        f = [sets.get("Minute"), sets.get("Hour"), sets.get("Day"), sets.get("Month"),
             sorted({v % 7 for v in sets["Weekday"]}) if "Weekday" in sets else None]
        sched = fields_to_cron([None if v is not None and v == list(range(lo, hi + 1)) else v
                                for v, (lo, hi) in zip(f, RANGES)])
    step = {"id": "main", "command": pa, "stdout": p.get("StandardOutPath"),
            "stderr": p.get("StandardErrorPath"), "report": None}
    return {"id": jid, "schedule": sched, "run_at_load": bool(p.get("RunAtLoad")), "catch_up": "run-once",
            "lock": [], "after": [], "needs": [], "wants": [], "lock_timeout": 900, "replaces": [],
            "bundle": (p.get("AssociatedBundleIdentifiers") or [None])[0], "steps": [step]}


def sd_field(s, lo, pad):
    if s == "*":
        return "*"
    if s.startswith("*/"):
        return f"{lo:0{pad}d}/{s[2:]}"
    return ",".join(f"{int(x):0{pad}d}" for x in s.split(","))


def render_systemd(job, ctx):
    """-> (service text, timer text or None). A job with no schedule and no run_at_load is
    manual-only: systemd rejects a timer without a trigger, so it gets the service alone."""
    windows_only(job, "systemd")
    unit = f"takt-{job['id']}"
    argv = " ".join(sd_quote(a) for a in wrapper_argv(ctx, job["id"]))
    service = (f"[Unit]\nDescription=takt job {job['id']}\n\n[Service]\nType=oneshot\n"
               f"Environment=PATH={ctx['path']}\nExecStart={argv}\n")
    timer = [f"[Unit]\nDescription=takt timer {job['id']}\n\n[Timer]"]
    if job["schedule"]:
        fl = parse_cron(job["schedule"])
        f = fields_to_cron(fl).split()
        dow = ""
        if fl[4] is not None:  # from the expanded list: compress() may have written */2
            names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
            dow = ",".join(names[x] for x in fl[4]) + " "
        timer.append(f"OnCalendar={dow}*-{sd_field(f[3], 1, 1)}-{sd_field(f[2], 1, 1)} "
                     f"{sd_field(f[1], 0, 2)}:{sd_field(f[0], 0, 2)}:00")
    if job["run_at_load"]:
        timer.append("OnActiveSec=5s")
    if job["catch_up"] == "run-once":
        timer.append("Persistent=true")
    timer.append(f"Unit={unit}.service\n\n[Install]\nWantedBy=timers.target\n")
    return service, ("\n".join(timer) if job["schedule"] or job["run_at_load"] else None)


TS_NS = "http://schemas.microsoft.com/windows/2004/02/mit/task"
SCHTASKS_MAX_TRIGGERS = 48  # Task Scheduler's limit per task
DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
          "October", "November", "December"]


def _sub(parent, tag, text=None):
    e = ET.SubElement(parent, f"{{{TS_NS}}}{tag}")
    if text is not None:
        e.text = str(text)
    return e


def schtasks_triggers(f):
    """cron fields -> [(HH, MM, repetition interval or None)].
    ponytail: every-minute-inside-an-hour-window (`* 9-17 * * *`) is not expressible; raises."""
    mi, hr = f[0], f[1]
    if hr is None:
        if mi is None:
            return [(0, 0, "PT1M")]
        if len(mi) > 1 and mi[0] == 0 and 60 % mi[1] == 0 and mi == list(range(0, 60, mi[1])):  # */7 restarts at :00
            return [(0, 0, f"PT{mi[1]}M")]
        return [(0, m, "PT1H") for m in mi]
    if mi is None:
        raise ValueError("every minute within selected hours has no Task Scheduler form")
    return [(h, m, None) for h in hr for m in mi]


def render_schtasks(job, ctx) -> str:
    ET.register_namespace("", TS_NS)
    root = ET.Element(f"{{{TS_NS}}}Task", version="1.2")
    _sub(_sub(root, "RegistrationInfo"), "Description", f"takt job {job['id']}")
    trig = _sub(root, "Triggers")
    if job["schedule"]:
        f = parse_cron(job["schedule"])
        if f[4] is not None and (f[2] is not None or f[3] is not None):
            raise ValueError("schtasks: weekday combined with day/month is not supported")
        for h, m, rep in schtasks_triggers(f):
            t = _sub(trig, "CalendarTrigger")
            if rep:
                r = _sub(t, "Repetition")
                _sub(r, "Interval", rep)
                _sub(r, "Duration", "P1D")
                _sub(r, "StopAtDurationEnd", "false")
            _sub(t, "StartBoundary", f"2026-01-01T{h:02d}:{m:02d}:00")
            _sub(t, "Enabled", "true")
            if f[4] is not None:
                sw = _sub(t, "ScheduleByWeek")
                _sub(sw, "WeeksInterval", 1)
                w = _sub(sw, "DaysOfWeek")
                for d in f[4]:
                    _sub(w, DAYS[d])
            elif f[2] is not None or f[3] is not None:
                mo = _sub(t, "ScheduleByMonth")
                dm = _sub(mo, "DaysOfMonth")
                for d in f[2] or range(1, 32):
                    _sub(dm, "Day", d)
                ms = _sub(mo, "Months")
                for x in f[3] or range(1, 13):
                    _sub(ms, MONTHS[x - 1])
            else:
                _sub(_sub(t, "ScheduleByDay"), "DaysInterval", 1)
    if job["run_at_load"]:
        _sub(_sub(trig, "LogonTrigger"), "Enabled", "true")
    if job.get("on_event"):  # e.g. a Remote Desktop session event; the query is XML text, escaped by ElementTree
        ev = _sub(trig, "EventTrigger")
        _sub(ev, "Enabled", "true")
        _sub(ev, "Subscription", job["on_event"])
    if len(trig) > SCHTASKS_MAX_TRIGGERS:  # registration would fail; refuse while planning, before any change
        raise ValueError(f"job {job['id']}: schedule {job['schedule']!r} needs {len(trig)} Task Scheduler triggers; "
                         f"a task can have at most {SCHTASKS_MAX_TRIGGERS}")
    s = _sub(root, "Settings")
    _sub(s, "MultipleInstancesPolicy", "IgnoreNew")
    _sub(s, "StartWhenAvailable", "true" if job["catch_up"] == "run-once" else "false")
    _sub(s, "DisallowStartIfOnBatteries", "false")
    _sub(s, "StopIfGoingOnBatteries", "false")
    ex = _sub(_sub(root, "Actions"), "Exec")
    argv = wrapper_argv(ctx, job["id"])
    # pythonw: python.exe under a logged-on user's task flashes a console window every run
    _sub(ex, "Command", re.sub(r"python\.exe$", "pythonw.exe", argv[0], flags=re.I))
    _sub(ex, "Arguments", " ".join(sd_quote(a) for a in argv[1:]))
    ET.indent(root)
    return ET.tostring(root, encoding="unicode") + "\n"


def stage(spec, ctx, out: Path, backends):
    """Write rendered files under `out` only. Returns the list of paths."""
    made = []
    for b in backends:
        d = out / b
        d.mkdir(parents=True, exist_ok=True)
        for j in spec.values():
            if b != "schtasks" and j.get("on_event"):
                print(f"skip {b}/{j['id']}: on_event is Windows-only", file=sys.stderr)
                continue
            if b == "launchd":
                files = {f"{PREFIX}.{j['id']}.plist": render_launchd(j, ctx)}
            elif b == "systemd":
                svc, tmr = render_systemd(j, ctx)
                files = {f"takt-{j['id']}.service": svc, **({f"takt-{j['id']}.timer": tmr} if tmr else {})}
            else:
                files = {f"takt-{j['id']}.xml": render_schtasks(j, ctx)}
            for name, body in files.items():
                (d / name).write_bytes(body if isinstance(body, bytes) else body.encode())
                made.append(d / name)
    return made


# ---------------------------------------------------------------------- import

def cron_like(body):
    t = body.split()
    return bool(t) and bool(re.fullmatch(r"@\w+|[\d*][\d*,/-]*", t[0])) and (len(t) >= 6 or t[0].startswith("@"))


SHELL_BUILTINS = {"cd", "export", "source", ".", "set", "unset", "ulimit", "umask", "exec", "eval", "alias",
                  "trap", "pushd", "popd", "shopt", "read", "wait", "exit", "builtin", "command", "local"}


def split_steps(cmd):
    """Shell line -> steps, when it is only argv, `;`, `>>` and `2>>`. Anything else raises,
    and the caller keeps the line as one `sh -c` step with the text verbatim: expansion
    ($, `, globs, ~, braces), cron's % and a truncating > change meaning outside a shell."""
    if re.search(r"[$`*?\[{~\\#]", cmd):  # expansion, backslash escapes (`echo \;`), `abc#def`
        raise ValueError("shell expansion")
    if re.search(r"(^|\s)(['\"]2['\"]\s*|2\s+)>>", cmd):  # `echo 2 >> f`, `echo "2">> f`: the 2 is an argument
        raise ValueError("ambiguous 2 before >>")
    if any(fd != "2" for fd in re.findall(r"(?:^|\s)(\d+)>>", cmd)):  # `echo hi 1>> f`: the 1 is a descriptor
        raise ValueError("descriptor before >>")
    if re.search(r"['\"][;<>|&]|[;<>|&]['\"]", cmd):  # `echo ";"`: shlex drops the quotes that made it an argument
        raise ValueError("quote next to an operator")
    lex = shlex.shlex(cmd, posix=True, punctuation_chars=True)
    lex.whitespace_split = True
    toks = list(lex)
    steps, argv, out, err = [], [], None, None
    i = 0
    while i < len(toks):
        t = toks[i]
        if t == ">>":  # a takt log appends; `>` would truncate, so it raises below
            if i + 1 >= len(toks):
                raise ValueError("dangling redirect")
            if argv and argv[-1] == "2":
                argv.pop()
                err = toks[i + 1]
            else:
                out = toks[i + 1]
            i += 2
            continue
        if not argv and re.match(r"[A-Za-z_]\w*=", t):  # NAME=value cmd: an environment prefix
            raise ValueError("environment assignment")
        if not argv and t in SHELL_BUILTINS:  # `cd /tmp; cmd`: cd changes the next command's shell
            raise ValueError(f"shell builtin {t}")
        if t == ";":
            steps.append((argv, out, err))
            argv, out, err = [], None, None
        elif re.fullmatch(r"[();<>|&]+", t):
            raise ValueError(f"shell operator {t}")
        else:
            argv.append(t)
        i += 1
    if argv:
        steps.append((argv, out, err))
    return steps


def import_cron(text):
    """crontab text -> (jobs, skipped commented-out cron lines, warnings).
    A commented-out cron line is never a job (it is how python-crontab's
    name-by-the-comment-above made a disabled line steal a live job's identity)."""
    lines = text.splitlines()
    jobs, skipped, warns, seen = [], [], [], set()
    for i, raw in enumerate(lines):
        s = raw.strip()
        if not s:
            continue
        if re.match(r"^[A-Za-z_]\w*\s*=", s):
            warns.append(f"line {i + 1}: crontab environment {s!r} is not imported; set it in "
                         "[settings] path or in the commands")
            continue
        if s.startswith("#"):
            if cron_like(s.lstrip("#").strip()):
                skipped.append(i + 1)
            continue
        parts = s.split(None, 1) if s.startswith("@") else s.split(None, 5)
        sched, cmd = (parts[0], parts[1]) if s.startswith("@") else (" ".join(parts[:5]), parts[5])
        sched = ALIASES.get(sched, sched)
        if re.search(r"(?<!\\)%", cmd):  # cron turns an unescaped % into a newline and stdin
            warns.append(f"line {i + 1}: not imported: an unescaped % is cron's stdin separator, "
                         "which a takt job cannot express")
            continue
        cmd = cmd.replace("\\%", "%")
        name = None
        j = i - 1
        block = []
        while j >= 0 and lines[j].strip().startswith("#"):
            block.append(lines[j].strip().lstrip("#").strip())
            j -= 1
        for body in reversed(block):
            if body and not cron_like(body):
                name = re.split(r"\s+[\u2013\u2014-]\s+|\s+", body)[0]
                break
        if not name:
            m = re.search(r"\s#\s*([\w.-]+)\s*$", cmd)
            name = m.group(1) if m else f"line-{i + 1}"
            warns.append(f"line {i + 1}: no name comment above, using {name!r}")
        name = re.sub(r"[^A-Za-z0-9_-]", "-", name)
        base, n = name, 1
        while name in seen:
            n += 1
            name = f"{base}-{n}"
        if name != base:
            warns.append(f"line {i + 1}: duplicate id {base!r}, renamed {name!r}")
        seen.add(name)
        cmd = re.sub(r"\s#\s*[\w.-]+\s*$", "", cmd)  # the trailing name comment is not part of the command
        try:
            raw_steps = split_steps(cmd)
        except ValueError:
            raw_steps = [(["/bin/sh", "-c", cmd], None, None)]
            warns.append(f"{name}: shell operators kept as one sh -c step")
        steps = [{"id": f"step{k + 1}", "command": a, "stdout": o, "stderr": e, "report": None}
                 for k, (a, o, e) in enumerate(raw_steps)]
        if len(steps) == 1:
            steps[0]["id"] = "main"
        jobs.append(normalize(name, {"schedule": sched, "step": steps}) if len(steps) > 1 else
                    normalize(name, {"schedule": sched, **steps[0]}))
    return jobs, skipped, warns


# --------------------------------------------------------------------- install

def backend(platform=None):
    return {"darwin": "launchd", "win32": "schtasks"}.get(platform or sys.platform, "systemd")


def _uid():
    return os.getuid() if hasattr(os, "getuid") else 0


def task_name(jid):
    return f"\\takt\\{jid}"


def default_dir(be, state: Path) -> Path:
    """Where installed scheduler files live: LaunchAgents, systemd user units, or (schtasks
    registers from a file) the XML kept under state."""
    return {"launchd": Path.home() / "Library/LaunchAgents",
            "systemd": Path.home() / ".config/systemd/user"}.get(be, state / "schtasks")


def plan_install(spec, ctx, agents_dir: Path, crontab_text: str, be="launchd", loaded=None):
    """Ordered actions. Nothing here touches the machine. `loaded` is the set of launchd labels
    the scheduler has loaded (from inventory): a loaded job must boot out, and a failure stops
    the plan before its plist is moved or written. None (no inventory) tolerates the bootout."""
    acts = [("mkdir", Path(ctx["state"]) / "log", None)]  # launchd will not create a log dir
    uid = _uid()
    # Retire the old schedulers first: run_at_load fires a reseed the moment a new plist is
    # bootstrapped, and it must not meet a still-armed old job on the same Edge dir.
    new_cron, cron_changed, booted = crontab_text.splitlines(), False, set()
    for j in spec.values():
        for rep in j["replaces"]:
            kind, _, arg = rep.partition(":")
            if kind == "launchd" and be == "launchd":
                old = agents_dir / f"{arg}.plist"
                if (loaded is None or arg in loaded) and arg not in booted:  # once, if two jobs replace it
                    booted.add(arg)
                    acts.append(("run" if loaded is not None else "try", ["launchctl", "bootout", f"gui/{uid}/{arg}"], None))
                if old.exists():  # already retired by an earlier install: nothing to move, so plan nothing
                    acts.append(("move", old, Path(ctx["state"]) / "retired" / old.name))
            elif kind == "schtasks" and be == "schtasks":  # disabled, not deleted: /ENABLE brings it back
                acts.append(("run", ["schtasks", "/End", "/TN", arg], None))
                acts.append(("run", ["schtasks", "/Change", "/TN", arg, "/DISABLE"], None))
            elif kind == "cron":
                for k, line in enumerate(new_cron):
                    if not line.lstrip().startswith("#") and arg in line:
                        new_cron[k] = f"#takt-migrated# {line}"
                        cron_changed = True
    if cron_changed:
        acts.append(("write", Path(ctx["state"]) / "crontab.bak", crontab_text.encode()))
        acts.append(("run", ["crontab", "-"], ("\n".join(new_cron) + "\n").encode()))
    for j in spec.values():
        jid = j["id"]
        if be == "launchd":
            p, label = agents_dir / f"{PREFIX}.{jid}.plist", f"gui/{uid}/{PREFIX}.{jid}"
            if (loaded is None or f"{PREFIX}.{jid}" in loaded) and f"{PREFIX}.{jid}" not in booted:  # a reinstall
                acts.append(("run" if loaded is not None else "try", ["launchctl", "bootout", label], None))
            acts.append(("write", p, render_launchd(j, ctx)))
            acts.append(("run", ["launchctl", "enable", label], None))  # clears a `takt disable`
            acts.append(("run", ["launchctl", "bootstrap", f"gui/{uid}", str(p)], None))
        elif be == "systemd":
            svc, tmr = render_systemd(j, ctx)
            acts.append(("write", agents_dir / f"takt-{jid}.service", svc.encode()))
            if tmr:
                acts.append(("write", agents_dir / f"takt-{jid}.timer", tmr.encode()))
            elif (agents_dir / f"takt-{jid}.timer").exists():  # the schedule was removed: retire the old timer
                acts.append(("run", ["systemctl", "--user", "disable", "--now", f"takt-{jid}.timer"], None))
                acts.append(("rm", agents_dir / f"takt-{jid}.timer", None))
        else:
            x = agents_dir / f"takt-{jid}.xml"
            acts.append(("write", x, render_schtasks(j, ctx).encode("utf-16")))  # schtasks wants UTF-16
            acts.append(("run", ["schtasks", "/Create", "/TN", task_name(jid), "/XML", str(x), "/F"], None))
    if be == "systemd":
        acts.append(("run", ["systemctl", "--user", "daemon-reload"], None))
        for jid, j in spec.items():  # restart, so a reinstalled timer takes its new schedule now
            if not (j["schedule"] or j["run_at_load"]):
                continue  # manual-only: no timer; `takt start` runs the service
            acts.append(("run", ["systemctl", "--user", "enable", f"takt-{jid}.timer"], None))
            acts.append(("run", ["systemctl", "--user", "restart", f"takt-{jid}.timer"], None))
    return acts


def read_crontab(spec, run=subprocess.run):
    """The crontab, read only when a job has `replaces = ["cron:..."]`."""
    if not any(r.startswith("cron:") for j in spec.values() for r in j["replaces"]):
        return ""
    try:
        return run(["crontab", "-l"], capture_output=True, text=True).stdout
    except OSError:
        sys.exit("a job has replaces = [\"cron:...\"], but crontab is not installed")


def installed_ids(be, agents_dir: Path, text=None):
    """Every job takt has registered here, from the scheduler's files (launchd, systemd) or
    its task list (schtasks), whether or not the spec still names it."""
    if be == "launchd":  # plists on disk, plus loaded labels whose plist is gone
        ids = {p.name[len(PREFIX) + 1:-6] for p in agents_dir.glob(f"{PREFIX}.*.plist")}
        labels = (line.split("\t")[-1] for line in (text or "").splitlines())
        return sorted(ids | {lb[len(PREFIX) + 1:] for lb in labels if lb.startswith(PREFIX + ".")})
    if be == "systemd":
        return sorted({p.name[5:-6] for p in agents_dir.glob("takt-*.timer")}
                      | {p.name[5:-8] for p in agents_dir.glob("takt-*.service")})  # manual-only: service alone
    text = native_text(be, []) if text is None else text
    return sorted(n for n in (line.strip().partition("|")[0] for line in text.splitlines()) if n)


def plan_uninstall(ids, agents_dir: Path, be, nat):
    """Stop and remove what install registered. Only a job the scheduler has is stopped, so a
    refusal from the scheduler is a real error, and the plan stops before deleting its files.
    `nat` is parse_native() output. ponytail: does not un-retire `replaces`; the retired
    plist and crontab.bak are kept under state for doing that by hand."""
    acts = []
    for jid in ids:
        inst, loaded = nat.get(jid, (False, False))
        if be == "launchd":
            if loaded:
                acts.append(("run", ["launchctl", "bootout", f"gui/{_uid()}/{PREFIX}.{jid}"], None))
            acts.append(("rm", agents_dir / f"{PREFIX}.{jid}.plist", None))
        elif be == "systemd":
            if inst:  # the timer, then a run that is still going (stop kills the service's cgroup)
                acts.append(("run", ["systemctl", "--user", "disable", "--now", f"takt-{jid}.timer"], None))
            if inst or (agents_dir / f"takt-{jid}.service").exists():  # also a manual-only job's service
                acts.append(("run", ["systemctl", "--user", "stop", f"takt-{jid}.service"], None))
            acts += [("rm", agents_dir / f"takt-{jid}.{x}", None) for x in ("timer", "service")]
        else:
            if inst:  # /Delete leaves a running instance alive; /End first (exit 0 when idle)
                acts.append(("run", ["schtasks", "/End", "/TN", task_name(jid)], None))
                acts.append(("run", ["schtasks", "/Delete", "/TN", task_name(jid), "/F"], None))
            acts.append(("rm", agents_dir / f"takt-{jid}.xml", None))
    if be == "systemd" and ids:
        acts.append(("run", ["systemctl", "--user", "daemon-reload"], None))
    return acts


def installed_from(be, agents_dir: Path, jid, spec_path):
    """The installed unit of `jid` runs `--spec <spec_path>`: it belongs to this jobs file. A job
    installed from another file (a second `--spec`, another tool's jobs) is someone else's."""
    found = installed_spec(be, agents_dir, jid)
    return found is not None and same_path(found, spec_path)


def same_path(a, b):
    return os.path.normcase(os.path.normpath(str(a))) == os.path.normcase(os.path.normpath(str(b)))


def installed_spec(be, agents_dir: Path, jid):
    """The --spec value in the installed unit of `jid` (plist ProgramArguments, systemd ExecStart,
    task XML Arguments), parsed rather than searched, or None."""
    f = agents_dir / {"launchd": f"{PREFIX}.{jid}.plist", "systemd": f"takt-{jid}.service"}.get(be, f"takt-{jid}.xml")
    try:
        raw = f.read_bytes()
        if be == "launchd":
            args = [str(x) for x in plistlib.loads(raw).get("ProgramArguments", [])]
        else:
            text = raw.decode("utf-16") if raw[:2] in (b"\xff\xfe", b"\xfe\xff") else raw.decode(errors="replace")
            if be == "systemd":
                line = next((x for x in text.splitlines() if x.startswith("ExecStart=")), "")[len("ExecStart="):]
            else:
                el = ET.fromstring(text).find(f".//{{{TS_NS}}}Arguments")
                line = el.text or "" if el is not None else ""
            args = [q or w for q, w in re.findall(r'"([^"]*)"|(\S+)', line)]  # sd_quote: "..." around spaces
        return args[args.index("--spec") + 1]
    except (OSError, ValueError, IndexError, plistlib.InvalidFileException, ET.ParseError):
        return None


def plan_admin(cmd, spec, be, agents_dir: Path, known, nat, ids=None, ctx=None, crontab="", loaded=None, owned=None,
               web=None):
    """install: retire the jobs of this jobs file that it no longer names, then install the file.
    uninstall: the given ids, or every job in the file and every job installed from it.
    `owned`: the registered ids installed from this jobs file (installed_from). A job from another
    file is never retired or removed implicitly.
    `web`: None leaves the web service alone; a command list installs it (install only); False
    retires it (install on a device that is not a web host, and uninstall without ids)."""
    owned = known if owned is None else owned
    kl, ol = {k.lower() for k in known}, {o.lower() for o in owned}  # Foo and foo are one unit file on macOS and Windows
    foreign = [i for i in spec if i.lower() in kl and i.lower() not in ol]  # same id, installed from another jobs file
    if cmd == "install":
        if foreign:
            raise ValueError(f"{', '.join(foreign)}: installed from another jobs file. Rename the job here, or "
                             "uninstall it with that file first")
        stale = [i for i in owned if i not in spec]
        if loaded is not None:  # retired below: a `replaces` naming one of them must not boot it out again
            loaded = loaded - {f"{PREFIX}.{i}" for i in stale}
        return (plan_uninstall(stale, agents_dir, be, nat)
                + plan_install(spec, ctx, agents_dir, crontab, be, loaded)
                + (plan_web(be, ctx, agents_dir, web or None, loaded) if web is not None else []))
    targets = list(ids) if ids else sorted({i for i in spec if i not in foreign} | set(owned))
    return (plan_uninstall(targets, agents_dir, be, nat)
            + (plan_web(be, ctx, agents_dir, None, loaded) if web is not None and not ids else []))


def describe(act):
    kind, a, b = act
    if kind == "write":
        return f"write {a} ({len(b)} bytes)"
    if kind in ("mkdir", "rm"):
        return f"{kind} {a}"
    if kind == "move":
        return f"move {a} -> {b}"
    if kind == "try":
        return "run   " + " ".join(a) + "   (may fail: not loaded)"
    return "run   " + " ".join(a) + (f"   [stdin: {len(b.splitlines())} lines]" if b else "")


def apply_plan(acts, runner):
    for kind, a, b in acts:
        if kind == "write":
            a.parent.mkdir(parents=True, exist_ok=True)
            a.write_bytes(b)
        elif kind == "mkdir":
            a.mkdir(parents=True, exist_ok=True)
        elif kind == "rm":
            a.unlink(missing_ok=True)
        elif kind == "try":
            try:
                runner(a, b)
            except (OSError, subprocess.SubprocessError):
                pass
        elif kind == "move":
            if a.exists():
                b.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(a), str(b))
        else:
            runner(a, b)


# ------------------------------------------------------------- admin, native state

def admin_argv(be, verb, jid, agents_dir: Path):
    """start (now, through the scheduler), enable, disable -> the native commands."""
    label, unit, tn, dom = f"{PREFIX}.{jid}", f"takt-{jid}", task_name(jid), f"gui/{_uid()}"
    return {
        ("launchd", "start"): [["launchctl", "kickstart", f"{dom}/{label}"]],
        ("launchd", "enable"): [["launchctl", "enable", f"{dom}/{label}"],
                                ["launchctl", "bootstrap", dom, str(agents_dir / f"{label}.plist")]],
        ("launchd", "disable"): [["launchctl", "bootout", f"{dom}/{label}"], ["launchctl", "disable", f"{dom}/{label}"]],
        ("systemd", "start"): [["systemctl", "--user", "start", "--no-block", f"{unit}.service"]],
        ("systemd", "enable"): [["systemctl", "--user", "enable", "--now", f"{unit}.timer"]],
        ("systemd", "disable"): [["systemctl", "--user", "disable", "--now", f"{unit}.timer"]],
        ("schtasks", "start"): [["schtasks", "/Run", "/TN", tn]],
        ("schtasks", "enable"): [["schtasks", "/Change", "/TN", tn, "/ENABLE"]],
        ("schtasks", "disable"): [["schtasks", "/Change", "/TN", tn, "/DISABLE"]],
    }[(be, verb)]


def native_query(be, ids):
    """One read-only call per host that reports the scheduler's view of every takt job."""
    if be == "launchd":
        return ["launchctl", "list"]
    if be == "systemd":
        return ["systemctl", "--user", "show", "-p", "Id,LoadState,ActiveState", *[f"takt-{j}.timer" for j in ids]]
    # State is an enum name (Ready/Running/Disabled), not localized text like schtasks /Query.
    # No \takt\ folder yet is an empty list (exit 0); any other error exits 1, so a strict
    # inventory can tell "no tasks" from "could not ask".
    return ["powershell", "-NoProfile", "-NonInteractive", "-Command",
            "try { Get-ScheduledTask -TaskPath '\\takt\\' -ErrorAction Stop | ForEach-Object { $_.TaskName + '|' + $_.State } } "
            "catch { if ($_.FullyQualifiedErrorId -like 'CmdletizationQuery_NotFound*') { exit 0 }; "
            "[Console]::Error.WriteLine($_); exit 1 }"]


def parse_native(be, text, ids, agents_dir: Path):
    """-> {jid: (installed, enabled)} for the jobs the scheduler knows about."""
    out = {}
    if be == "launchd":
        loaded = {line.split("\t")[-1] for line in text.splitlines()}
        for j in ids:
            out[j] = ((agents_dir / f"{PREFIX}.{j}.plist").exists(), f"{PREFIX}.{j}" in loaded)
    elif be == "systemd":
        for block in text.strip().split("\n\n"):
            kv = dict(x.split("=", 1) for x in block.splitlines() if "=" in x)
            j = kv.get("Id", "").removeprefix("takt-").removesuffix(".timer")
            if j in ids:
                out[j] = (kv.get("LoadState") == "loaded", kv.get("ActiveState") == "active")
    else:
        for line in text.splitlines():
            n, _, st = line.strip().partition("|")
            if n in ids:
                out[n] = (True, st != "Disabled")
    return out


def native_text(be, ids, strict=False, run=subprocess.run):
    """The scheduler's own list of jobs. `status` tolerates a failed query. A plan that changes
    the scheduler passes strict=True: there, "the query failed" must never read as "nothing is
    registered", or uninstall would delete the files of a job that is still loaded."""
    q = native_query(be, ids)
    try:
        r = run(q, capture_output=True, text=True, timeout=30, **NO_WINDOW)
    except (OSError, subprocess.SubprocessError) as e:
        if strict:
            raise RuntimeError(f"cannot read the scheduler ({q[0]}): {e}") from e
        return ""
    if strict and r.returncode != 0:
        raise RuntimeError(f"cannot read the scheduler: {q[0]} exited {r.returncode}: {(r.stderr or '').strip()[:200]}")
    return r.stdout


def inventory(spec, be, agents_dir: Path, run=subprocess.run, extra=()):
    """(takt ids registered here, scheduler state, loaded launchd labels) for install and
    uninstall. Strict. `extra`: ids named on the command line, looked up even when nothing
    else knows them."""
    listing = native_text(be, [], True, run) if be in ("launchd", "schtasks") else None  # full lists
    known = installed_ids(be, agents_dir, listing)
    ids = sorted(set(spec) | set(known) | set(extra))
    text = listing if listing is not None else native_text(be, ids, True, run)
    loaded = {line.split("\t")[-1] for line in text.splitlines()} if be == "launchd" else set()
    return known, parse_native(be, text, ids, agents_dir), loaded


def run_checked(argv, inp):
    """The runner of a plan that changes the scheduler: a failed command stops the plan."""
    return subprocess.run(argv, input=inp, check=True, **NO_WINDOW)


def native_states(ids, be, agents_dir: Path):
    ids = list(ids)
    return parse_native(be, native_text(be, ids), ids, agents_dir)


def detail_of(r):
    if not r:
        return ""
    detail = []
    if r.get("note") and r["status"] in ("skipped", "lock-timeout"):
        detail.append(r["note"])
    for s in r["steps"]:
        note = f" ({s['note']})" if s.get("note") else ""
        if s["status"] != "ok":
            detail.append(f"{s['id']}: {s['status']} exit {s['exit']}{note}"
                          + (f" failed sources: {', '.join(s['failed'])}" if s["failed"] else ""))
        elif note:  # an ok step can still report something (`{"error": "connection refused"}`)
            detail.append(f"{s['id']}:{note}")
    for p in r.get("preflight", []):
        if not p["ok"] and not p["hard"]:
            detail.append(f"warn {p['need']}: {p['detail']}")
    if r.get("waited_s", 0) >= 1:
        detail.append(f"waited {r['waited_s']}s on lock {r.get('blocked_on')}")
    if r.get("notify_error"):
        detail.append(f"notify failed: {r['notify_error']}")
    return "; ".join(detail)


def status_rows(spec, state, nat, host="local", now=None):
    """One row per job: the scheduler's view (on/off/- = not installed) next to the last record."""
    now = now or datetime.now()
    rows = []
    for jid, j in spec.items():
        r = read_record(state, jid)
        inst, on = nat.get(jid, (False, False))
        nxt = next_slot(parse_cron(j["schedule"]), now) if on and j["schedule"] else None
        rows.append({"host": host, "id": jid, "sched": "on" if on else ("off" if inst else "-"),
                     "schedule": j["schedule"], "status": r["status"] if r else "never",
                     "last": r["started"] if r else None, "next": nxt.isoformat(timespec="minutes") if nxt else None,
                     "detail": detail_of(r), "record": r})
    return rows


def fmt_rows(rows):
    """Fixed columns; HOST and ID first and space-free, so fzf's {1} {2} address a job."""
    t = [("HOST", "ID", "SCHED", "STATUS", "LAST", "NEXT", "DETAIL")]
    hm = lambda s: s[5:16].replace("T", " ") if s else "-"
    t += [(r["host"], r["id"], r["sched"], r["status"], hm(r["last"]), hm(r["next"]), r["detail"]) for r in rows]
    w = [max(len(row[i]) for row in t) for i in range(6)]
    return ["  ".join(c.ljust(w[i]) if i < 6 else c for i, c in enumerate(row)).rstrip() for row in t]


def format_record(jid, r):
    """The text of one record: status line, steps, notes, preflight."""
    if not r:
        return f"{jid}: never run"
    out = [f"{jid}  {r.get('status')}  started {r.get('started')}  {r.get('duration_s')}s  slot {r.get('slot')}  "
           f"{r.get('trigger')}" + (f" by {r['pulled_by']}" if r.get("pulled_by") else "")]
    if r.get("note"):
        out.append(f"  note: {r['note']}")
    for s in r.get("steps", []):
        out.append(f"  step {s.get('id', ''):14} {s.get('status', ''):8} exit {s.get('exit')}  {s.get('duration_s')}s"
                   + (f"  failed: {', '.join(s['failed'])}" if s.get("failed") else "")
                   + (f"  note: {s['note']}" if s.get("note") else ""))
    for t in r.get("sent", []):
        out.append(f"  sent: {t}")
    if r.get("notify_error"):
        out.append(f"  notify failed: {r['notify_error']}")
    for p in r.get("preflight", []):
        out.append(f"  {'needs' if p.get('hard') else 'wants'} {p.get('need', ''):12} "
                   f"{'ok' if p.get('ok') else 'MISSING'}  {p.get('detail', '')}")
    return "\n".join(out)


def tail(p, n=8, cap=65536):
    """The last n lines of a file, from its last `cap` bytes: logs only grow."""
    with open(p, "rb") as f:
        start = f.seek(max(0, f.seek(0, 2) - cap))
        lines = f.read().decode(errors="replace").splitlines()
    return (lines[1:] if start else lines)[-n:]  # from mid-file, the first line is a fragment


def show(spec, state, jid):
    """Last record of one job and the tail of its logs: the TUI preview."""
    if jid not in spec:
        print(f"no job {jid!r} here")
        return 1
    print(format_record(jid, read_record(state, jid)))
    logs = [state / "log" / f"{jid}.log"] + [Path(os.path.expanduser(s[k])) for s in spec[jid]["steps"]
                                              for k in ("stdout", "stderr") if s[k] and s[k] != "/dev/null"]
    for p in dict.fromkeys(logs):
        if p.is_file():
            print(f"\n== {p}")
            print("\n".join(tail(p)))
    return 0


# -------------------------------------------------------------------- remote hosts

REMOTE_DIR = ".local/share/takt"  # relative to the ssh login dir, the same on sh and pwsh
SAFE_ARG = re.compile(r"[\w./:\\=,@+-]+")
HOST_RE = re.compile(r"[A-Za-z0-9][\w.@-]*")  # an ssh alias; a leading - would be an ssh option


def hosts():
    """`local` plus every host with a `jobs.<host>.toml` in the config dir."""
    return ["local"] + sorted(h for p in CONFIG.glob("jobs.*.toml") if HOST_RE.fullmatch(h := p.name[5:-5]))


def host_settings(host):
    if not HOST_RE.fullmatch(host):
        sys.exit(f"bad host {host!r}: use an ssh alias (letters, digits, . _ @ -, no leading -)")
    p = CONFIG / f"jobs.{host}.toml"
    if not p.exists():
        sys.exit(f"no spec for host {host!r}: create {p}")
    return read_toml(p).get("settings", {})


def remote_argv(host, args, cfg=None):
    """ssh argv that runs takt on `host` with `args`. The login shell may be sh or pwsh and
    ssh joins with spaces, so nothing is quoted: every token must be a plain word."""
    if not HOST_RE.fullmatch(host):
        raise ValueError(f"not an ssh alias: {host!r}")
    cfg = host_settings(host) if cfg is None else cfg
    cmd = [cfg.get("python", "python3"), cfg.get("script", f"{REMOTE_DIR}/takt.py"), *args]
    bad = [x for x in cmd if not SAFE_ARG.fullmatch(x)] + [x for x in args if "\\" in x]  # sh: `\--x` is `--x`
    if bad:
        raise ValueError(f"not forwardable over ssh: {bad}")
    return ["ssh", host, " ".join(cmd)]


def push(host):
    """scp takt.py to host:~/.local/share/takt/ (where the installers put it too) and
    jobs.<host>.toml to host:~/.config/takt/jobs.toml. One `scp -r` of a staged tree, so
    no remote mkdir in a shell we would have to quote for."""
    with tempfile.TemporaryDirectory() as t:
        (Path(t) / REMOTE_DIR).mkdir(parents=True)
        (Path(t) / ".config/takt").mkdir(parents=True)
        shutil.copy(Path(__file__).resolve(), Path(t) / REMOTE_DIR / "takt.py")
        shutil.copy(CONFIG / f"jobs.{host}.toml", Path(t) / ".config/takt/jobs.toml")
        (Path(t) / ".config/takt/net.toml").write_text(net_toml(host, net_settings()), encoding="utf-8")
        return subprocess.run(["scp", "-q", "-r", str(Path(t) / ".local"), str(Path(t) / ".config"),
                               f"{host}:"]).returncode


def git_checkout(p: Path):
    return any((d / ".git").exists() for d in (p.parent, *p.parent.parents))


def fetch_url(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return r.read()


def update_verify(path, spec_path):
    """-> None when the new takt.py starts and can load and render this host's jobs file."""
    run = lambda *a: subprocess.run([sys.executable, str(path), *a], capture_output=True, text=True,
                                    timeout=120, **NO_WINDOW)
    r = run("--version")
    if r.returncode or not r.stdout.startswith("takt "):
        return "does not start"
    if spec_path and Path(spec_path).exists():
        with tempfile.TemporaryDirectory() as t:
            r = run("render", "--spec", str(spec_path), "--out", t)
        if r.returncode:
            return f"cannot load {spec_path}: {(r.stderr or r.stdout).strip()[-200:]}"
    return None


def update(spec_path, ref="main", force=False, fetch=fetch_url, script=None, say=print):
    """Replace this takt.py with the one on GitHub at `ref`, once the new file has shown that it
    starts and can load this host's jobs file. A copy in a git checkout is left to git. -> exit code."""
    script = Path(script or __file__).resolve()
    tag = lambda b: hashlib.sha256(b).hexdigest()[:10]
    if git_checkout(script) and not force:
        say(f"skipped: {script} is in a git checkout; update it with git pull")
        return 0
    try:
        new = fetch(UPDATE_URL.format(ref=ref))
    except (OSError, ValueError) as e:
        say(f"update failed: cannot fetch {ref}: {e}")
        return 1
    old = script.read_bytes()
    if new == old:
        say(f"up to date ({tag(old)}, {ref})")
        return 0
    tmp = script.with_name(f".takt.{os.getpid()}.py")
    try:
        tmp.write_bytes(new)
        if why := update_verify(tmp, spec_path):
            say(f"update failed: the new takt.py ({tag(new)}) {why}; kept {tag(old)}")
            return 1
        os.chmod(tmp, script.stat().st_mode)
        os.replace(tmp, script)  # atomic: a job that starts now reads the old file or the new one
    finally:
        tmp.unlink(missing_ok=True)
    say(f"updated takt.py {tag(old)} -> {tag(new)} ({ref})")
    return 0


def stale_plan_note(host):
    """A dry-run `--host H install` pushes nothing, so H plans from its current jobs.toml. When the
    local jobs.<host>.toml declares other jobs, say so: that plan is not what --allow-writes installs."""
    local = set(read_toml(CONFIG / f"jobs.{host}.toml").get("job", {}))
    remote = {r["id"] for r in remote_rows(host) if r.get("id") not in (None, "-")}
    if local == remote:
        return None
    return (f"NOTE the plan below comes from {host}'s current jobs.toml, because nothing is pushed yet. "
            f"jobs.{host}.toml adds {', '.join(sorted(local - remote)) or 'nothing'} and drops "
            f"{', '.join(sorted(remote - local)) or 'nothing'}; --allow-writes pushes it and installs that.")


def on_host(host, rest):
    """`takt --host H <cmd> ...`: H's own copy of takt runs the command, so units are rendered
    with H's paths and the scheduler is H's. install also pushes; push is gated like install."""
    host_settings(host)
    cmdline = remote_argv(host, rest)  # validate every argument before push writes anything
    cmd, writes = (rest[0] if rest else ""), "--allow-writes" in rest
    if cmd in ("push", "install"):
        try:
            build_parser().parse_args(rest)  # a typo fails here, before the host is changed
        except SystemExit as e:
            return e.code or 0
        if not writes:
            print(f"PLAN push {Path(__file__).name} to {host}:{REMOTE_DIR}/ and jobs.{host}.toml to "
                  f"{host}:.config/takt/jobs.toml", flush=True)
            note = stale_plan_note(host) if cmd == "install" else None
            if note:
                print(note, flush=True)
        elif push(host):
            return 1
        if cmd == "push":
            print("" if writes else "\nnothing written. Re-run with --allow-writes.")
            return 0
    return subprocess.run(cmdline).returncode


STATUS_MODES = ("master", "all", "client", "none")
STATUS_PULL = ["status", "--json", "--here"]  # a peer answers with its own rows, whatever its mode


def self_name():
    return socket.gethostname().split(".")[0].lower()


def net_settings():
    """This device's view of the takt-net: {status, master, self, hosts: {name: {python, script?}}}.
    A host reads net.toml, which `push` writes. The controller (the device with the jobs.<host>.toml
    files) reads `[settings] status / master / name` from its jobs.toml. A device with neither is a
    net of one and its own master."""
    f = CONFIG / "net.toml"
    if f.exists():
        net = read_toml(f)
    else:
        jt = CONFIG / "jobs.toml"
        cfg = read_toml(jt).get("settings", {}) if jt.exists() else {}
        me = cfg.get("name") or self_name()
        net = {"status": cfg.get("status", "master"), "master": cfg.get("master", me), "self": me,
               "hosts": {h: {"python": host_settings(h).get("python", "python3")} for h in hosts()[1:]},
               "python": cfg.get("python") or (sys.executable if os.name == "nt" else shutil.which("python3") or sys.executable),
               **{k: cfg[k] for k in ("web", "web_port", "web_writes", "web_bind") if k in cfg}}
    net.setdefault("self", self_name())
    net.setdefault("status", "master")
    net.setdefault("master", net["self"])
    net.setdefault("hosts", {})
    if net["status"] not in STATUS_MODES:
        raise ValueError(f"settings.status {net['status']!r}: use one of {', '.join(STATUS_MODES)}")
    if net["master"] == "local":
        net["master"] = net["self"]
    net.update(web_settings(net, net["self"]))
    return net


def status_scope(net, all_hosts, here=False):
    """Which statuses this device shows -> ("off" | "local" | "net", note or None).
    master: only the master shows statuses, and its -A shows every member. all: every device's -A
    shows every member. client: each device shows only its own jobs. none: no statuses."""
    if here:
        return "local", None
    mode, is_master = net["status"], net["master"] == net["self"]
    if mode == "none":
        return "off", "status display is off (settings.status = none)"
    if mode == "master" and not is_master:
        return "off", (f"statuses show on {net['master']} (settings.status = master); "
                       "`takt status --here` shows this device")
    if mode == "client":
        return "local", ("each device shows only its own jobs (settings.status = client)" if all_hosts else None)
    return ("net" if all_hosts else "local"), None


def net_toml(host, net):
    """net.toml for `host`: the mode, the master, its own name, and every other member with the
    python (and script path) to reach it, so `all` can fan out from that host too."""
    members = {h: c for h, c in net["hosts"].items() if h != host}
    members[net["self"]] = {"python": net.get("python") or "python3", "script": str(Path(__file__).resolve())}
    out = [f"# written by `takt --host {host} push` from {net['self']}; edit the controller's jobs.toml instead",
           f"status = {toml_val(net['status'])}", f"master = {toml_val(net['master'])}", f"self = {toml_val(host)}",
           f"web = {toml_val(net['web'])}", f"web_port = {net['web_port']}", f"web_writes = {toml_val(net['web_writes'])}", ""]
    if net["web_bind"]:
        out += ["[web_bind]"] + [f"{toml_val(k)} = {toml_val(v)}" for k, v in sorted(net["web_bind"].items())] + [""]
    for name, c in sorted(members.items()):
        out.append(f"[hosts.{toml_val(name)}]")
        out += [f"{k} = {toml_val(v)}" for k, v in sorted(c.items())] + [""]
    return "\n".join(out)


def remote_rows(host, cfg=None, timeout=60, opts=()):
    down = lambda why: [{"host": host, "id": "-", "sched": "-", "status": "unreachable", "last": None,
                         "next": None, "detail": why}]
    try:
        argv = remote_argv(host, STATUS_PULL, cfg)
        r = subprocess.run(argv[:1] + list(opts) + argv[1:], capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        return down(str(e))
    try:
        rows = json.loads(r.stdout)
    except ValueError:
        err = (r.stderr.strip().splitlines() or [f"no takt there? takt --host {host} push --allow-writes"])[-1]
        if "unrecognized arguments" in err or "invalid choice" in err:  # the host runs an older takt.py
            err = f"{host} runs an older takt; update it: takt --host {host} push --allow-writes"
        return down(err)
    for x in rows:
        x["host"] = host
    return rows


def ui(allow_writes):
    """fzf over every host's status: preview = `show`, keys run the admin verbs on that host."""
    if not shutil.which("fzf"):
        sys.exit("takt ui needs fzf")
    scope, note = status_scope(net_settings(), True)
    if scope == "off":
        print(note)
        return 0
    me = shlex.quote(str(Path(__file__).resolve()))
    ls = f"{me} status -A"
    binds = [f"start:reload({ls})", f"ctrl-r:reload({ls})",
             f"enter:execute({me} --host {{1}} show {{2}} | less -R)"]
    keys = "enter details · ctrl-r refresh"
    if allow_writes:
        for k, v in (("ctrl-s", "start"), ("ctrl-e", "enable"), ("ctrl-x", "disable")):
            binds.append(f"{k}:transform-footer({me} --host {{1}} {v} {{2}} --allow-writes 2>&1 | tail -1)+reload({ls})")
        keys += " · ctrl-s start · ctrl-e enable · ctrl-x disable"
    else:
        keys += " · read-only (takt ui --allow-writes to act)"
    r = subprocess.run(["fzf", "--header-lines=1", "--layout=reverse", "--no-sort", "--prompt", "takt> ",
                        "--header", keys, "--footer", " ", "--preview", f"{me} --host {{1}} show {{2}}",
                        "--preview-window", "down,55%,wrap", *[x for b in binds for x in ("--bind", b)]],
                        stdin=subprocess.DEVNULL)
    return 0 if r.returncode in (0, 1, 130) else r.returncode


# ------------------------------------------------------------------------- web
# `takt web`: a dashboard over the same rows as `status -A`, served to the tailnet. Settings:
# `web` (device names), `web_port`, `web_bind` (name -> address), `web_writes`. The service that
# keeps it running is NOT a job: its names (dev.takt-web, takt.web.service, \takt-web\serve) lie
# outside the job id rules, so a job named `web` cannot collide and uninstall treats it apart.

WEB_PORT = 8787
WEB_LABEL, WEB_UNIT, WEB_TASK = "dev.takt-web", "takt.web", "\\takt-web\\serve"
TAILSCALE_BINS = ["/Applications/Tailscale.app/Contents/MacOS/Tailscale", r"C:\Program Files\Tailscale\tailscale.exe"]
SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]  # no prompt can hang the page
REPORT_KEYS = ("id", "sched", "schedule", "status", "last", "next", "detail", "record")
WEB_VERBS = ("start", "enable", "disable")


def web_settings(cfg, me):
    """Validate the web keys of a settings table -> {web, web_port, web_writes, web_bind}."""
    names = cfg.get("web", [])
    if not isinstance(names, list) or not all(isinstance(n, str) and (n == "local" or HOST_RE.fullmatch(n)) for n in names):
        raise ValueError("settings.web: a list of device names, for example [\"myvps\"]")
    port = cfg.get("web_port", WEB_PORT)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError(f"settings.web_port {port!r}: use a number from 1 to 65535")
    binds = cfg.get("web_bind", {})
    if not isinstance(binds, dict):
        raise ValueError("settings.web_bind: a table of device name = address")
    for k, v in binds.items():
        try:
            if ipaddress.ip_address(v).version != 4:
                raise ValueError
        except ValueError:
            raise ValueError(f"settings.web_bind.{k} {v!r}: use an IPv4 address") from None
    return {"web": [me if n == "local" else n for n in names], "web_port": port,
            "web_writes": bool(cfg.get("web_writes", False)), "web_bind": dict(binds)}


def web_scope(net):
    """A web host is a viewer: it shows every device, whatever settings.status says (master, all,
    client). Only `none` turns the page off. This does not change `takt status -A` on that host."""
    return "off" if net["status"] == "none" else "net"


def tailscale_ip(run=subprocess.run, which=shutil.which, isfile=os.path.isfile, peer=None, timeout=10):
    """This machine's Tailscale IPv4 address (or `peer`'s, by its machine name), or None. Tries the
    CLI on PATH, then the app paths. Works without MagicDNS."""
    for c in [which("tailscale"), *TAILSCALE_BINS]:
        if not c or not isfile(c):
            continue
        try:
            out = run([c, "ip", "-4", *([peer] if peer else [])], capture_output=True, text=True, timeout=timeout,
                      **NO_WINDOW).stdout
        except (OSError, subprocess.SubprocessError):
            continue
        for line in out.split():
            try:
                if ipaddress.ip_address(line).version == 4:
                    return line
            except ValueError:
                pass
    return None


def tailscale_whois(ip, run=subprocess.run, which=shutil.which, isfile=os.path.isfile):
    """The tailnet machine behind `ip`: the first label of Node.Name, lowercase (kmbp from
    kmbp.example.ts.net.), or None when Tailscale does not know the address."""
    for c in [which("tailscale"), *TAILSCALE_BINS]:
        if not c or not isfile(c):
            continue
        try:
            out = run([c, "whois", "--json", ip], capture_output=True, text=True, timeout=10, **NO_WINDOW).stdout
            name = json.loads(out)["Node"]["Name"]
        except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError):
            continue
        return str(name).split(".")[0].lower() or None
    return None


def web_bind_addr(arg, net, finder=tailscale_ip):
    """--bind, else settings.web_bind for this device, else the Tailscale address. Never a wildcard
    by default: with none of these, the server refuses to start."""
    addr = arg or net["web_bind"].get(net["self"]) or finder()
    if not addr:
        sys.exit("takt web: no Tailscale address (is tailscale up?). Set --bind, or [settings.web_bind] "
                 f"{net['self']} = \"<address>\" in the jobs file.")
    try:
        ip = ipaddress.ip_address(addr)
    except ValueError:
        sys.exit(f"takt web: bad address {addr!r}")
    if ip.version == 6:
        sys.exit(f"takt web: {addr} is IPv6; the server listens on IPv4 only (use the Tailscale IPv4 address)")
    return addr


def host_ok(header):
    """DNS rebinding guard: a Host header must be an IP address, localhost, a bare name or a
    MagicDNS name (.ts.net). A page on another domain that resolves to this address fails."""
    h = (header or "").strip().lower()
    h = h[1:h.index("]")] if h.startswith("[") and "]" in h else h.rsplit(":", 1)[0] if h.count(":") == 1 else h
    try:
        ipaddress.ip_address(h)
        return True
    except ValueError:
        return bool(h) and ("." not in h or h.endswith(".ts.net") or h == "localhost")


class WebApp:
    """What the page shows and does. Every outside effect goes through `run`, `remote` and
    `local_rows`, so the check can swap them."""

    def __init__(self, net, spec, state, writes, run=None, local_rows=None, ttl=8, whois=None, now=None):
        self.net, self.spec, self.state, self.writes, self.ttl = net, spec, Path(state), writes, ttl
        self.run_ = run or self._run
        self.local_rows = local_rows or self._local_rows
        self.whois = whois or tailscale_whois
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.lock, self.cache, self.quiet = threading.Lock(), (0.0, None), False
        self.reports = self._load_reports()

    def _load_reports(self):
        """The last report of each peer, kept in <state>/reports/<device>.json across restarts."""
        out = {}
        for f in (self.state / "reports").glob("*.json") if (self.state / "reports").is_dir() else []:
            try:
                rep_ = json.loads(f.read_text())
                rep_["rows"] = self.clean_rows(rep_.get("rows"))
                out[f.stem] = rep_
            except (OSError, ValueError, TypeError, AttributeError):
                pass
        return out

    @staticmethod
    def clean_rows(rows):
        """A peer's rows, reduced to the known fields of plain types, so one bad report cannot break
        the page for every viewer. ValueError when it is not a list of rows."""
        if not isinstance(rows, list) or len(rows) > 500 or not all(isinstance(r, dict) for r in rows):
            raise ValueError("bad rows")
        plain = lambda v: v if v is None or isinstance(v, (str, int, float)) else str(v)
        return [{k: (r.get(k) if isinstance(r.get(k), dict) else None) if k == "record" else plain(r.get(k))
                 for k in REPORT_KEYS} for r in rows]

    def receive(self, ip, body):
        """A peer's report: accepted only when Tailscale says the caller IS that device, and the
        device is a member of this takt-net. Nothing in a report runs anything."""
        try:
            host, rows = str(body["host"]).lower(), self.clean_rows(body["rows"])
        except (KeyError, TypeError, ValueError):
            return 400, "bad report"
        if host == self.net["self"] or host not in self.net["hosts"]:
            return 403, f"{host} is not a device of this takt-net"
        who = self.whois(ip)
        if who != host:
            return 403, f"the tailnet says {ip} is {who or 'unknown'}, not {host}"
        rep_ = {"host": host, "received": self.now().isoformat(timespec="seconds"), "rows": rows}
        d = self.state / "reports"
        with self.lock:  # two jobs of one device can report at once, on two server threads
            d.mkdir(parents=True, exist_ok=True)
            tmp = d / f".{host}.{os.getpid()}.{threading.get_ident()}.tmp"
            tmp.write_text(json.dumps(rep_))
            os.replace(tmp, d / f"{host}.json")
            self.reports[host] = rep_
            self.cache = (0.0, None)
        return 200, "ok"

    def age(self, host):
        rep_ = self.reports.get(host)
        if not rep_:
            return "no report yet"
        try:
            s = int((self.now() - datetime.fromisoformat(rep_["received"])).total_seconds())
        except (KeyError, ValueError, TypeError):
            return "report time unknown"
        return "reported " + (f"{s}s" if s < 120 else f"{s // 60} min" if s < 7200 else f"{s // 3600} h") + " ago"

    def _local_rows(self):
        spec = load_spec(self.spec) if Path(self.spec).exists() else {}
        be = backend()
        return status_rows(spec, self.state, native_states(spec, be, default_dir(be, self.state)))

    @staticmethod
    def _run(argv):
        try:
            r = subprocess.run(argv, capture_output=True, text=True, timeout=60, stdin=subprocess.DEVNULL, **NO_WINDOW)
        except (OSError, subprocess.SubprocessError) as e:
            return 1, str(e)
        return r.returncode, (r.stdout + r.stderr).strip()

    def rows(self):
        """Local rows plus every peer's, in parallel; an unreachable peer is one row, never an error.
        Cached for `ttl` seconds, so open pages share one fan-out."""
        with self.lock:
            at, data = self.cache
            if data is None or time.monotonic() - at > self.ttl:
                data = self.collect()
                self.cache = (time.monotonic(), data)
            return data

    def collect(self):
        me = self.net["self"]
        if web_scope(self.net) == "off":
            return {"rows": [], "note": "status display is off (settings.status = none)"}
        try:
            rows = self.local_rows()
        except Exception as e:  # a broken jobs file must show, not kill the page
            rows = [{"id": "-", "sched": "-", "status": "error", "last": None, "next": None, "detail": str(e)}]
        for r in rows:
            r["host"] = me
        peers = [h for h in self.net["hosts"] if h != me]
        for h in peers:  # from the peer's last report: this server never logs in to a peer
            rep_ = self.reports.get(h)
            if rep_:
                rows += [{**r, "host": h} for r in rep_["rows"]]
            else:
                rows.append({"host": h, "id": "-", "sched": "-", "status": "no report", "last": None, "next": None,
                             "detail": f"{h} reports after each job run, or with `takt report`"})
        rows = [{k: v for k, v in r.items() if k != "record"} for r in rows]
        return {"rows": rows, "note": None, "reports": " · ".join(f"{h} {self.age(h)}" for h in peers)}

    def argv(self, host, args):
        """The command for `host`: this script for the local device, ssh for a peer. None if unknown."""
        if host == self.net["self"]:
            return [sys.executable, str(Path(__file__).resolve()), *args, "--spec", str(self.spec), "--state", str(self.state)]
        if host in self.net["hosts"]:
            a = remote_argv(host, args, self.net["hosts"][host])
            return a[:1] + SSH_OPTS + a[1:]
        return None

    def job_call(self, host, jid, args):
        if not ID_RE.fullmatch(jid or ""):
            return 400, "bad job id"
        argv = self.argv(host, args)
        if argv is None:
            return 400, "unknown device"
        code, text = self.run_(argv)
        return (200 if code == 0 else 502), text

    def show(self, host, jid):
        if host == self.net["self"] or host not in self.net["hosts"]:
            return self.job_call(host, jid, ["show", jid])
        if not ID_RE.fullmatch(jid or ""):
            return 400, "bad job id"
        rep_ = self.reports.get(host) or {}
        row = next((r for r in rep_.get("rows", []) if r.get("id") == jid), None)
        if row is None:
            return 404, f"no report from {host} for {jid}"
        try:
            text = format_record(jid, row.get("record")) if isinstance(row.get("record"), dict) else f"{jid}: never run"
        except (TypeError, AttributeError, KeyError, ValueError, IndexError):
            return 502, f"{host} reported a record for {jid} that takt cannot read"
        return 200, f"{text}\n\n({host} {self.age(host)}; the log tails stay on {host})"

    def act(self, host, jid, verb):
        if not self.writes:
            return 403, "read-only: start the server with --allow-writes"
        if verb not in WEB_VERBS:
            return 400, "bad action"
        self.cache = (0.0, None)  # the next refresh shows the effect
        return self.job_call(host, jid, [verb, jid, "--allow-writes"])


def web_handler(app):
    class H(BaseHTTPRequestHandler):
        server_version = "takt"
        timeout = 10  # a client that stops sending mid-request frees its thread

        def log_message(self, fmt, *args):
            if app.quiet:
                return
            print(f"{self.address_string()} {fmt % args}", file=sys.stderr, flush=True)

        def send(self, code, body, ctype="text/plain; charset=utf-8"):
            data = body.encode() if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; "
                             "style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'")
            self.end_headers()
            self.wfile.write(data)

        def json(self, code, obj):
            self.send(code, json.dumps(obj), "application/json")

        def do_GET(self):
            if not host_ok(self.headers.get("Host")):
                return self.send(403, "bad Host header")
            u = urlsplit(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            if u.path == "/":
                return self.send(200, WEB_HTML, "text/html; charset=utf-8")
            if u.path == "/api/status":
                return self.json(200, {**app.rows(), "self": app.net["self"], "writes": app.writes,
                                       "generated": datetime.now().isoformat(timespec="seconds")})
            if u.path == "/api/show":
                code, text = app.show(q.get("host", ""), q.get("id", ""))
                return self.send(code, text)
            self.send(404, "not found")

        def do_POST(self):
            if not host_ok(self.headers.get("Host")):
                return self.send(403, "bad Host header")
            # CSRF: a custom header cannot be sent cross-origin without a preflight, which this
            # server never answers; Origin, when sent, must be this server.
            origin = self.headers.get("Origin")
            if self.headers.get("X-Takt") != "1" or (origin and urlsplit(origin).netloc != self.headers.get("Host")):
                return self.send(403, "missing X-Takt header or foreign Origin")
            if urlsplit(self.path).path == "/api/report":
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                    if n < 0:
                        raise ValueError("negative length")
                    if n > 2_000_000:
                        return self.send(413, "report too large")
                    body = json.loads(self.rfile.read(n) or b"{}")
                except (ValueError, TypeError):
                    return self.send(400, "bad report")
                code, text = app.receive(self.client_address[0], body)
                return self.send(code, text)
            if urlsplit(self.path).path != "/api/act":
                return self.send(404, "not found")
            try:
                n = int(self.headers.get("Content-Length") or 0)
                if n < 0:
                    raise ValueError("negative length")
                req = json.loads(self.rfile.read(min(n, 4096)) or b"{}")
                host, jid, verb = str(req["host"]), str(req["id"]), str(req["verb"])
            except (ValueError, KeyError, TypeError):
                return self.send(400, "bad request")
            code, text = app.act(host, jid, verb)
            self.send(code, text)
    return H


def web_target(net, host, finder=tailscale_ip, timeout=3):
    """Where to reach the web host `host`: settings.web_bind, else its Tailscale address (no MagicDNS
    needed), else the name itself."""
    addr = net.get("web_bind", {}).get(host) or finder(peer=host, timeout=timeout) or host
    return f"http://{'[' + addr + ']' if ':' in addr else addr}:{net.get('web_port', WEB_PORT)}/api/report"


REPORT_BUDGET = 8  # seconds for one report to every web host: address lookups and HTTP together


def send_reports(net, rows, post=None, finder=tailscale_ip, budget=REPORT_BUDGET, clock=time.monotonic):
    """POST this device's rows to every web host but itself. Best effort, and `budget` seconds at most
    in all (lookup 3 s, HTTP 5 s each, within the budget): a web host that is down never fails a job
    or holds its wrapper. -> {web host: "ok" or the error}."""
    def post_(url, body, timeout):
        req = urllib.request.Request(url, data=body, headers={"X-Takt": "1", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    post = post or post_
    body = json.dumps({"host": net["self"], "rows": rows}).encode()
    out, t0 = {}, clock()
    for h in net.get("web", []):
        if h == net["self"]:
            continue
        left = budget - (clock() - t0)
        if left < 0.5:
            out[h] = f"skipped: the report used its {budget} s"
            continue
        try:
            url = web_target(net, h, finder, timeout=min(3, left))
            left = budget - (clock() - t0)
            out[h] = "ok" if post(url, body, max(0.5, min(5, left))) == 200 else "refused"
        except Exception as e:  # noqa: BLE001  any failure is a line in the log, never a failed job
            out[h] = f"{type(e).__name__}: {e}"
    return out


def should_report(spec_path, env=os.environ):
    """Only a run of this device's own jobs file reports, so a test or a second --spec never shows
    up on the dashboard. TAKT_NO_REPORT=1 turns it off (the self-test sets it)."""
    return not env.get("TAKT_NO_REPORT") and Path(spec_path).resolve() == (CONFIG / "jobs.toml").resolve()


def report_now(spec_path, state):
    """Send this device's rows (its own jobs file) to the web hosts. Quiet when there are none."""
    net = net_settings()
    if not [h for h in net.get("web", []) if h != net["self"]]:
        return {}
    spec = load_spec(spec_path) if Path(spec_path).exists() else {}
    be = backend()
    return send_reports(net, status_rows(spec, Path(state), native_states(spec, be, default_dir(be, Path(state)))))


def web_main(a):
    net = net_settings()
    port = a.port if a.port is not None else net["web_port"]
    addr = web_bind_addr(a.bind, net)
    if ipaddress.ip_address(addr).is_unspecified:
        print("takt web: warning: this address listens on every interface, not only the tailnet", file=sys.stderr)
    app = WebApp(net, a.spec, state_dir(a), a.allow_writes or net.get("web_writes", False))
    srv = ThreadingHTTPServer((addr, port), web_handler(app))
    srv.daemon_threads = True
    shown = f"[{addr}]" if ":" in addr else addr
    print(f"takt web on http://{shown}:{srv.server_address[1]}/  "
          f"({'writes ON' if a.allow_writes else 'read-only'}; this device: {net['self']})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


# The service. Install writes it on a web host; install on a device that left `web`, and
# uninstall with no ids, remove it.

def web_service_argv(ctx, net):
    """The command the service runs, or None when this device is not a web host."""
    if net["self"] not in net["web"]:
        return None
    argv = [ctx["python"], ctx["script"], "web", "--spec", ctx["spec"], "--state", ctx["state"], "--port", str(net["web_port"])]
    if net["web_bind"].get(net["self"]):
        argv += ["--bind", net["web_bind"][net["self"]]]
    return argv + (["--allow-writes"] if net["web_writes"] else [])


def web_file(be, agents_dir: Path) -> Path:
    return agents_dir / {"launchd": f"{WEB_LABEL}.plist", "systemd": f"{WEB_UNIT}.service"}.get(be, f"{WEB_UNIT}.xml")


def render_web_launchd(argv, ctx) -> bytes:
    d = {"Label": WEB_LABEL, "ProgramArguments": argv, "EnvironmentVariables": {"PATH": ctx["path"]},
         "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 10, "ProcessType": "Background",
         "StandardOutPath": f"{ctx['state']}/log/web.log", "StandardErrorPath": f"{ctx['state']}/log/web.log"}
    return plistlib.dumps(d, sort_keys=False)


def render_web_systemd(argv, ctx) -> str:
    return ("[Unit]\nDescription=takt web dashboard\nAfter=network-online.target\n\n[Service]\nType=simple\n"
            f"Environment=PATH={ctx['path']}\nExecStart={' '.join(sd_quote(a) for a in argv)}\n"
            "Restart=always\nRestartSec=10\n\n[Install]\nWantedBy=default.target\n")


def render_web_schtasks(argv) -> str:
    ET.register_namespace("", TS_NS)
    root = ET.Element(f"{{{TS_NS}}}Task", version="1.2")
    _sub(_sub(root, "RegistrationInfo"), "Description", "takt web dashboard")
    _sub(_sub(_sub(root, "Triggers"), "LogonTrigger"), "Enabled", "true")
    s = _sub(root, "Settings")
    _sub(s, "MultipleInstancesPolicy", "IgnoreNew")
    _sub(s, "DisallowStartIfOnBatteries", "false")
    _sub(s, "StopIfGoingOnBatteries", "false")
    _sub(s, "ExecutionTimeLimit", "PT0S")  # no limit: it is a server
    r = _sub(s, "RestartOnFailure")
    _sub(r, "Interval", "PT1M")
    _sub(r, "Count", 999)
    ex = _sub(_sub(root, "Actions"), "Exec")
    _sub(ex, "Command", re.sub(r"python\.exe$", "pythonw.exe", argv[0], flags=re.I))
    _sub(ex, "Arguments", " ".join(sd_quote(a) for a in argv[1:]))
    ET.indent(root)
    return ET.tostring(root, encoding="unicode") + "\n"


def plan_web(be, ctx, agents_dir: Path, argv, loaded=None):
    """Actions that install the web service (`argv` is its command) or retire it (`argv` falsy;
    nothing if it is not there). `loaded` is the set of launchd labels, as for plan_install."""
    f, acts, uid = web_file(be, agents_dir), [], _uid()
    if be == "launchd":
        t, here = f"gui/{uid}/{WEB_LABEL}", f.exists() or WEB_LABEL in (loaded or ())
        if here:
            acts.append(("run" if loaded is not None and WEB_LABEL in loaded else "try", ["launchctl", "bootout", t], None))
        if argv:
            acts += [("mkdir", Path(ctx["state"]) / "log", None), ("write", f, render_web_launchd(argv, ctx)),
                     ("run", ["launchctl", "enable", t], None), ("run", ["launchctl", "bootstrap", f"gui/{uid}", str(f)], None)]
        elif here:
            acts.append(("rm", f, None))
    elif be == "systemd":
        u = f"{WEB_UNIT}.service"
        if argv:
            acts += [("write", f, render_web_systemd(argv, ctx).encode()), ("run", ["systemctl", "--user", "daemon-reload"], None),
                     ("run", ["systemctl", "--user", "enable", u], None), ("run", ["systemctl", "--user", "restart", u], None)]
        elif f.exists():
            acts += [("try", ["systemctl", "--user", "disable", "--now", u], None), ("rm", f, None),
                     ("run", ["systemctl", "--user", "daemon-reload"], None)]
    else:
        if argv:
            if f.exists():
                acts.append(("try", ["schtasks", "/End", "/TN", WEB_TASK], None))
            acts += [("write", f, render_web_schtasks(argv).encode("utf-16")),
                     ("run", ["schtasks", "/Create", "/TN", WEB_TASK, "/XML", str(f), "/F"], None),
                     ("run", ["schtasks", "/Run", "/TN", WEB_TASK], None)]
        elif f.exists():
            acts += [("try", ["schtasks", "/End", "/TN", WEB_TASK], None),
                     ("try", ["schtasks", "/Delete", "/TN", WEB_TASK, "/F"], None), ("rm", f, None)]
    return acts


WEB_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>takt</title>
<style>
:root{--bg:#fff;--fg:#1c1f23;--mut:#6b7280;--line:#e5e7eb;--card:#f6f7f9;--ok:#15803d;--bad:#b91c1c;--warn:#b45309;--acc:#2563eb}
@media(prefers-color-scheme:dark){:root{--bg:#14161a;--fg:#e6e8eb;--mut:#9aa1ab;--line:#2a2e35;--card:#1c1f25;--ok:#4ade80;--bad:#f87171;--warn:#fbbf24;--acc:#60a5fa}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif}
header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;padding:12px 16px;border-bottom:1px solid var(--line)}
h1{font-size:18px;margin:0}.mut{color:var(--mut)}main{padding:12px 16px}
input[type=search]{padding:5px 8px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--fg)}
.tw{overflow-x:auto}table{border-collapse:collapse;width:100%;min-width:640px}
th,td{text-align:left;padding:5px 10px;border-bottom:1px solid var(--line);white-space:nowrap}
td.d{white-space:normal;color:var(--mut);max-width:40ch}tbody tr{cursor:pointer}tbody tr:hover,tr.sel{background:var(--card)}
.ok{color:var(--ok)}.failed,.unreachable,.error,.lock-timeout{color:var(--bad)}.partial,.skipped{color:var(--warn)}.never{color:var(--mut)}
pre{background:var(--card);border:1px solid var(--line);border-radius:6px;padding:10px;overflow:auto;max-height:50vh;white-space:pre-wrap}
button{padding:4px 12px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--fg);cursor:pointer}
button:hover{border-color:var(--acc)}#banner{color:var(--bad)}
</style></head><body>
<header><h1>takt</h1><span id="meta" class="mut"></span><input id="q" type="search" placeholder="filter">
<label class="mut"><input id="auto" type="checkbox" checked> refresh every 15 s</label><button id="re">refresh</button><span id="banner"></span></header>
<main><div class="tw"><table><thead><tr><th>HOST</th><th>ID</th><th>SCHED</th><th>STATUS</th><th>LAST</th><th>NEXT</th><th>DETAIL</th></tr></thead><tbody id="rows"></tbody></table></div>
<h2 id="title" class="mut" style="font-size:15px">Select a job</h2><div id="acts"></div><span id="msg" class="mut"></span><pre id="show" hidden></pre></main>
<script>
const $=s=>document.querySelector(s);let data={rows:[],writes:false},sel=null,timer=null;
const hm=s=>s?s.slice(5,16).replace('T',' '):'-';
function td(t,c){const e=document.createElement('td');e.textContent=t;if(c)e.className=c;return e}
function render(){const f=$('#q').value.toLowerCase(),tb=$('#rows');tb.textContent='';
 for(const r of data.rows){const line=[r.host,r.id,r.sched,r.status,r.detail].join(' ').toLowerCase();if(f&&!line.includes(f))continue;
  const tr=document.createElement('tr');if(sel&&sel.host===r.host&&sel.id===r.id)tr.className='sel';
  tr.append(td(r.host),td(r.id),td(r.sched),td(r.status,r.status),td(hm(r.last)),td(hm(r.next)),td(r.detail||'','d'));
  if(r.id!=='-')tr.onclick=()=>{sel={host:r.host,id:r.id};render();detail()};tb.append(tr)}
 $('#meta').textContent=(data.note||('on '+data.self+' · '+(data.writes?'writes on':'read-only')+' · '+(data.generated||'')+(data.reports?' · '+data.reports:'')))}
async function load(){try{const r=await fetch('/api/status');data=await r.json();$('#banner').textContent='';render();if(sel)detail()}
 catch(e){$('#banner').textContent='cannot reach takt: '+e}}
async function detail(){if(!sel)return;$('#title').textContent=sel.host+' / '+sel.id;const p=$('#show');p.hidden=false;
 try{const r=await fetch('/api/show?host='+encodeURIComponent(sel.host)+'&id='+encodeURIComponent(sel.id));p.textContent=await r.text()}catch(e){p.textContent=String(e)}
 const a=$('#acts');a.textContent='';if(data.writes)for(const v of ['start','enable','disable']){const b=document.createElement('button');b.textContent=v;b.onclick=()=>act(v);a.append(b,' ')}}
async function act(v){$('#msg').textContent=v+'...';
 try{const r=await fetch('/api/act',{method:'POST',headers:{'X-Takt':'1','Content-Type':'application/json'},body:JSON.stringify({host:sel.host,id:sel.id,verb:v})});
  $('#msg').textContent=(await r.text()).split('\n').pop()}catch(e){$('#msg').textContent=String(e)}load()}
function tick(){clearInterval(timer);if($('#auto').checked)timer=setInterval(load,15000)}
$('#q').oninput=render;$('#re').onclick=load;$('#auto').onchange=tick;load();tick();
</script></body></html>
"""


# ------------------------------------------------------------------------- CLI

def starter_spec():
    """A first jobs.toml. python and path are pinned for the scheduler: `which python3` is a
    stable symlink, where realpath(sys.executable) is a versioned Homebrew Cellar path, and a
    captured shell PATH can hold throwaway entries (fnm multishells). On Windows `python3`
    is the Microsoft Store stub, so the running interpreter is used."""
    py = sys.executable if os.name == "nt" else (shutil.which("python3") or sys.executable)
    out = ["# takt jobs. Reference: https://github.com/damsleth/takt/blob/main/docs/jobs.md", "",
           "[settings]", f"python = {toml_val(py)}"]
    if os.name != "nt":
        out.append(f"path = {toml_val(str(Path.home() / '.local/bin') + ':' + DEFAULT_PATH)}")
    out += ["", "# A web dashboard on one device of your tailnet (docs/web.md). Install starts it there:",
            '# web = ["myvps"]']
    out += ["", "# Every 15 minutes, append a timestamp to a log. Replace it with a real job.",
            "[job.hello]", 'schedule = "*/15 * * * *"',
            f"command = {toml_val([py, '-c', 'import datetime; print(datetime.datetime.now().isoformat())'])}",
            'stdout = "~/.local/state/takt/hello.log"', ""]
    return "\n".join(out)


def init_spec(path: Path):
    if path.exists():
        sys.exit(f"{path} exists. Edit it, or remove it first.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(starter_spec(), encoding="utf-8")
    print(f"wrote {path}\nnext: takt install (shows the plan), then takt install --allow-writes")
    return 0


def make_ctx(a):
    """Render context. `[settings] path/python` pin what the scheduler gets; a captured shell PATH
    can carry throwaway entries (fnm multishells), so the spec should pin it."""
    cfg = read_toml(a.spec).get("settings", {})
    return {"python": cfg.get("python") or os.path.realpath(sys.executable),
            "script": str(Path(__file__).resolve()), "spec": str(Path(a.spec).resolve()),
            "path": cfg.get("path") or os.environ.get("PATH", DEFAULT_PATH), "state": str(state_dir(a))}


def build_parser():
    ap = argparse.ArgumentParser(prog="takt", description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="offline self-test")
    ap.add_argument("--version", action="version", version=f"takt {__version__}")
    sub = ap.add_subparsers(dest="cmd")

    def common(p, spec=True):
        if spec:
            p.add_argument("--spec", default=str(CONFIG / "jobs.toml"))
        p.add_argument("--state", help="state dir (default ~/.local/state/takt or $TAKT_STATE)")
        return p
    r = common(sub.add_parser("run", help="run one job through the wrapper"))
    r.add_argument("id")
    r.add_argument("--scheduled", action="store_true", help="started by the scheduler (enables dedup/skip)")
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--now", help="ISO local time, for tests")
    st = common(sub.add_parser("status", help="scheduler state and last run of every job"))
    st.add_argument("--json", action="store_true")
    st.add_argument("-A", "--all-hosts", action="store_true", help="every device in the takt-net, if settings.status allows")
    st.add_argument("--here", action="store_true", help="this device's jobs, whatever settings.status says")
    sh = common(sub.add_parser("show", help="last record and log tails of one job"))
    sh.add_argument("id")
    for verb in ("start", "enable", "disable"):
        v = common(sub.add_parser(verb, help=f"{verb} one installed job through the native scheduler"))
        v.add_argument("id")
        v.add_argument("--allow-writes", action="store_true")
    u = sub.add_parser("ui", help="fzf TUI over every host (also: bare `takt` on a terminal)")
    u.add_argument("--allow-writes", action="store_true", help="enable the start/enable/disable keys")
    rd = common(sub.add_parser("render", help="stage backend files"))
    rd.add_argument("--out", default="staging")
    rd.add_argument("--backend", choices=["launchd", "systemd", "schtasks", "all"], default="all")
    common(sub.add_parser("preflight", help="real needs/wants checks for every job"))
    ip = sub.add_parser("import-plist", help="read a LaunchAgent plist into spec TOML")
    ip.add_argument("file")
    ic = sub.add_parser("import-cron", help="read `crontab -l` (or --file) into spec TOML")
    ic.add_argument("--file")
    for name in ("install", "uninstall"):
        ins = common(sub.add_parser(name, help=f"{name} plan; --allow-writes performs it"))
        if name == "uninstall":
            ins.add_argument("ids", nargs="*", help="default: every job in the spec")
        ins.add_argument("--allow-writes", action="store_true")
        ins.add_argument("--agents-dir", help="LaunchAgents / systemd user unit dir (default per OS)")
    common(sub.add_parser("report", help="send this device's job rows to the web hosts now"))
    sub.add_parser("push", help="with --host H: copy takt.py and jobs.H.toml to H").add_argument(
        "--allow-writes", action="store_true")
    common(sub.add_parser("init", help="write a starter jobs.toml (never overwrites)"))
    up = common(sub.add_parser("update", help="install the takt.py on GitHub (settings.update_ref, default main)"))
    up.add_argument("-A", "--all-hosts", action="store_true", help="also every host with a jobs.<host>.toml")
    up.add_argument("--ref", help="branch or tag (default: settings.update_ref, else main)")
    up.add_argument("--force", action="store_true", help="also replace a takt.py in a git checkout")
    w = common(sub.add_parser("web", help="serve the dashboard to the tailnet (Tailscale address only)"))
    w.add_argument("--port", type=int, help=f"default settings.web_port, else {WEB_PORT}")
    w.add_argument("--bind", help="address to listen on (default: this device's Tailscale IPv4)")
    w.add_argument("--allow-writes", action="store_true", help="show start/enable/disable buttons")
    return ap


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["--host"] and len(argv) > 1:
        if argv[1] != "local":
            return on_host(argv[1], argv[2:])
        argv = argv[2:]
    if not argv and sys.stdin.isatty() and sys.stdout.isatty():
        return ui(False)
    ap = build_parser()
    a = ap.parse_args(argv)

    if a.check:
        return self_check()
    if a.cmd == "ui":
        return ui(a.allow_writes)
    if a.cmd == "push":
        sys.exit("push needs a host: takt --host <host> push --allow-writes")
    if a.cmd == "init":
        return init_spec(Path(a.spec))
    if a.cmd == "update":
        cfg = read_toml(a.spec).get("settings", {}) if Path(a.spec).exists() else {}
        code = update(a.spec, a.ref or cfg.get("update_ref", "main"), a.force)
        for h in (hosts()[1:] if a.all_hosts else []):
            print(f"{h}:", flush=True)
            code = on_host(h, ["update"] + (["--ref", a.ref] if a.ref else [])) or code
        return code
    if a.cmd == "report":
        res = report_now(a.spec, state_dir(a))
        for h, r in res.items():
            print(f"{h}: {r}")
        if not res:
            print("no web hosts in settings.web")
        return 0 if all(r == "ok" for r in res.values()) else 1
    if a.cmd == "web":
        return web_main(a)
    if a.cmd == "run":
        spec = load_spec(a.spec)
        if a.id not in spec:
            sys.exit(f"unknown job {a.id!r}; have: {', '.join(spec)}")
        now = datetime.fromisoformat(a.now) if a.now else None
        if CONTAIN and not a.dry_run and (err := end_steps_with_wrapper()):
            # without the job object, /End or a killed wrapper would leave steps running unlocked
            note = f"not run: cannot put the steps in a Windows job object ({err})"
            write_record(state_dir(a), {"id": a.id, "slot": slot_of(spec[a.id], now or datetime.now()).isoformat(),
                                        "trigger": "scheduled" if a.scheduled else "manual", "pulled_by": None,
                                        "started": datetime.now().isoformat(timespec="seconds"), "duration_s": 0,
                                        "status": "skipped", "exit": 2, "steps": [], "preflight": [], "note": note})
            print(f"{a.id}: {note}", file=sys.stderr)
            return 2
        scheduled, trig = a.scheduled, None
        if scheduled and not a.dry_run and take_start(state_dir(a), a.id):
            scheduled, trig = False, "start"  # asked for by `takt start`: run it, whatever the slot
        code = run_job(spec, a.id, state_dir(a), now, scheduled, a.dry_run, trigger=trig)
        # report this device to the web hosts: only for its own jobs file, so a test or a second
        # --spec never shows up on the dashboard. Never fails the job.
        if not a.dry_run and should_report(a.spec):
            try:
                for h, res in report_now(a.spec, state_dir(a)).items():
                    if res != "ok":
                        print(f"report to {h}: {res}", file=sys.stderr)
            except Exception as e:  # noqa: BLE001
                print(f"report: {e}", file=sys.stderr)
        return code
    be = backend()
    adir = Path(getattr(a, "agents_dir", None) or default_dir(be, state_dir(a)))
    if a.cmd == "status":
        net = net_settings()
        scope, note = status_scope(net, a.all_hosts, a.here)
        if scope == "off":
            print("[]" if a.json else note)
            if a.json:
                print(note, file=sys.stderr)
            return 0
        spec = load_spec(a.spec) if Path(a.spec).exists() else {}  # a controller may only have hosts
        rows = status_rows(spec, state_dir(a), native_states(spec, be, adir))
        if scope == "net":
            peers = [(h, c) for h, c in net["hosts"].items() if h != net["self"]]
            with ThreadPoolExecutor() as ex:
                rows += [x for rs in ex.map(lambda hc: remote_rows(*hc), peers) for x in rs]
        if a.json:
            print(json.dumps(rows, indent=1))
        else:
            print("\n".join(fmt_rows(rows)))
        if note:
            print(note, file=sys.stderr)
        return 0
    if a.cmd == "show":
        return show(load_spec(a.spec), state_dir(a), a.id)
    if a.cmd in ("start", "enable", "disable"):
        spec = load_spec(a.spec)
        if a.id not in spec:
            sys.exit(f"unknown job {a.id!r}; have: {', '.join(spec)}")
        cmds = admin_argv(be, a.cmd, a.id, adir)
        if not a.allow_writes:
            print("\n".join("PLAN " + " ".join(c) for c in cmds) + "\nnothing done. Re-run with --allow-writes.")
            return 0
        if a.cmd == "start":
            if job_running(state_dir(a), a.id):  # the scheduler would not start a second copy; no request
                print(f"{a.id}: already running; not started")
                return 0
            request_start(state_dir(a), a.id)
        err = run_admin(cmds)
        if err:
            if a.cmd == "start":
                (state_dir(a) / "start" / a.id).unlink(missing_ok=True)
            print(f"{a.id}: {a.cmd} failed: {err}")
            return 1
        print(f"{a.id}: {a.cmd} ok ({be})")
        return 0
    if a.cmd == "render":
        spec = load_spec(a.spec)
        bs = ["launchd", "systemd", "schtasks"] if a.backend == "all" else [a.backend]
        for p in stage(spec, make_ctx(a), Path(a.out), bs):
            print(p)
        return 0
    if a.cmd == "preflight":
        spec, cache, bad = load_spec(a.spec), {}, 0
        for j in spec.values():
            for p in preflight(j, cache):
                bad += not p["ok"]
                print(f"{j['id']:14} {'needs' if p['hard'] else 'wants'} {p['need']:14} "
                      f"{'ok     ' if p['ok'] else 'MISSING'} {p['detail']}")
        return 1 if bad else 0
    if a.cmd == "import-plist":
        print(toml_job(parse_launchd(Path(a.file).expanduser().read_bytes())))
        return 0
    if a.cmd == "import-cron":
        text = Path(a.file).read_text(encoding="utf-8") if a.file else subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True).stdout
        jobs, skipped, warns = import_cron(text)
        for w in warns:
            print(f"# warning: {w}", file=sys.stderr)
        print(f"# {len(jobs)} live jobs; commented-out cron lines ignored: {len(skipped)}", file=sys.stderr)
        print("\n".join(toml_job(j) for j in jobs))
        return 0
    if a.cmd in ("install", "uninstall"):
        spec = load_spec(a.spec)
        try:
            known, nat, loaded = inventory(spec, be, adir, extra=getattr(a, "ids", None) or ())
        except RuntimeError as e:
            print(f"failed: {e}. Nothing changed.", file=sys.stderr)
            return 1
        try:
            spec_path = Path(a.spec).resolve()
            ctx = make_ctx(a) if a.cmd == "install" else None
            acts = plan_admin(a.cmd, spec, be, adir, known, nat, ids=getattr(a, "ids", None), ctx=ctx,
                              crontab=read_crontab(spec) if a.cmd == "install" else "", loaded=loaded,
                              owned=[i for i in known if installed_from(be, adir, i, spec_path)],
                              web=(web_service_argv(ctx, net_settings()) or False) if ctx else False)
        except ValueError as e:  # a job the scheduler cannot take: nothing has changed yet
            print(f"failed: {e}. Nothing changed.", file=sys.stderr)
            return 1
        for x in acts:
            print(("DO   " if a.allow_writes else "PLAN ") + describe(x))
        if not a.allow_writes:
            print("\nnothing written. Re-run with --allow-writes to perform the plan above.")
            return 0
        try:
            apply_plan(acts, run_checked)
        except subprocess.CalledProcessError as e:
            print(f"failed: {' '.join(e.cmd)} exited {e.returncode}. Stopped; the steps after it did not run.",
                  file=sys.stderr)
            return 1
        return 0
    ap.print_help()
    return 0


# ----------------------------------------------------------------------- check

def self_check():
    # The checks call main(["run", ...]) in this process. On Windows that would put this process in a
    # kill-on-close job, and the next Locks.__exit__ would end the wrappers the checks spawn. Spawned
    # wrappers keep containment (their own module), and check_windows_last tests it directly.
    global CONTAIN
    CONTAIN = False
    os.environ["TAKT_NO_REPORT"] = "1"  # wrappers spawned by the checks never report to a real web host
    os.environ["TAKT_NO_UPDATE_JOB"] = "1"  # specs written by the checks hold only their own jobs; check_update tests it
    n = {"ok": 0, "bad": 0}

    def ok(cond, name):
        n["ok" if cond else "bad"] += 1
        if not cond:
            print(f"FAIL {name}")

    tmp = Path(tempfile.mkdtemp(prefix="takt-check-"))
    try:
        check_cron(ok)
        check_render(ok)
        check_runtime(ok, tmp)
        check_import(ok, tmp)
        check_install(ok, tmp)
        check_admin(ok, tmp)
        check_review(ok, tmp)
        check_review2(ok, tmp)
        check_review3(ok, tmp)
        check_review4(ok, tmp)
        check_review5(ok, tmp)
        check_review6(ok, tmp)
        check_review7(ok, tmp)
        check_review8(ok, tmp)
        check_review9(ok, tmp)
        check_review10(ok, tmp)
        check_owa_reseed(ok, tmp)
        check_status_scope(ok, tmp)
        check_windows_jobs(ok, tmp)
        check_owned(ok, tmp)
        check_web(ok, tmp)
        check_plans_apply(ok, tmp)
        check_web_reports(ok, tmp)
        check_pub_review1(ok, tmp)
        check_pub_review2(ok, tmp)
        check_pub_review3(ok, tmp)
        check_watch(ok, tmp)
        check_update(ok, tmp)
        check_examples(ok, tmp)
        check_windows_last(ok)  # joins a kill-on-close job on Windows, so it runs last
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"{n['ok']} passed, {n['bad']} failed")
    return 1 if n["bad"] else 0


CTX = {"python": "/usr/bin/python3", "script": "/opt/takt/takt.py", "spec": "/opt/takt/jobs.toml",
       "path": "/usr/local/bin:/usr/bin", "state": "/var/takt"}


def mkjob(jid, **kw):
    if "step" not in kw:
        kw.setdefault("command", ["true"])
    return normalize(jid, kw)


def check_cron(ok):
    ok(parse_cron("*/15 * * * *")[0] == [0, 15, 30, 45], "step minute expands")
    ok(parse_cron("0 */2 * * *")[1] == list(range(0, 24, 2)), "step hour expands")
    ok(parse_cron("* * * * *") == [None] * 5, "all stars")
    ok(parse_cron("0 0 * * 7")[4] == [0], "dow 7 is sunday")
    for bad in ("*/0 * * * *", "99 * * * *", "* * * *", "0 0 1 * 1"):
        try:
            parse_cron(bad)
            ok(False, f"rejects {bad}")
        except ValueError:
            ok(True, "")
    for e in ("0 * * * *", "*/15 * * * *", "0 */2 * * *", "* * * * *", "30 6 * * 1-5".replace("1-5", "1,2,3,4,5"),
              "5,25 0,1,2 * * *", "0 9 1 * *"):
        k, v = launchd_schedule(parse_cron(e))
        d = {"Label": "x", "ProgramArguments": ["x"]}
        d["StartInterval" if k == "interval" else "StartCalendarInterval"] = v if k == "interval" else (v[0] if len(v) == 1 else v)
        ok(parse_launchd(plistlib.dumps(d))["schedule"] == e, f"cron -> launchd -> cron {e}")
    t = datetime(2026, 9, 30, 14, 0)
    ok(prev_slot(parse_cron("0 */2 * * *"), t + timedelta(minutes=33)) == t, "prev_slot after a 33 minute gap")
    ok(matches(parse_cron("0 */2 * * *"), t) and not matches(parse_cron("10 */2 * * *"), t), "matches")


def check_render(ok):
    c = CTX
    yaams = mkjob("yaams-ingest", schedule="0 */2 * * *", lock=["owa-edge"], after=["owa-reseed"])
    p = plistlib.loads(render_launchd(yaams, c))
    cal = p["StartCalendarInterval"]
    ok(len(cal) == 12 and cal[0] == {"Hour": 0, "Minute": 0} and cal[-1] == {"Hour": 22, "Minute": 0},
       "launchd 0 */2 -> 12 dicts")
    ok(p["ProgramArguments"] == ["/usr/bin/python3", "/opt/takt/takt.py", "run", "yaams-ingest",
                                 "--spec", "/opt/takt/jobs.toml", "--state", "/var/takt", "--scheduled"], "launchd runs through the wrapper, state dir pinned")
    ok(p["EnvironmentVariables"] == {"PATH": "/usr/local/bin:/usr/bin"} and p["Label"] == f"{PREFIX}.yaams-ingest",
       "launchd captures PATH and label")
    q = plistlib.loads(render_launchd(mkjob("q", schedule="*/15 * * * *"), c))
    ok(q["StartCalendarInterval"] == [{"Minute": m} for m in (0, 15, 30, 45)], "launchd */15 -> 4 dicts")
    m = plistlib.loads(render_launchd(mkjob("m", schedule="* * * * *"), c))
    ok(m["StartInterval"] == 60 and "StartCalendarInterval" not in m, "launchd every minute -> StartInterval 60")
    h = plistlib.loads(render_launchd(mkjob("h", schedule="0 * * * *", run_at_load=True), c))
    ok(h["StartCalendarInterval"] == {"Minute": 0} and h["RunAtLoad"] is True, "launchd hourly + RunAtLoad")

    svc, tmr = render_systemd(yaams, c)
    ok(svc == ("[Unit]\nDescription=takt job yaams-ingest\n\n[Service]\nType=oneshot\n"
               "Environment=PATH=/usr/local/bin:/usr/bin\nExecStart=/usr/bin/python3 /opt/takt/takt.py run "
               "yaams-ingest --spec /opt/takt/jobs.toml --state /var/takt --scheduled\n"), "systemd service golden")
    ok(tmr == ("[Unit]\nDescription=takt timer yaams-ingest\n\n[Timer]\nOnCalendar=*-*-* 00/2:00:00\n"
               "Persistent=true\nUnit=takt-yaams-ingest.service\n\n[Install]\nWantedBy=timers.target\n"),
       "systemd timer golden (0 */2)")
    ok("OnCalendar=*-*-* *:00/15:00" in render_systemd(mkjob("q", schedule="*/15 * * * *"), c)[1], "systemd */15")
    t2 = render_systemd(mkjob("w", schedule="30 6 * * 1,2", catch_up="skip"), c)[1]
    t3 = render_systemd(mkjob("w", schedule="30 6 * * 1,2", catch_up="run-once", run_at_load=True), c)[1]
    ok("OnCalendar=Mon,Tue *-*-* 06:30:00" in t2 and "Persistent" not in t2 and "OnActiveSec=5s" in t3,
       "systemd weekday, skip has no Persistent, run_at_load")

    x = render_schtasks(yaams, c)
    gold = """<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task" version="1.2">"""
    ok(x.startswith(gold), "schtasks root golden")
    root = ET.fromstring(x)
    g = lambda e, tag: e.findall(f".//{{{TS_NS}}}{tag}")
    starts = [e.text for e in g(root, "StartBoundary")]
    ok(len(starts) == 12 and starts[0] == "2026-01-01T00:00:00" and starts[-1] == "2026-01-01T22:00:00",
       "schtasks 0 */2 -> 12 daily triggers")
    ok(g(root, "Command")[0].text == "/usr/bin/python3" and g(root, "StartWhenAvailable")[0].text == "true",
       "schtasks action + catch-up")
    ok(render_schtasks(mkjob("q", schedule="*/15 * * * *", catch_up="skip"), c) == SCHTASKS_GOLDEN, "schtasks golden XML (*/15, skip)")
    xq = ET.fromstring(render_schtasks(mkjob("q", schedule="*/15 * * * *"), c))
    ok(len(g(xq, "CalendarTrigger")) == 1 and g(xq, "Interval")[0].text == "PT15M" and g(xq, "Duration")[0].text == "P1D",
       "schtasks */15 -> one trigger repeating PT15M")
    xw = ET.fromstring(render_schtasks(mkjob("w", schedule="30 6 * * 1,5", catch_up="skip"), c))
    ok([e.tag.split("}")[1] for e in g(xw, "DaysOfWeek")[0]] == ["Monday", "Friday"]
       and g(xw, "StartWhenAvailable")[0].text == "false", "schtasks weekdays, skip -> no catch-up")
    xh = ET.fromstring(render_schtasks(mkjob("h", schedule="0 * * * *", run_at_load=True), c))
    ok(g(xh, "Interval")[0].text == "PT1H" and len(g(xh, "LogonTrigger")) == 1, "schtasks hourly + logon")

    real = b"""<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>Label</key><string>com.damsleth.owa-piggy.scheduled</string>
<key>ProgramArguments</key><array><string>/Users/x/.config/owa-piggy/OwaPiggy.app/Contents/MacOS/owa-piggy-reseed</string></array>
<key>AssociatedBundleIdentifiers</key><array><string>com.damsleth.owa-piggy.scheduled</string></array>
<key>StartCalendarInterval</key><dict><key>Minute</key><integer>0</integer></dict>
<key>RunAtLoad</key><true/>
<key>StandardOutPath</key><string>/dev/null</string>
<key>StandardErrorPath</key><string>/Users/x/.config/owa-piggy/refresh.log</string>
<key>ProcessType</key><string>Background</string></dict></plist>"""
    j = parse_launchd(real)
    ok(j["id"] == "owa-piggy-scheduled" and j["schedule"] == "0 * * * *" and j["run_at_load"]
       and j["bundle"] == "com.damsleth.owa-piggy.scheduled"
       and j["steps"][0]["command"][0].endswith("owa-piggy-reseed")
       and j["steps"][0]["stderr"] == "/Users/x/.config/owa-piggy/refresh.log", "real owa-piggy plist -> spec")
    back = plistlib.loads(render_launchd(normalize(j["id"], {**j, "command": j["steps"][0]["command"]}), c))
    ok(back["StartCalendarInterval"] == {"Minute": 0} and back["RunAtLoad"] and
       back["AssociatedBundleIdentifiers"] == ["com.damsleth.owa-piggy.scheduled"], "spec -> plist keeps schedule")
    ok(parse_launchd(render_launchd(yaams, c))["schedule"] == "0 */2 * * *", "rendered plist -> spec round trip")
    rt = tomllib.loads(toml_job(yaams))["job"]["yaams-ingest"]
    ok(normalize("yaams-ingest", rt) == yaams, "toml emit -> load round trip")


SCHTASKS_GOLDEN = '<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task" version="1.2">\n  <RegistrationInfo>\n    <Description>takt job q</Description>\n  </RegistrationInfo>\n  <Triggers>\n    <CalendarTrigger>\n      <Repetition>\n        <Interval>PT15M</Interval>\n        <Duration>P1D</Duration>\n        <StopAtDurationEnd>false</StopAtDurationEnd>\n      </Repetition>\n      <StartBoundary>2026-01-01T00:00:00</StartBoundary>\n      <Enabled>true</Enabled>\n      <ScheduleByDay>\n        <DaysInterval>1</DaysInterval>\n      </ScheduleByDay>\n    </CalendarTrigger>\n  </Triggers>\n  <Settings>\n    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n    <StartWhenAvailable>false</StartWhenAvailable>\n    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n  </Settings>\n  <Actions>\n    <Exec>\n      <Command>/usr/bin/python3</Command>\n      <Arguments>/opt/takt/takt.py run q --spec /opt/takt/jobs.toml --state /var/takt --scheduled</Arguments>\n    </Exec>\n  </Actions>\n</Task>\n'

PY = sys.executable
# One marker file per job name: Windows emulates append as seek-then-write, so two processes
# appending to one file at once clobber each other's lines (found running --check on kwin).
MARK = ("import sys,time;p,n,d=sys.argv[1:4];p=f'{p}.{n}';open(p,'a').write(f'{n} start {time.time()}\\n');"
        "time.sleep(float(d));open(p,'a').write(f'{n} end {time.time()}\\n')")


def mark_text(path):
    return "".join(q.read_text() for q in sorted(Path(path).parent.glob(Path(path).name + ".*")))


def marks(path):
    ev = {}
    for line in mark_text(path).splitlines():
        n, k, t = line.split()
        ev.setdefault(n, {})[k] = float(t)
    return ev


def write_spec(dirp, *jobs):
    p = Path(dirp) / "jobs.toml"
    p.write_text("\n".join(toml_job(j) for j in jobs))
    return p


def cmd_mark(mk, name, d=0.5):
    return [PY, "-c", MARK, str(mk), name, str(d)]


def spawn(spec, state, jid, now="2026-09-30T14:00:10"):
    return subprocess.Popen([PY, __file__, "run", jid, "--spec", str(spec), "--state", str(state),
                             "--scheduled", "--now", now], stdout=subprocess.PIPE, text=True)


def check_runtime(ok, tmp):
    # 1. shared lock: same minute, never overlap, the loser waits and succeeds
    for locked in (True, False):
        d = tmp / f"lock{locked}"
        d.mkdir()
        mk, lk = d / "m.txt", (["edge"] if locked else [])
        spec = write_spec(d, mkjob("a", schedule="0 * * * *", lock=lk, command=cmd_mark(mk, "a")),
                          mkjob("b", schedule="0 * * * *", lock=lk, command=cmd_mark(mk, "b")))
        ps = [spawn(spec, d / "st", "a"), spawn(spec, d / "st", "b")]
        [p.communicate() for p in ps]
        ev = marks(mk)
        first, second = sorted(ev, key=lambda k: ev[k]["start"])
        overlap = ev[second]["start"] < ev[first]["end"]
        if locked:
            ok(not overlap, "lock: second job starts after first ends")
            ra, rb = read_record(d / "st", "a"), read_record(d / "st", "b")
            loser = rb if first == "a" else ra
            ok(all(p.returncode == 0 for p in ps) and ra["status"] == rb["status"] == "ok", "lock: both succeed (wait, not fail)")
            ok(loser["waited_s"] >= 0.3 and loser["blocked_on"] is not None, "lock: loser recorded its wait")
        else:
            ok(overlap, "control: without a lock the same two jobs do overlap")
    # 1b. lock timeout is a visible status, not a hang
    d = tmp / "lockto"
    d.mkdir()
    mk = d / "m.txt"
    spec = write_spec(d, mkjob("a", lock=["edge"], command=cmd_mark(mk, "a", 1.5)),
                      mkjob("b", lock=["edge"], lock_timeout=1, command=cmd_mark(mk, "b")))
    pa = spawn(spec, d / "st", "a")
    time.sleep(0.4)
    pb = spawn(spec, d / "st", "b")
    pb.communicate(), pa.communicate()
    rb = read_record(d / "st", "b")
    ok(pb.returncode == 75 and rb["status"] == "lock-timeout" and "b" not in marks(mk), "lock timeout recorded, job not run")

    # 2. ordering: B after A, both due this slot. Whoever starts first, A runs once and before B.
    for first in ("a", "b"):
        d = tmp / f"after{first}"
        d.mkdir()
        mk = d / "m.txt"
        spec = write_spec(d, mkjob("a", schedule="0 */2 * * *", command=cmd_mark(mk, "a", 0.3)),
                          mkjob("b", schedule="0 */2 * * *", after=["a"], command=cmd_mark(mk, "b", 0.1)))
        order = ["a", "b"] if first == "a" else ["b", "a"]
        ps = []
        for jid in order:
            ps.append(spawn(spec, d / "st", jid, "2026-09-30T14:00:05"))
            time.sleep(0.05)
        [p.communicate() for p in ps]
        ev = marks(mk)
        lines = mark_text(mk).count("a start")
        ok(lines == 1 and ev["a"]["end"] <= ev["b"]["start"], f"after: a runs once, before b (b or a started first: {first})")
    ra = read_record(d / "st", "a")
    ok(ra["status"] == "ok", "after: a recorded ok")
    # wake at 15:33 spans two of a's hourly slots: b (14:00 slot) pulls a in once, recorded under
    # a's own 15:00, so a's own catch-up start dedups instead of running a second time
    d = tmp / "wake"
    d.mkdir()
    mk = d / "m.txt"
    spec = write_spec(d, mkjob("a", schedule="0 * * * *", command=cmd_mark(mk, "a", 0)),
                      mkjob("b", schedule="0 */2 * * *", after=["a"], command=cmd_mark(mk, "b", 0)))
    for jid in ("b", "a"):
        spawn(spec, d / "st", jid, "2026-09-30T15:33:00").communicate()
    ok(mark_text(mk).count("a start") == 1 and read_record(d / "st", "a")["slot"] == "2026-09-30T15:00:00",
       "after: wake spanning two dependency slots runs it once")
    # not due: dependency whose schedule does not match this slot is not pulled in
    d = tmp / "afternotdue"
    d.mkdir()
    mk = d / "m.txt"
    spec = write_spec(d, mkjob("a", schedule="0 3 * * *", command=cmd_mark(mk, "a", 0)),
                      mkjob("b", schedule="0 */2 * * *", after=["a"], command=cmd_mark(mk, "b", 0)))
    spawn(spec, d / "st", "b", "2026-09-30T14:00:05").communicate()
    ok("a" not in marks(mk) and "b" in marks(mk), "after: dependency not due this slot is not pulled in")
    try:
        normalize("x", {"command": ["true"], "after": ["y"]})
        spec2 = {"x": normalize("x", {"command": ["true"], "after": ["y"]}), "y": normalize("y", {"command": ["true"], "after": ["x"]})}
        chain_of(spec2, "x")
        ok(False, "cycle rejected")
    except ValueError:
        ok(True, "")

    # 3. status: exit code, duration, failing sub-steps from a yaams-style JSON line
    d = tmp / "status"
    d.mkdir()
    line = json.dumps({"totals": {"seen": 289, "new": 22}, "warnings": [],
                       "error": {"code": "partial_failure", "message": "2 source(s) failed; 26 succeeded",
                                 "failed_sources": ["imessage", "teams_channels_dno"]}})
    mk = d / "m.txt"
    job = mkjob("ing", step=[
        {"id": "index", "command": [PY, "-c", "pass"]},
        {"id": "tier2", "command": [PY, "-c", "import sys;sys.exit(3)"]},
        {"id": "ingest", "command": [PY, "-c", f"print({line!r})"], "report": "json-failed-sources",
         "stdout": str(d / "ingest.log")},
        {"id": "after-failure", "command": cmd_mark(mk, "last", 0)}])
    spec = write_spec(d, job)
    sp = load_spec(spec)
    code = run_job(sp, "ing", d / "st", say=lambda *_: None)
    r = read_record(d / "st", "ing")
    by = {s["id"]: s for s in r["steps"]}
    ok(code == 1 and r["status"] == "partial" and r["exit"] == 1, "status: job exit and status")
    ok(by["tier2"]["exit"] == 3 and by["tier2"]["status"] == "failed", "status: failing step exit code")
    ok(by["ingest"]["status"] == "partial" and by["ingest"]["failed"] == ["imessage", "teams_channels_dno"]
       and by["ingest"]["exit"] == 0, "status: failed_sources surfaced with exit 0")
    ok(all("duration_s" in s for s in r["steps"]) and r["duration_s"] >= 0 and "last" in marks(mk),
       "status: durations recorded, later steps still run")
    ok((d / "ingest.log").read_text().strip() == line, "status: stdout still appended to the log")
    row = status_rows(sp, d / "st", {})[0]
    ok("imessage" in row["detail"] and "tier2: failed exit 3" in row["detail"], "status: table names failing sub-steps")

    # 4. preflight: missing requirement reported by name, job not run blind
    d = tmp / "pre"
    d.mkdir()
    mk = d / "m.txt"
    locked_file = d / "chat.db"
    locked_file.write_text("x")
    locked_file.chmod(0)
    fake = d / "owa-piggy"
    future = (datetime.now(timezone.utc) + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    past = (datetime.now(timezone.utc) - timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    fake.write_text("#!/bin/sh\ncat <<E\n"
                    f"\nprofile:      good\nauthtoken:    expires {past} (-5h)\nrefreshtoken: expires {future} (4h59m)\nscheduled:    true\n"
                    f"\nprofile:      dead\nauthtoken:    expires {past} (-5h)\nrefreshtoken: expires {past} (-5h)\nscheduled:    true\n"
                    "\nprofile:      off\nstatus:       disabled\nE\n")
    fake.chmod(0o755)
    os.environ["TAKT_OWA_PIGGY"] = str(fake)
    try:
        hard = mkjob("hard", needs=["exe:takt-no-such-binary-xyz"], command=cmd_mark(mk, "hard", 0))
        fda = mkjob("fda", needs=[f"fda:{locked_file}"], command=cmd_mark(mk, "fda", 0))
        soft = mkjob("soft", wants=["owa:good", "owa:dead", "owa:off", "owa:ghost"], command=cmd_mark(mk, "soft", 0))
        sp = {j["id"]: j for j in (hard, fda, soft)}
        q = lambda *_: None
        ok(run_job(sp, "hard", d / "st", say=q) == 2 and read_record(d / "st", "hard")["status"] == "skipped"
           and "takt-no-such-binary-xyz" in read_record(d / "st", "hard")["note"], "preflight: missing exe named, job skipped")
        if hasattr(os, "geteuid") and os.geteuid() != 0:  # chmod 000 means nothing on Windows
            run_job(sp, "fda", d / "st", say=q)
            note = read_record(d / "st", "fda")["note"]
            ok("Full Disk Access" in note and os.path.realpath(sys.executable) in note and "fda" not in marks(mk),
               "preflight: FDA miss names the binary that needs the grant")
        ok(run_job(sp, "soft", d / "st", say=q) == 0 and "soft" in marks(mk), "preflight: wants do not block the job")
        rows = {p["need"]: (p["ok"], p["detail"]) for p in read_record(d / "st", "soft")["preflight"]}
        if os.name != "nt":  # the fake owa-piggy is a sh script
            ok(rows["owa:good"][0] and not rows["owa:dead"][0] and "needs sign-in" in rows["owa:dead"][1]
               and not rows["owa:off"][0] and not rows["owa:ghost"][0], "preflight: owa profile health, dead profile = needs sign-in")
        ok("warn owa:dead" in status_rows({"soft": soft}, d / "st", {})[0]["detail"], "preflight: warning visible in status")
        ok(parse_owa_status("profile: brkh\nauthtoken: expires 2026-09-30T17:30:12Z (1h23m)\n")["brkh"]["auth"].hour == 17,
           "preflight: parses real owa-piggy status shape")
    finally:
        os.environ.pop("TAKT_OWA_PIGGY")
        locked_file.chmod(0o644)

    # 5. catch_up=skip drops a start that is long past its slot; run-once keeps it
    d = tmp / "cu"
    d.mkdir()
    mk = d / "m.txt"
    sk = {"s": mkjob("s", schedule="0 */2 * * *", catch_up="skip", command=cmd_mark(mk, "s", 0)),
          "r": mkjob("r", schedule="0 */2 * * *", command=cmd_mark(mk, "r", 0))}
    late = datetime(2026, 9, 30, 14, 33)
    q = lambda *_: None
    run_job(sk, "s", d / "st", late, scheduled=True, say=q)
    run_job(sk, "r", d / "st", late, scheduled=True, say=q)
    ok("s" not in marks(mk) and "r" in marks(mk), "catch_up: skip drops a 33 minute old start, run-once keeps it")
    run_job(sk, "s", d / "st", datetime(2026, 9, 30, 14, 1), scheduled=True, say=q)
    ok("s" in marks(mk), "catch_up: skip still runs on time")


def check_import(ok, tmp):
    cron = """PATH=/usr/bin:/bin

# sync-ado - mirror ADO work items
47 6 * * * /Users/x/sync-ado.sh >> /tmp/sync-ado.log 2>&1

# yaams-ingest - refresh the feed firehose
# 0 */2 * * * /Users/x/.local/bin/yaams ingest --json >> /l/ingest.log 2>> /l/ingest.err
0 */2 * * * /v/python -c 'from ledger.retrieval import rebuild_note_index; rebuild_note_index()' >> /l/ingest.log 2>> /l/ingest.err; /b/yaams ingest --source tier2_ledger --reindex --json >> /l/ingest.log 2>> /l/ingest.err; /b/yaams ingest --json >> /l/ingest.log 2>> /l/ingest.err

10 */2 * * * /b/yaams ingest --json >> /l/ingest.log 2>> /l/ingest.err # yaams-ingest
@hourly cd /Users/x/feed && ./sync.sh >> /l/sync.log 2>> /l/sync.err
"""
    jobs, skipped, warns = import_cron(cron)
    by = {j["id"]: j for j in jobs}
    ok(len(jobs) == 4 and skipped == [7], "import: 4 live jobs, the commented-out cron line is not a job")
    y = by["yaams-ingest"]
    ok(y["schedule"] == "0 */2 * * *" and len(y["steps"]) == 3, "import: live yaams line keeps its id and its 3 steps")
    ok(y["steps"][0]["command"][:2] == ["/v/python", "-c"] and ";" in y["steps"][0]["command"][2]
       and y["steps"][2]["stdout"] == "/l/ingest.log" and y["steps"][2]["stderr"] == "/l/ingest.err",
       "import: quoted ; survives, redirects become stdout/stderr")
    ok("yaams-ingest-2" in by and by["yaams-ingest-2"]["schedule"] == "10 */2 * * *" and any("duplicate" in w for w in warns),
       "import: duplicate id is renamed and reported")
    ok(any(j["schedule"] == "0 * * * *" and j["steps"][0]["command"][:2] == ["/bin/sh", "-c"] for j in jobs),
       "import: @hourly with && falls back to one sh -c step")


def check_install(ok, tmp):
    d = tmp / "install"
    (d / "agents").mkdir(parents=True)
    old = d / "agents" / "com.example.old.plist"
    old.write_text("old")
    spec = {"new": normalize("new", {"command": ["true"], "schedule": "0 * * * *",
                                      "replaces": ["launchd:com.example.old", "cron:yaams ingest"]})}
    cron = "# keep\n0 * * * * /b/other\n# 0 1 * * * /b/yaams ingest\n0 */2 * * * /b/yaams ingest --json\n"
    ctx = {**CTX, "state": str(d / "st")}
    acts = plan_install(spec, ctx, d / "agents", cron)
    ok(acts[0][0] == "mkdir" and list((d / "agents").iterdir()) == [old] and old.read_text() == "old", "install: planning writes nothing")
    calls = []
    apply_plan(acts, lambda a, i: calls.append((a, i)))
    ok((d / "agents" / f"{PREFIX}.new.plist").exists() and not old.exists() and (d / "st/retired/com.example.old.plist").exists(),
       "install: writes new plist, retires the old one aside (not deleted)")
    ok((d / "st/log").is_dir() and [c[0][:2] for c in calls] == [["launchctl", "bootout"], ["crontab", "-"], ["launchctl", "bootout"], ["launchctl", "enable"], ["launchctl", "bootstrap"]],
       "install: retire old launchd job and cron line before bootstrapping the new one")
    newcron = calls[1][1].decode()
    ok(calls[1][0] == ["crontab", "-"] and "#takt-migrated# 0 */2 * * * /b/yaams ingest --json" in newcron
       and "0 * * * * /b/other\n" in newcron and newcron.count("takt-migrated") == 1, "install: only the matching live cron line is commented out")
    ok((d / "st/crontab.bak").read_text() == cron, "install: crontab backed up first")


def check_admin(ok, tmp):
    ad = Path("/u")
    ok(admin_argv("systemd", "enable", "x", ad) == [["systemctl", "--user", "enable", "--now", "takt-x.timer"]],
       "admin: systemd enable arms the timer now")
    ok(admin_argv("schtasks", "start", "x", ad) == [["schtasks", "/Run", "/TN", "\\takt\\x"]],
       "admin: schtasks start runs the task in its folder")
    en = admin_argv("launchd", "enable", "x", ad)
    ok([c[1] for c in en] == ["enable", "bootstrap"] and en[1][-1] == str(ad / f"{PREFIX}.x.plist"),
       "admin: launchd enable clears the disabled flag before bootstrap")
    # the scheduler's own view, from the real output shapes
    sd = ("Id=takt-a.timer\nLoadState=loaded\nActiveState=active\n\nId=takt-b.timer\nLoadState=loaded\n"
          "ActiveState=inactive\n\nId=takt-c.timer\nLoadState=not-found\nActiveState=inactive\n")
    ok(parse_native("systemd", sd, ["a", "b", "c"], ad) == {"a": (True, True), "b": (True, False), "c": (False, False)},
       "native: systemctl show -> on, off, not installed")
    ok(parse_native("schtasks", "a|Ready\r\nb|Disabled\r\nother|Ready\r\n", ["a", "b", "c"], ad)
       == {"a": (True, True), "b": (True, False)}, "native: Get-ScheduledTask states, foreign tasks ignored")
    (tmp / "la").mkdir()
    (tmp / "la" / f"{PREFIX}.a.plist").write_text("x")
    ok(parse_native("launchd", f"PID\tStatus\tLabel\n-\t0\t{PREFIX}.a\n", ["a", "b"], tmp / "la")
       == {"a": (True, True), "b": (False, False)}, "native: launchctl list plus plist on disk")
    spec = {"a": mkjob("a", schedule="*/5 * * * *"), "b": mkjob("b", schedule="*/5 * * * *")}
    rows = status_rows(spec, tmp / "nost", {"a": (True, True), "b": (True, False)}, now=datetime(2026, 10, 2, 14, 3, 10))
    ok([(r["sched"], r["next"], r["status"]) for r in rows] == [("on", "2026-10-02T14:05", "never"), ("off", None, "never")],
       "status: next slot shown only while the scheduler is armed")
    ok(fmt_rows(rows)[1].split()[:2] == ["local", "a"], "status: host and id are fzf fields 1 and 2")
    # install and uninstall on systemd and schtasks, through an injected runner
    ctx, ud = {**CTX, "state": str(tmp / "ist")}, tmp / "units"
    acts = plan_install(spec, ctx, ud, "", "systemd")
    ok(not ud.exists(), "install systemd: planning writes nothing")
    calls = []
    apply_plan(acts, lambda a, i: calls.append(a))
    ok("Persistent=true" in (ud / "takt-a.timer").read_text() and (ud / "takt-b.service").exists()
       and calls == [["systemctl", "--user", "daemon-reload"]]
       + [c for j in "ab" for c in (["systemctl", "--user", "enable", f"takt-{j}.timer"],
                                    ["systemctl", "--user", "restart", f"takt-{j}.timer"])],
       "install systemd: units written, daemon-reload, then enable and restart (a reinstall takes effect)")
    calls = []
    apply_plan(plan_uninstall(["a"], ud, "systemd", {"a": (True, True)}), lambda a, i: calls.append(a))
    ok(calls == [["systemctl", "--user", "disable", "--now", "takt-a.timer"], ["systemctl", "--user", "stop", "takt-a.service"],
                 ["systemctl", "--user", "daemon-reload"]]
       and not (ud / "takt-a.timer").exists() and (ud / "takt-b.timer").exists(),
       "uninstall systemd: stops the timer and a running service, removes only that job")
    xd, calls = tmp / "xml", []
    apply_plan(plan_install({"a": spec["a"]}, {**ctx, "python": "C:\\Py\\python.exe"}, xd, "", "schtasks"),
               lambda a, i: calls.append(a))
    x = (xd / "takt-a.xml").read_bytes()
    ok(x[:2] == b"\xff\xfe" and "<Command>C:\\Py\\pythonw.exe</Command>" in x.decode("utf-16")
       and calls == [["schtasks", "/Create", "/TN", "\\takt\\a", "/XML", str(xd / "takt-a.xml"), "/F"]],
       "install schtasks: UTF-16 XML, pythonw (no console), registered under \\takt\\")
    # ssh transport: one plain-word command line, valid in sh and pwsh alike
    ok(remote_argv("kwin", ["status", "--json"], {"python": "C:\\Py\\python.exe"})
       == ["ssh", "kwin", "C:\\Py\\python.exe .local/share/takt/takt.py status --json"], "remote: plain-word command line")
    ok(remote_argv("kvps", ["show", "x"], {})[-1].startswith("python3 "), "remote: python3 by default")
    try:
        remote_argv("h", ["show", "x; rm -rf ~"], {})
        ok(False, "remote: refuses a token with shell syntax")
    except ValueError:
        ok(True, "")


def check_review(ok, tmp):
    """Regressions for the first external review (2026-10-03)."""
    q = lambda *_: None
    # log paths: a missing directory is made; an unopenable path is a failed step, not a crash
    d = tmp / "logs"
    d.mkdir()
    (d / "afile").write_text("x")
    mk = d / "m.txt"
    sp = {"l": mkjob("l", step=[
        {"id": "newdir", "command": [PY, "-c", "print('hi')"], "stdout": str(d / "new/sub/out.log")},
        {"id": "bad", "command": [PY, "-c", "pass"], "stdout": str(d / "afile/x.log")},
        {"id": "after", "command": cmd_mark(mk, "after", 0)}])}
    code = run_job(sp, "l", d / "st", say=q)
    r = read_record(d / "st", "l")
    by = {x["id"]: x for x in r["steps"]} if r else {}
    ok((d / "new/sub/out.log").read_text().strip() == "hi", "logs: a missing log directory is created")
    ok(code == 1 and by.get("bad", {}).get("status") == "failed" and by["bad"]["exit"] == 127
       and "afile" in (by["bad"]["note"] or "") and "after" in marks(mk),
       "logs: an unopenable log is a failed step with a note, later steps run, the record is written")

    # preflight runs after the dependencies: A makes the file that B needs
    d = tmp / "prelate"
    d.mkdir()
    made = d / "token"
    sp = {"a": mkjob("a", schedule="0 */2 * * *", command=[PY, "-c", f"open({str(made)!r}, 'w').write('t')"]),
          "b": mkjob("b", schedule="0 */2 * * *", after=["a"], needs=[f"fda:{made}"], command=[PY, "-c", "pass"])}
    run_job(sp, "b", d / "st", datetime(2026, 9, 30, 14, 0, 5), scheduled=True, say=q)
    ok(read_record(d / "st", "b")["status"] == "ok", "preflight: checked after the dependency that satisfies it")

    # a pulled-in dependency keeps its own catch_up = "skip"
    d = tmp / "depskip"
    d.mkdir()
    mk = d / "m.txt"
    sp = {"a": mkjob("a", schedule="0 * * * *", catch_up="skip", command=cmd_mark(mk, "a", 0)),
          "b": mkjob("b", schedule="0 */2 * * *", after=["a"], command=cmd_mark(mk, "b", 0))}
    run_job(sp, "b", d / "st", datetime(2026, 9, 30, 15, 33), scheduled=True, say=q)
    ok("a" not in marks(mk) and "b" in marks(mk), "after: a late dependency with catch_up=skip is not pulled in")

    # sparse schedules: the real slot, months back; never the current minute
    yearly = mkjob("y", schedule="0 0 1 1 *", catch_up="skip", command=[PY, "-c", "pass"])
    ok(slot_of(yearly, datetime(2026, 3, 5, 10, 7)) == datetime(2026, 1, 1)
       and next_slot(parse_cron("0 0 1 1 *"), datetime(2026, 3, 5)) == datetime(2027, 1, 1),
       "slots: a yearly job finds its slot 2 months back and its next one 10 months ahead")
    ok(slot_of(mkjob("f", schedule="0 12 29 2 *"), datetime(2026, 3, 1)) == datetime(2024, 2, 29, 12),
       "slots: 29 February is found 2 years back")
    ok(run_job({"y": yearly}, "y", tmp / "yst", datetime(2026, 3, 5, 10, 7), scheduled=True, say=q) == 0
       and read_record(tmp / "yst", "y") is None, "slots: catch_up=skip drops a yearly job woken months late")
    try:
        mkjob("never", schedule="0 0 30 2 *")
        ok(False, "spec: a schedule with no date (30 February) is rejected")
    except ValueError:
        ok(True, "")

    # systemd weekday steps render from the expanded field
    ok("OnCalendar=Sun,Tue,Thu,Sat *-*-* 00:00:00" in render_systemd(mkjob("w", schedule="0 0 * * */2"), CTX)[1],
       "systemd: */2 in the weekday field renders as day names")

    # cron import keeps shell meaning
    jobs, _, _ = import_cron('0 1 * * * echo "$HOME" > /tmp/out\n0 2 * * * ~/bin/x\n0 3 * * * /bin/a >> /tmp/log\n'
                             '0 4 * * * /bin/a > /tmp/out\n')
    cmds = [j["steps"][0]["command"] for j in jobs]
    ok(cmds[0] == ["/bin/sh", "-c", 'echo "$HOME" > /tmp/out'] and cmds[1][:2] == ["/bin/sh", "-c"]
       and cmds[2] == ["/bin/a"] and jobs[2]["steps"][0]["stdout"] == "/tmp/log"
       and cmds[3] == ["/bin/sh", "-c", "/bin/a > /tmp/out"],
       "import: $, ~ and a truncating > stay in sh -c; plain argv with >> is still split")

    # launchd reinstall: unload the old copy (may fail), clear a disable, then bootstrap
    acts = plan_install({"n": mkjob("n", schedule="0 * * * *")}, {**CTX, "state": str(tmp / "ri")}, tmp / "ri-agents", "")
    runs = [(k, a[1]) for k, a, _ in acts if k in ("run", "try")]
    ok(runs == [("try", "bootout"), ("run", "enable"), ("run", "bootstrap")], "install launchd: reinstall boots out the loaded copy")
    calls = []

    def flaky(a, i):
        calls.append(a[1])
        if a[1] == "bootout":
            raise subprocess.CalledProcessError(5, a)
    apply_plan(acts, flaky)
    ok(calls == ["bootout", "enable", "bootstrap"], "install launchd: a failed bootout (not loaded) does not stop the install")

    # crontab is read only when a job replaces a cron line
    def no_crontab(*a, **k):
        raise FileNotFoundError("crontab")
    ok(read_crontab({"x": mkjob("x")}, no_crontab) == "", "install: no crontab needed without cron replaces")
    try:
        read_crontab({"x": mkjob("x", replaces=["cron:foo"])}, no_crontab)
        ok(False, "install: cron replaces without crontab is a clear error")
    except SystemExit as e:
        ok("crontab is not installed" in str(e), "install: cron replaces without crontab is a clear error")

    # ssh: a host is never an option
    for bad in ("-oProxyCommand=x", "-l"):
        try:
            remote_argv(bad, ["status"], {})
            ok(False, f"remote: refuses host {bad}")
        except ValueError:
            ok(True, "")

    # Windows: a killed wrapper takes its step with it (job object), so no step runs unlocked
    if os.name == "nt":
        d = tmp / "winjob"
        d.mkdir()
        mk = d / "m.txt"
        spec = write_spec(d, mkjob("a", lock=["res"], command=cmd_mark(mk, "a", 2.5)))
        pa = spawn(spec, d / "st", "a")
        time.sleep(1.0)
        pa.kill()
        pa.wait()
        pa.stdout.close()
        time.sleep(2.5)
        ev = marks(mk)
        ok("start" in ev.get("a", {}) and "end" not in ev["a"], "lock (windows): killing the wrapper ends its step")

    # a killed wrapper does not free a lock its running step still uses (POSIX: inherited fds)
    if fcntl:
        d = tmp / "orphan"
        d.mkdir()
        mk = d / "m.txt"
        spec = write_spec(d, mkjob("a", lock=["res"], command=cmd_mark(mk, "a", 1.5)),
                          mkjob("b", lock=["res"], command=cmd_mark(mk, "b", 0)))
        pa = spawn(spec, d / "st", "a")
        time.sleep(0.5)
        pa.kill()  # the wrapper only; its step keeps running
        pa.wait()  # not communicate(): the orphaned step still holds the stdout pipe
        pa.stdout.close()
        spawn(spec, d / "st", "b").communicate()
        ev = marks(mk)
        ok("end" in ev.get("a", {}) and ev["b"]["start"] >= ev["a"]["end"],
           "lock: a killed wrapper's still-running step keeps the lock")


def check_review2(ok, tmp):
    """Regressions for the second external review (2026-10-03)."""
    q = lambda *_: None
    # a relative state dir is made absolute, so the scheduler and a shell share the locks
    old = os.environ.get("TAKT_STATE")
    os.environ["TAKT_STATE"] = "rel/state"
    try:
        ok(state_dir(argparse.Namespace(state="rel")).is_absolute() and state_dir().is_absolute()
           and state_dir().parts[-2:] == ("rel", "state"), "state: relative --state and TAKT_STATE become absolute")
    finally:
        os.environ.pop("TAKT_STATE") if old is None else os.environ.__setitem__("TAKT_STATE", old)

    # a job that was pulled in for its slot exits at once, and a lock-timeout never overwrites that record
    d = tmp / "dedup"
    d.mkdir()
    a = mkjob("a", schedule="0 * * * *", lock_timeout=1, command=[PY, "-c", "pass"])
    now, slot = datetime(2026, 9, 30, 14, 0, 30), datetime(2026, 9, 30, 14, 0)
    pulled = {"id": "a", "slot": slot.isoformat(), "pulled_by": "b", "status": "ok", "steps": []}
    holder = Locks(d / "st", ["job-a"], 5).__enter__()
    try:
        write_record(d / "st", pulled)
        t0 = time.time()
        code = run_job({"a": a}, "a", d / "st", now, scheduled=True, say=q)
        ok(code == 0 and time.time() - t0 < 0.5 and read_record(d / "st", "a")["pulled_by"] == "b",
           "dedup: a job pulled in for this slot exits before it waits for the lock")
        (d / "st" / "a.json").unlink()
        import threading
        threading.Timer(0.3, write_record, (d / "st", pulled)).start()
        code = run_job({"a": a}, "a", d / "st", now, scheduled=True, say=q)
        ok(code == 0 and read_record(d / "st", "a").get("pulled_by") == "b",
           "dedup: a lock-timeout keeps the record of a pull-in that happened during the wait")
    finally:
        holder.__exit__()

    # preflight runs after the lock: the holder makes the file that the waiter needs
    d = tmp / "prelock"
    d.mkdir()
    made = d / "made"
    spec = write_spec(d, mkjob("b", lock=["res"], command=[PY, "-c", f"import time; open({str(made)!r}, 'w'); time.sleep(0.6)"]),
                      mkjob("a", lock=["res"], needs=[f"fda:{made}"], command=[PY, "-c", "pass"]))
    pb = spawn(spec, d / "st", "b")
    time.sleep(0.25)
    spawn(spec, d / "st", "a").communicate()
    pb.communicate()
    ok(read_record(d / "st", "a")["status"] == "ok", "preflight: runs after the lock, so the holder can satisfy it")

    # cron import: 2 as an argument, NAME=value prefixes, %, environment lines
    jobs, _, warns = import_cron("MAILTO=me\nPATH=/x:/y\n0 1 * * * echo 2 >> /tmp/log\n0 2 * * * FOO=1 /bin/a\n"
                                 "0 3 * * * /bin/mail me%hello\n0 4 * * * /bin/date +\\%F\n")
    cmds = {j["schedule"]: j["steps"][0]["command"] for j in jobs}
    ok(cmds.get("0 1 * * *") == ["/bin/sh", "-c", "echo 2 >> /tmp/log"]
       and cmds.get("0 2 * * *") == ["/bin/sh", "-c", "FOO=1 /bin/a"], "import: `echo 2 >>` and NAME=value stay in sh -c")
    ok("0 3 * * *" not in cmds and any("% is cron" in w for w in warns) and cmds.get("0 4 * * *") == ["/bin/date", "+%F"],
       "import: an unescaped % is refused with a warning; \\% becomes %")
    ok(sum("crontab environment" in w for w in warns) == 2, "import: environment lines are reported, not dropped silently")

    # registered jobs: found on disk or in the task list, including ones the spec no longer names
    la = tmp / "r2-la"
    la.mkdir()
    for n in ("old", "new"):
        (la / f"{PREFIX}.{n}.plist").write_text("x")
    ok(installed_ids("launchd", la) == ["new", "old"] and installed_ids("schtasks", la, "probe|Ready\r\nx|Disabled\r\n")
       == ["probe", "x"], "installed: takt jobs found from plists and from the task list")
    spec = {"new": mkjob("new", schedule="0 * * * *")}
    nat = {"old": (True, True), "new": (True, False)}
    acts = plan_admin("install", spec, "launchd", la, ["new", "old"], nat, ctx={**CTX, "state": str(tmp / "r2st")})
    steps = [(k, a[1] if k in ("run", "try") else Path(a).name) for k, a, _ in acts]
    ok(steps[:2] == [("run", "bootout"), ("rm", f"{PREFIX}.old.plist")] and ("rm", f"{PREFIX}.new.plist") not in steps,
       "install: retires a takt job the spec no longer names, before installing")
    un = plan_admin("uninstall", spec, "launchd", la, ["new", "old"], nat)
    ok([(k, a[1] if k == "run" else Path(a).name) for k, a, _ in un]
       == [("rm", f"{PREFIX}.new.plist"), ("run", "bootout"), ("rm", f"{PREFIX}.old.plist")],
       "uninstall: covers jobs gone from the spec; stops only what the scheduler has loaded")

    def refuse(a, i):
        if a[1] == "bootout":
            raise subprocess.CalledProcessError(5, a)
    try:
        apply_plan(plan_uninstall(["old"], la, "launchd", nat), refuse)
        ok(False, "uninstall: a scheduler refusal stops before the files are deleted")
    except subprocess.CalledProcessError:
        ok((la / f"{PREFIX}.old.plist").exists(), "uninstall: a scheduler refusal stops before the files are deleted")

    # --host: every argument is checked before push writes anything to the host
    g, pushed = globals(), []
    saved = g["CONFIG"], g["push"]
    g["CONFIG"], g["push"] = tmp, (lambda h: pushed.append(h) or 0)
    (tmp / "jobs.h.toml").write_text('[settings]\npython = "python3"\n')
    try:
        g["on_host"]("h", ["install", "a b", "--allow-writes"])
        ok(False, "remote: a bad argument is refused before the push")
    except ValueError:
        ok(pushed == [], "remote: a bad argument is refused before the push")
    finally:
        g["CONFIG"], g["push"] = saved


def check_review3(ok, tmp):
    """Regressions for the third external review (2026-10-03), and checks the second one lacked."""
    import contextlib
    import io
    # a failed scheduler query stops install/uninstall; status still tolerates it
    def failed(*a, **k):
        return subprocess.CompletedProcess(a[0], 1, "", "no user bus")

    def broken(*a, **k):
        raise FileNotFoundError("systemctl")
    for be, run in (("systemd", failed), ("schtasks", failed), ("launchd", broken)):
        try:
            inventory({"x": mkjob("x")}, be, tmp, run)
            ok(False, f"inventory: a failed {be} query stops the plan")
        except RuntimeError:
            ok(True, "")
    ok(native_text("schtasks", [], run=broken) == "", "status: a failed scheduler query still shows the table")

    # uninstall and retirement stop running work: the systemd service, the running Windows task
    nat = {"old": (True, True)}
    un_sd = plan_admin("uninstall", {}, "systemd", tmp / "r3u", ["old"], nat)
    ok([a[2:4] for k, a, _ in un_sd if k == "run"][:2] == [["disable", "--now"], ["stop", "takt-old.service"]],
       "uninstall systemd: stops the running service as well as the timer")
    re_sd = plan_admin("install", {"new": mkjob("new", schedule="0 * * * *")}, "systemd", tmp / "r3u", ["old"], nat,
                       ctx={**CTX, "state": str(tmp / "r3st")})
    ok([a[2:4] for k, a, _ in re_sd if k == "run"][:2] == [["disable", "--now"], ["stop", "takt-old.service"]]
       and any(k == "write" and Path(a).name == "takt-new.timer" for k, a, _ in re_sd),
       "install systemd: retires a job the spec no longer names, then installs the spec")
    re_win = plan_admin("install", {"new": mkjob("new", schedule="0 * * * *")}, "schtasks", tmp / "r3x", ["old"], nat,
                        ctx={**CTX, "state": str(tmp / "r3st")})
    runs = [a[:3] for k, a, _ in re_win if k == "run"]
    ok(runs[:2] == [["schtasks", "/End", "/TN"], ["schtasks", "/Delete", "/TN"]] and runs[2][:2] == ["schtasks", "/Create"],
       "install schtasks: ends a running instance, deletes the old task, then creates the new one")

    # the production runner checks exit codes
    try:
        run_checked([PY, "-c", "import sys; sys.exit(3)"], None)
        ok(False, "runner: a failed scheduler command raises")
    except subprocess.CalledProcessError:
        ok(True, "")

    # one temp file per writer: a stale fixed-name temp entry cannot block a write
    st = tmp / "r3tmp"
    (st / ".a.json.tmp").mkdir(parents=True)
    write_record(st, {"id": "a", "status": "ok"})
    ok(read_record(st, "a")["status"] == "ok", "record: written through a temp file of its own")

    # cron import: a quoted 2 before >>, and a quoted operator
    jobs, _, _ = import_cron("0 1 * * * echo \"2\">> /tmp/log\n0 2 * * * echo \";\"\n0 3 * * * echo '2' >> /tmp/log\n")
    ok([j["steps"][0]["command"][:2] for j in jobs] == [["/bin/sh", "-c"]] * 3,
       "import: a quoted 2 before >> and a quoted ; stay in sh -c")

    # --host install: a typo in the command fails locally, before push
    g, pushed = globals(), []
    saved = g["CONFIG"], g["push"]
    g["CONFIG"], g["push"] = tmp, (lambda h: pushed.append(h) or 0)
    (tmp / "jobs.h3.toml").write_text('[settings]\npython = "python3"\n')
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            code = g["on_host"]("h3", ["install", "--bogus", "--allow-writes"])
        ok(code == 2 and pushed == [], "remote: an unknown option fails before the push")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = g["on_host"]("h3", ["push", "--allow-writes"])
        ok(code == 0 and pushed == ["h3"], "remote: a valid push passes the local parse and pushes")
        # dry-run install plans from the host's old spec: the note must say what will change
        (tmp / "jobs.h3.toml").write_text('[settings]\npython = "python3"\n[job.web]\ncommand = ["true"]\n')
        import types
        rows, run = g["remote_rows"], subprocess.run
        g["remote_rows"] = lambda h, cfg=None: [{"host": h, "id": "probe"}]
        subprocess.run = lambda *a, **k: types.SimpleNamespace(returncode=0)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            g["on_host"]("h3", ["install"])
        ok("NOTE" in out.getvalue() and "adds web" in out.getvalue() and "drops probe" in out.getvalue() and pushed == ["h3"],
           "remote: a dry-run install names the jobs the pushed spec adds and drops")
        g["remote_rows"] = lambda h, cfg=None: [{"host": h, "id": "web"}]
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            g["on_host"]("h3", ["install"])
        ok("NOTE" not in out.getvalue(), "remote: no note when the host already has the same jobs")
        # replaces: a plist that an earlier install already retired must not show up as a move
        la2, rspec = tmp / "hp-la", {"n": mkjob("n", schedule="0 * * * *", replaces=["launchd:com.example.gone"])}
        la2.mkdir(exist_ok=True)
        rctx = {**CTX, "state": str(tmp / "hp-st")}
        acts = plan_install(rspec, rctx, la2, "", "launchd", set())
        ok(not any(a[0] == "move" for a in acts), "install launchd: an already-retired replaces plist plans no move")
        (la2 / "com.example.gone.plist").write_text("x")
        acts = plan_install(rspec, rctx, la2, "", "launchd", set())
        ok(any(a[0] == "move" for a in acts), "install launchd: an existing replaces plist is still moved")
    finally:
        g["CONFIG"], g["push"] = saved
        if "rows" in locals():
            g["remote_rows"], subprocess.run = rows, run


def check_review4(ok, tmp):
    """Regressions for the fourth external review (2026-10-03)."""
    import contextlib
    import io
    g = globals()
    # launchd install: a loaded job must boot out first, and a refusal stops before any write
    la, ctx = tmp / "r4-la", {**CTX, "state": str(tmp / "r4-st")}
    spec = {"n": mkjob("n", schedule="0 * * * *", replaces=["launchd:com.example.old"])}
    loaded_all = {f"{PREFIX}.n", "com.example.old"}
    acts = plan_install(spec, ctx, la, "", "launchd", loaded_all)
    boots = [(k, a[-1].rsplit("/", 1)[-1]) for k, a, _ in acts if k in ("run", "try") and a[1] == "bootout"]
    ok(boots == [("run", "com.example.old"), ("run", f"{PREFIX}.n")], "install launchd: a loaded job's bootout must succeed")
    acts0 = plan_install(spec, ctx, la, "", "launchd", set())
    ok(not [a for k, a, _ in acts0 if k in ("run", "try") and a[1] == "bootout"],
       "install launchd: a job that is not loaded is not booted out")

    def refuse(a, i):
        if a[1] == "bootout":
            raise subprocess.CalledProcessError(5, a)
    try:
        apply_plan(acts, refuse)
        ok(False, "install launchd: a refused bootout stops before the plist is moved or written")
    except subprocess.CalledProcessError:
        ok(not (la / f"{PREFIX}.n.plist").exists() and not (tmp / "r4-st/retired").exists(),
           "install launchd: a refused bootout stops before the plist is moved or written")

    # inventory: a loaded takt label with no plist, and an id named on the command line
    def listing(*a, **k):
        return subprocess.CompletedProcess(a[0], 0, f"PID\tStatus\tLabel\n-\t0\t{PREFIX}.ghost\n-\t0\tcom.other\n", "")
    known, nat, loaded = inventory({}, "launchd", tmp / "r4-empty", listing, extra=["named"])
    ok(known == ["ghost"] and nat["ghost"] == (False, True) and "named" in nat and f"{PREFIX}.ghost" in loaded,
       "inventory: finds a loaded takt job with no plist, and looks up named ids")
    un = plan_admin("uninstall", {}, "launchd", tmp / "r4-empty", known, nat, ids=["ghost"])
    ok([a[1] for k, a, _ in un if k == "run"] == ["bootout"], "uninstall: boots out a loaded job whose plist is gone")

    # Windows containment: without the job object, the job does not run, and the record says why
    d = tmp / "r4-contain"
    d.mkdir()
    mk = d / "m.txt"
    specf = write_spec(d, mkjob("c", command=cmd_mark(mk, "c", 0)))
    saved = g["CONTAIN"], g["end_steps_with_wrapper"]
    g["CONTAIN"], g["end_steps_with_wrapper"] = True, (lambda: "Windows error 5")
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            code = main(["run", "c", "--spec", str(specf), "--state", str(d / "st")])
    finally:
        g["CONTAIN"], g["end_steps_with_wrapper"] = saved
    r = read_record(d / "st", "c")
    ok(code == 2 and "c" not in marks(mk) and r["status"] == "skipped" and "job object" in r["note"],
       "windows: no job object means no run, and the record says why")

    # leftovers are ended while the locks are still held
    held = []

    def probe(locks):
        f = open(locks.dir / "r4.lock", "a+")
        try:
            _trylock(f)
            held.append(False)
        except OSError:
            held.append(True)
        finally:
            f.close()
    g["BEFORE_UNLOCK"].append(probe)
    try:
        Locks(tmp / "r4-locks", ["r4"], 5).__enter__().__exit__()
    finally:
        g["BEFORE_UNLOCK"].remove(probe)
    ok(held == [True], "lock: leftovers are ended before the locks are released")

    # cron import: backslash escapes stay in sh -c
    jobs, _, _ = import_cron("0 1 * * * echo \\;\n0 2 * * * echo \\2>> /tmp/f\n")
    ok([j["steps"][0]["command"][:2] for j in jobs] == [["/bin/sh", "-c"]] * 2, "import: backslash escapes stay in sh -c")


def check_review5(ok, tmp):
    """Regressions for the fifth external review (2026-10-03)."""
    # a manual-only job: systemd gets the service, no timer, and nothing enables a timer
    manual = mkjob("m", command=[PY, "-c", "pass"])
    ok(render_systemd(manual, CTX)[1] is None and render_systemd(mkjob("r", run_at_load=True), CTX)[1] is not None,
       "systemd: a job with no schedule and no run_at_load gets no timer")
    ud = tmp / "r5-units"
    acts = plan_install({"m": manual}, {**CTX, "state": str(tmp / "r5st")}, ud, "", "systemd")
    ok([Path(a).name for k, a, _ in acts if k == "write"] == ["takt-m.service"]
       and not [a for k, a, _ in acts if k == "run" and "takt-m.timer" in a],
       "install systemd: a manual-only job writes the service and enables no timer")
    apply_plan(acts, lambda a, i: None)
    ok(installed_ids("systemd", ud) == ["m"], "installed: a manual-only systemd job is found from its service")
    stops = [a[2:] for k, a, _ in plan_uninstall(["m"], ud, "systemd", {}) if k == "run"]
    ok(["stop", "takt-m.service"] in stops and not [x for x in stops if "takt-m.timer" in x],
       "uninstall systemd: a manual-only job's service is stopped, and no timer is touched")

    # one bootout per label: a rename that also `replaces` the old label, and two jobs replacing one label
    nat = {"old": (True, True)}
    spec = {"new": mkjob("new", schedule="0 * * * *", replaces=[f"launchd:{PREFIX}.old"])}
    acts = plan_admin("install", spec, "launchd", tmp / "r5-la", ["old"], nat, ctx={**CTX, "state": str(tmp / "r5st")},
                      loaded={f"{PREFIX}.old"})
    ok([a[-1] for k, a, _ in acts if k in ("run", "try") and a[1] == "bootout"] == [f"gui/{_uid()}/{PREFIX}.old"],
       "install launchd: a label retired as stale is not booted out again by `replaces`")
    two = {x: mkjob(x, schedule="0 * * * *", replaces=["launchd:com.example.shared"]) for x in ("p", "q")}
    acts = plan_install(two, {**CTX, "state": str(tmp / "r5st")}, tmp / "r5-la", "", "launchd", {"com.example.shared"})
    ok(sum(1 for k, a, _ in acts if k in ("run", "try") and a[-1].endswith("com.example.shared")) == 1,
       "install launchd: two jobs replacing one label boot it out once")

    # cron import: `#` inside a word and shell builtins stay in sh -c; the name comment is dropped
    jobs, _, _ = import_cron("0 1 * * * /bin/echo abc#def\n0 2 * * * cd /tmp; /bin/pwd\n# nightly\n0 3 * * * /bin/a # nightly\n")
    cmds = {j["schedule"]: j["steps"][0]["command"] for j in jobs}
    ok(cmds["0 1 * * *"] == ["/bin/sh", "-c", "/bin/echo abc#def"] and cmds["0 2 * * *"] == ["/bin/sh", "-c", "cd /tmp; /bin/pwd"]
       and cmds["0 3 * * *"] == ["/bin/a"], "import: `#` in a word and `cd` stay in sh -c; the name comment is not a command")


def check_review6(ok, tmp):
    """Regressions for the sixth external review (2026-10-04)."""
    import contextlib
    import io
    # removing a schedule retires the systemd timer that the old install left armed
    ud = tmp / "r6-units"
    ud.mkdir()
    (ud / "takt-m.timer").write_text("[Timer]\n")
    acts = plan_install({"m": mkjob("m", command=[PY, "-c", "pass"])}, {**CTX, "state": str(tmp / "r6st")}, ud, "", "systemd")
    steps = [(k, a[2:4] if k == "run" else Path(a).name) for k, a, _ in acts if k in ("run", "rm")]
    ok(steps[:2] == [("run", ["disable", "--now"]), ("rm", "takt-m.timer")] and steps[2] == ("run", ["daemon-reload"]),
       "install systemd: removing a schedule disables and removes the old timer before the reload")

    # `takt start` is a manual run even though the scheduler starts it with --scheduled
    d = tmp / "r6-start"
    d.mkdir()
    mk = d / "m.txt"
    specf = write_spec(d, mkjob("d", schedule="0 0 * * *", catch_up="skip", command=cmd_mark(mk, "d", 0)))
    st = d / "st"
    noon = ["run", "d", "--spec", str(specf), "--state", str(st), "--scheduled", "--now", "2026-10-03T12:00:00"]
    q = contextlib.redirect_stdout(io.StringIO())
    with q:
        main(noon)
    ok("d" not in marks(mk), "start: a scheduled run 12 hours late with catch_up=skip is skipped")
    request_start(st, "d")
    with contextlib.redirect_stdout(io.StringIO()):
        main(noon)
    r = read_record(st, "d")
    ok("d" in marks(mk) and r["trigger"] == "start" and not (st / "start" / "d").exists(),
       "start: the run that `takt start` asked for runs, records trigger start, and consumes the request")
    (st / "start" / "d").write_text(repr(time.time() - START_TTL - 5))
    ok(not take_start(st, "d") and not (st / "start" / "d").exists(), "start: an expired request is dropped")

    # the start verb leaves the request, and removes it again when the scheduler refuses
    g, calls = globals(), []
    saved = g["run_admin"]
    try:
        g["run_admin"] = lambda cmds: calls.append(cmds) or None
        with contextlib.redirect_stdout(io.StringIO()):
            code = main(["start", "d", "--spec", str(specf), "--state", str(st), "--allow-writes"])
        ok(code == 0 and calls and (st / "start" / "d").exists(), "start: the verb leaves a start request for the run")
        g["run_admin"] = lambda cmds: "refused"
        with contextlib.redirect_stdout(io.StringIO()):
            code = main(["start", "d", "--spec", str(specf), "--state", str(st), "--allow-writes"])
        ok(code == 1 and not (st / "start" / "d").exists(), "start: a refused start leaves no request behind")
    finally:
        g["run_admin"] = saved


def check_review7(ok, tmp):
    """Regressions for the seventh external review (2026-10-04)."""
    import contextlib
    import io
    # Task Scheduler: at most 48 triggers, checked while planning
    ok(len(ET.fromstring(render_schtasks(mkjob("t", schedule="0,20,40 1-16 * * 1-5"), CTX)).find(f"{{{TS_NS}}}Triggers")) == 48,
       "schtasks: 3 minutes in 16 hours is 48 triggers")
    for sched, run_at_load, fits in (("0,20,40 1-16 * * 1-5", False, True), ("0,20,40 1-16 * * 1-5", True, False),
                                     ("*/5 9-17 * * 1-5", False, False)):
        try:
            render_schtasks(mkjob("t", schedule=sched, run_at_load=run_at_load), CTX)
            ok(fits, f"schtasks: {sched} (run_at_load={run_at_load}) is refused above 48 triggers")
        except ValueError as e:
            ok(not fits and "at most 48" in str(e), f"schtasks: {sched} (run_at_load={run_at_load}) fits in 48 triggers")
    try:
        plan_install({"t": mkjob("t", schedule="*/5 9-17 * * 1-5")}, {**CTX, "state": str(tmp / "r7st")}, tmp / "r7x", "", "schtasks")
        ok(False, "install schtasks: too many triggers fails while planning")
    except ValueError:
        ok(not (tmp / "r7x").exists(), "install schtasks: too many triggers fails while planning, before any write")

    # report JSON in another shape: a note on the step, and the next steps still run
    d = tmp / "r7-report"
    d.mkdir()
    mk = d / "m.txt"
    sp = {"j": mkjob("j", step=[
        {"id": "r", "command": [PY, "-c", "print('{\"error\": \"connection refused\"}')"], "report": "json-failed-sources"},
        {"id": "after", "command": cmd_mark(mk, "after", 0)}])}
    run_job(sp, "j", d / "st", say=lambda *_: None)
    r = read_record(d / "st", "j")
    ok(r and r["steps"][0]["note"] == "report: connection refused" and "after" in marks(mk),
       "report: an error that is not an object becomes a note, and later steps run")
    ok(yaams_failed('[1, 2]') == ([], None) and yaams_failed('{"error": {"failed_sources": "imessage"}}') == (["imessage"], None),
       "report: other JSON shapes do not crash")

    # `takt start` on a running job: no request, and no native start
    d = tmp / "r7-start"
    d.mkdir()
    specf = write_spec(d, mkjob("d", schedule="0 0 * * *", command=[PY, "-c", "pass"]))
    g, calls = globals(), []
    saved = g["run_admin"]
    holder = Locks(d / "st", ["d"], 5, sub="running").__enter__()
    try:
        g["run_admin"] = lambda cmds: calls.append(cmds) or None
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            main(["start", "d", "--spec", str(specf), "--state", str(d / "st"), "--allow-writes"])
        ok(not calls and not (d / "st" / "start" / "d").exists() and "already running" in out.getvalue(),
           "start: a running job is not started again and leaves no request")
    finally:
        holder.__exit__()
        g["run_admin"] = saved


def check_review8(ok, tmp):
    """Regressions for the eighth external review (2026-10-04)."""
    import contextlib
    import io
    # a step note shows in status and in show
    st = tmp / "r8-notes"
    write_record(st, {"id": "n", "slot": "2026-10-04T00:00:00", "trigger": "manual", "pulled_by": None,
                      "started": "2026-10-04T00:00:01", "duration_s": 0.1, "status": "ok", "exit": 0,
                      "steps": [{"id": "r", "status": "ok", "exit": 0, "duration_s": 0.1, "failed": [],
                                 "note": "report: connection refused"}], "preflight": []})
    spec = {"n": mkjob("n", command=[PY, "-c", "pass"])}
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        show(spec, st, "n")
    ok("connection refused" in status_rows(spec, st, {})[0]["detail"] and "connection refused" in out.getvalue(),
       "status and show: a step note is visible")

    # a wrapper waiting for a shared lock already counts as running
    d = tmp / "r8-wait"
    d.mkdir()
    specf = write_spec(d, mkjob("j", lock=["res"], command=[PY, "-c", "pass"]))
    holder = Locks(d / "st", ["res"], 5).__enter__()
    try:
        pj = spawn(specf, d / "st", "j")
        deadline = time.time() + 5
        while not job_running(d / "st", "j") and time.time() < deadline:
            time.sleep(0.05)
        ok(job_running(d / "st", "j") and pj.poll() is None, "start: a run waiting for a shared lock counts as running")
    finally:
        holder.__exit__()
    pj.communicate()
    ok(not job_running(d / "st", "j") and read_record(d / "st", "j")["status"] == "ok",
       "start: the marker is released when the run ends")


def check_review9(ok, tmp):
    """Regressions for the ninth external review (2026-10-04)."""
    # the running marker has its own directory: a lock named like it does not wait on itself
    d = tmp / "r9"
    d.mkdir()
    sp = {"j": mkjob("j", lock=["run-j", "j"], lock_timeout=1, command=[PY, "-c", "pass"])}
    ok(run_job(sp, "j", d / "st", say=lambda *_: None) == 0 and read_record(d / "st", "j")["status"] == "ok",
       "lock: a lock named like the job's running marker does not block the job")
    for bad in ("../x", "a/b", "", 3):
        try:
            mkjob("x", lock=[bad])
            ok(False, f"spec: lock name {bad!r} is rejected")
        except ValueError:
            ok(True, "")


def check_review10(ok, tmp):
    """Regressions for the tenth external review (2026-10-04)."""
    d = tmp / "r10"
    d.mkdir()
    sp = {"j": mkjob("j", lock=["JOB-j", "Res", "res"], lock_timeout=1, command=[PY, "-c", "pass"])}
    ok(run_job(sp, "j", d / "st", say=lambda *_: None) == 0 and read_record(d / "st", "j")["status"] == "ok",
       "lock: names that differ only by case are one lock, not a wait on itself")
    for bad_id, bad_lock in (("NUL", None), ("com1", None), (None, "Con"), (None, "lpt9")):
        try:
            mkjob(bad_id or "x", lock=[bad_lock] if bad_lock else [])
            ok(False, f"spec: Windows device name {bad_id or bad_lock!r} is rejected")
        except ValueError:
            ok(True, "")
    ok(file_name_ok("console") and file_name_ok("com10"), "spec: names that only start like a device name are fine")
    f = d / "jobs.toml"
    f.write_text('[job.A]\ncommand = ["true"]\n[job.a]\ncommand = ["true"]\n')
    try:
        load_spec(f)
        ok(False, "spec: job ids that differ only by case are rejected")
    except ValueError:
        ok(True, "")


def check_owa_reseed(ok, tmp):
    """owa:<profile> reads reseed health: a valid token whose reseed needs a sign-in is not healthy."""
    data = {"profiles": [
        {"profile": "good", "state": "ok", "refresh_token": {"minutes_remaining": 1400},
         "reseed": {"state": "ok", "fails": 0, "max_fails": 3}},
        {"profile": "tired", "state": "ok", "refresh_token": {"minutes_remaining": 1380},
         "reseed": {"state": "needs_signin", "fails": 2, "max_fails": 3}},
        {"profile": "gone", "state": "ok", "refresh_token": {"minutes_remaining": 1380},
         "reseed": {"state": "backed_off", "fails": 3, "max_fails": 3}},
        {"profile": "dead", "state": "no valid token", "refresh_token": {}, "reseed": {"state": "ok"}},
        {"profile": "google", "state": "ok", "refresh_token": {"minutes_remaining": None}, "reseed": {"state": "unknown"}}]}
    cache = {"owa": {"json": parse_owa_json(json.dumps(data))}}
    r = {n: check_need(f"owa:{n}", cache) for n in ("good", "tired", "gone", "dead", "google", "ghost")}
    ok(r["good"][0] and "1400 min" in r["good"][1], "owa: a healthy profile passes with its token time")
    ok(not r["tired"][0] and "reseed needs signin (2/3)" in r["tired"][1] and not r["gone"][0] and "backed off (3/3)" in r["gone"][1],
       "owa: a valid token whose reseed needs a sign-in or backed off fails (the dno case)")
    ok(not r["dead"][0] and r["google"][0] and "reseed unknown" in r["google"][1] and not r["ghost"][0],
       "owa: no token fails, unknown reseed passes with a note, an unknown profile fails")
    ok(parse_owa_json("profile: x\nauthtoken: expires 2026-01-01T00:00:00Z\n") is None,
       "owa: text status is not taken for JSON (older owa-piggy falls back)")
    if os.name != "nt":  # the fake owa-piggy is a sh script that answers --json
        fake = tmp / "owa-piggy-json"
        fake.write_text("#!/bin/sh\n[ \"$2\" = --json ] && cat <<'E'\n" + json.dumps(data) + "\nE\n")
        fake.chmod(0o755)
        os.environ["TAKT_OWA_PIGGY"] = str(fake)
        try:
            st = owa_status()
        finally:
            os.environ.pop("TAKT_OWA_PIGGY")
        ok(st and set(st["json"]) == {"good", "tired", "gone", "dead", "google"}, "owa: status --json is read from owa-piggy")


def check_status_scope(ok, tmp):
    """settings.status: master (default), all, client, none."""
    import contextlib
    import io
    master, other = {"status": "master", "master": "kmbp", "self": "kmbp"}, {"status": "master", "master": "kmbp", "self": "kvps"}
    table = {
        ("master", "kmbp", True): "net", ("master", "kmbp", False): "local", ("master", "kvps", True): "off",
        ("all", "kvps", True): "net", ("all", "kvps", False): "local",
        ("client", "kmbp", True): "local", ("client", "kvps", True): "local",
        ("none", "kmbp", True): "off", ("none", "kvps", False): "off"}
    got = {(m, me, a): status_scope({"status": m, "master": "kmbp", "self": me}, a)[0] for (m, me, a) in table}
    ok(got == table, f"status: the scope table for master/all/client/none ({ {k: v for k, v in got.items() if table[k] != v} })")
    ok(status_scope(other, True, here=True)[0] == "local" and "kmbp" in status_scope(other, True)[1],
       "status: a non-master points to the master, and --here shows this device anyway")
    ok(status_scope(master, False) == ("local", None), "status: the master's plain status is its own jobs")
    ok(build_parser().parse_args(STATUS_PULL).here, "status: a peer is pulled with --here, so its mode cannot hide it")

    g = globals()
    saved = g["CONFIG"]
    try:
        cdir = tmp / "net-controller"
        cdir.mkdir()
        g["CONFIG"] = cdir
        ok(net_settings()["status"] == "master" and net_settings()["master"] == net_settings()["self"],
           "net: with no settings a device is its own master")
        (cdir / "jobs.toml").write_text('[settings]\nstatus = "all"\nname = "kmbp"\npython = "/opt/homebrew/bin/python3"\n')
        (cdir / "jobs.kvps.toml").write_text('[settings]\npython = "/usr/bin/python3"\n')
        (cdir / "jobs.kwin.toml").write_text("[settings]\npython = 'C:\\Py\\python.exe'\n")
        net = net_settings()
        ok(net["status"] == "all" and net["self"] == "kmbp" and set(net["hosts"]) == {"kvps", "kwin"},
           "net: the controller reads status, its name and the hosts from its config")
        text = net_toml("kvps", net)
        hdir = tmp / "net-host"
        hdir.mkdir()
        (hdir / "net.toml").write_text(text)
        g["CONFIG"] = hdir
        hn = net_settings()
        ok(hn["self"] == "kvps" and hn["status"] == "all" and hn["master"] == "kmbp" and set(hn["hosts"]) == {"kmbp", "kwin"}
           and hn["hosts"]["kmbp"]["script"].endswith("takt.py") and hn["hosts"]["kwin"]["python"] == "C:\\Py\\python.exe",
           "net: push gives a host the mode, the master and the other members (controller with its script path)")
        ok(remote_argv("kmbp", STATUS_PULL, hn["hosts"]["kmbp"])[-1].split()[1] == hn["hosts"]["kmbp"]["script"],
           "net: a host reaches the controller's takt.py by its own path")
        (cdir / "jobs.toml").write_text('[settings]\nstatus = "none"\n')
        g["CONFIG"] = cdir
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            main(["status", "-A"])
        ok("status display is off" in out.getvalue(), "status: settings.status = none shows no statuses")
        (cdir / "jobs.toml").write_text('[settings]\nstatus = "loud"\n')
        try:
            net_settings()
            ok(False, "net: an unknown status mode is an error")
        except ValueError:
            ok(True, "")
    finally:
        g["CONFIG"] = saved


def check_windows_jobs(ok, tmp):
    """Command preflights, on_event triggers, and replaces = ["schtasks:..."]."""
    passing, failing = [PY, "-c", "pass"], [PY, "-c", "import sys; print('wsl is not running'); sys.exit(3)"]
    ok(check_need(passing, {})[0] and check_need(failing, {}) == (False, "exit 3: wsl is not running"),
       "preflight: a command need passes on exit 0 and reports the exit code and last line")
    ok(not check_need(["takt-no-such-binary-xyz"], {})[0], "preflight: a missing command fails the need")
    d = tmp / "winjobs"
    d.mkdir()
    mk = d / "m.txt"
    sp = {"w": mkjob("w", needs=[failing], command=cmd_mark(mk, "w", 0))}
    run_job(sp, "w", d / "st", say=lambda *_: None)
    r = read_record(d / "st", "w")
    ok(r["status"] == "skipped" and "w" not in marks(mk) and f"needs cmd {Path(PY).name} -c" in r["note"],
       "preflight: a failing command need skips the job and names the command")
    for bad in (3, [], [1, 2]):
        try:
            mkjob("x", needs=[bad])
            ok(False, f"spec: need {bad!r} is rejected")
        except ValueError:
            ok(True, "")

    sub = "<QueryList><Query Id='0' Path='Microsoft-Windows-TerminalServices-LocalSessionManager/Operational'>" \
          "<Select>*[System[(EventID=21 or EventID=25)]]</Select></Query></QueryList>"
    j = mkjob("e", run_at_load=True, on_event=sub)
    x = ET.fromstring(render_schtasks(j, CTX))
    trig = x.find(f"{{{TS_NS}}}Triggers")
    ev = trig.find(f"{{{TS_NS}}}EventTrigger")
    ok(ev is not None and ev.find(f"{{{TS_NS}}}Subscription").text == sub and len(trig) == 2,
       "schtasks: on_event adds an EventTrigger with the query, next to the logon trigger")
    for render in (lambda: render_launchd(j, CTX), lambda: render_systemd(j, CTX)):
        try:
            render()
            ok(False, "launchd/systemd: on_event is refused")
        except ValueError:
            ok(True, "")
    import contextlib
    import io
    with contextlib.redirect_stderr(io.StringIO()):
        made = stage({"e": j, "p": mkjob("p", schedule="0 * * * *")}, CTX, d / "stage", ["launchd", "schtasks"])
    ok(sorted(m.name for m in made) == sorted([f"{PREFIX}.p.plist", "takt-e.xml", "takt-p.xml"]),
       "render: a Windows-only job is skipped for launchd, rendered for schtasks")

    acts = plan_install({"n": mkjob("n", schedule="*/10 * * * *", replaces=["schtasks:\\Old-Task"])},
                        {**CTX, "state": str(d / "ist")}, d / "x", "", "schtasks")
    runs = [a[:3] + a[3:4] for k, a, _ in acts if k == "run"]
    ok(runs[:2] == [["schtasks", "/End", "/TN", "\\Old-Task"], ["schtasks", "/Change", "/TN", "\\Old-Task"]]
       and acts[[k for k, *_ in acts].index("run") + 1][1][-1] == "/DISABLE" and runs[2][:2] == ["schtasks", "/Create"],
       "install schtasks: replaces ends and disables the old task before creating the new one")


def check_owned(ok, tmp):
    """install/uninstall touch only the jobs installed from the same jobs file."""
    la = tmp / "owned-la"
    la.mkdir()
    one, two = tmp / "specs/one.toml", tmp / "specs/two.toml"
    for jid, spec_path in (("a", one), ("b", two)):
        ctx = {**CTX, "spec": str(spec_path)}
        (la / f"{PREFIX}.{jid}.plist").write_bytes(render_launchd(mkjob(jid, schedule="0 * * * *"), ctx))
    ok(installed_from("launchd", la, "a", one) and not installed_from("launchd", la, "a", two)
       and not installed_from("launchd", la, "zzz", one), "owned: a job belongs to the jobs file its plist runs")
    x = tmp / "owned-x"
    x.mkdir()
    (x / "takt-w.xml").write_bytes(render_schtasks(mkjob("w", schedule="0 * * * *"), {**CTX, "spec": str(two)}).encode("utf-16"))
    ok(installed_from("schtasks", x, "w", two), "owned: the task XML (UTF-16) names its jobs file")
    nat = {"a": (True, True), "b": (True, True)}
    owned = [i for i in ("a", "b") if installed_from("launchd", la, i, two)]
    acts = plan_admin("install", {"c": mkjob("c", schedule="0 * * * *")}, "launchd", la, ["a", "b"], nat,
                      ctx={**CTX, "state": str(tmp / "owned-st"), "spec": str(two)}, loaded={f"{PREFIX}.a", f"{PREFIX}.b"}, owned=owned)
    gone = [Path(a).name for k, a, _ in acts if k == "rm"]
    ok(gone == [f"{PREFIX}.b.plist"], "install: retires only the stale jobs of its own jobs file")
    un = plan_admin("uninstall", {}, "launchd", la, ["a", "b"], nat, owned=owned)
    ok([Path(a).name for k, a, _ in un if k == "rm"] == [f"{PREFIX}.b.plist"], "uninstall: removes only its own jobs file's jobs")


def check_web(ok, tmp):
    """takt web: settings, scope, bind address, the service on each OS, retire, and a live server."""
    import urllib.error
    import urllib.request
    g = globals()
    saved = g["CONFIG"]
    try:
        cdir = tmp / "web-controller"
        cdir.mkdir()
        g["CONFIG"] = cdir
        (cdir / "jobs.toml").write_text('[settings]\nname = "kmbp"\nweb = ["kvps", "local"]\nweb_port = 9100\n'
                                        'web_writes = true\n\n[settings.web_bind]\nkvps = "100.68.171.58"\n')
        (cdir / "jobs.kvps.toml").write_text("[settings]\n")
        net = net_settings()
        ok(net["web"] == ["kvps", "kmbp"] and net["web_port"] == 9100 and net["web_writes"] and net["web_bind"] == {"kvps": "100.68.171.58"},
           "web: the controller reads web, web_port, web_writes and web_bind; local means this device")
        (cdir / "jobs.toml").write_text('[settings]\nname = "kmbp"\n')
        ok(net_settings()["web"] == [] and net_settings()["web_port"] == WEB_PORT and not net_settings()["web_writes"],
           "web: with no settings there is no web host, the default port, and writes are off")
        for bad in ('web = "kvps"', "web_port = 99999", 'web_bind = {kvps = "not-an-ip"}', "web = [\"a b\"]"):
            (cdir / "jobs.toml").write_text(f"[settings]\n{bad}\n")
            try:
                net_settings()
                ok(False, f"web: bad setting is an error: {bad}")
            except ValueError:
                ok(True, "")
        (cdir / "jobs.toml").write_text('[settings]\nname = "kmbp"\nweb = ["kvps"]\nweb_port = 9100\nweb_writes = true\n'
                                        '[settings.web_bind]\nkvps = "100.68.171.58"\n')
        hdir = tmp / "web-host"
        hdir.mkdir()
        (hdir / "net.toml").write_text(net_toml("kvps", net_settings()))
        g["CONFIG"] = hdir
        hn = net_settings()
        ok(hn["web"] == ["kvps"] and hn["self"] == "kvps" and hn["web_port"] == 9100 and hn["web_writes"]
           and hn["web_bind"] == {"kvps": "100.68.171.58"} and "kmbp" in hn["hosts"],
           "web: push carries the web settings to the host in net.toml, and the host is a web host")
        ctx = {**CTX, "state": str(tmp / "web-st")}
        argv = web_service_argv(ctx, hn)
        ok(argv[:3] == [CTX["python"], CTX["script"], "web"] and argv[argv.index("--port") + 1] == "9100"
           and argv[argv.index("--bind") + 1] == "100.68.171.58" and argv[-1] == "--allow-writes",
           "web: the service command carries the port, the bind address and the writes flag")
        ok(web_service_argv(ctx, {**hn, "self": "kwin"}) is None and "--allow-writes" not in web_service_argv(ctx, {**hn, "web_writes": False}),
           "web: a device outside `web` has no service, and writes are off unless asked for")
    finally:
        g["CONFIG"] = saved

    scope = {m: web_scope({"status": m}) for m in STATUS_MODES}
    ok(scope == {"master": "net", "all": "net", "client": "net", "none": "off"},
       f"web: a web host shows every device in master, all and client mode; none turns it off ({scope})")

    ts = lambda out: (lambda argv, **k: subprocess.CompletedProcess(argv, 0, out, ""))
    ok(tailscale_ip(ts("100.88.181.49\nfd7a:115c::1\n"), lambda n: "/x/tailscale", lambda p: True) == "100.88.181.49"
       and tailscale_ip(ts("fd7a:115c::1\n"), lambda n: "/x/tailscale", lambda p: True) is None
       and tailscale_ip(ts("100.1.1.1"), lambda n: None, lambda p: False) is None,
       "web: the bind address is the Tailscale IPv4; no tailscale means none")
    nn = {"self": "kvps", "web_bind": {"kvps": "100.9.9.9"}}
    ok(web_bind_addr("127.0.0.1", nn, lambda: "100.1.1.1") == "127.0.0.1" and web_bind_addr(None, nn, lambda: "100.1.1.1") == "100.9.9.9"
       and web_bind_addr(None, {**nn, "web_bind": {}}, lambda: "100.1.1.1") == "100.1.1.1",
       "web: --bind wins over settings.web_bind, which wins over the Tailscale address")
    try:
        web_bind_addr(None, {**nn, "web_bind": {}}, lambda: None)
        ok(False, "web: no address means no server, never a wildcard")
    except SystemExit:
        ok(True, "")
    hosts_ok = {h: host_ok(h) for h in ("100.68.171.58:8787", "[::1]:8787", "localhost:1", "kvps:8787", "kvps.tail1234.ts.net",
                                        "evil.example.com", "evil.com:80", "")}
    ok([k for k, v in hosts_ok.items() if not v] == ["evil.example.com", "evil.com:80", ""], f"web: Host header check ({hosts_ok})")

    # the service on each OS
    ctx = {**CTX, "state": str(tmp / "web-st")}
    cmd = [CTX["python"], CTX["script"], "web", "--spec", CTX["spec"], "--state", ctx["state"], "--port", "8787"]
    pl = plistlib.loads(render_web_launchd(cmd, ctx))
    ok(pl["KeepAlive"] is True and pl["RunAtLoad"] is True and pl["ProgramArguments"] == cmd and pl["Label"] == "dev.takt-web",
       "web: the launchd agent has KeepAlive and runs at load")
    sd = render_web_systemd(cmd, ctx)
    ok("Restart=always" in sd and "WantedBy=default.target" in sd and "Type=simple" in sd and "ExecStart=" + CTX["python"] in sd,
       "web: the systemd user service restarts always and starts with the user manager")
    x = render_web_schtasks(["C:\\Py\\python.exe", "C:\\t\\takt.py", "web"])
    root = ET.fromstring(x)
    q = lambda t: root.iter(f"{{{TS_NS}}}{t}")
    ok(len(list(q("LogonTrigger"))) == 1 and not list(q("CalendarTrigger")) and next(q("Command")).text.endswith("pythonw.exe")
       and next(q("ExecutionTimeLimit")).text == "PT0S" and list(q("RestartOnFailure")),
       "web: the Windows task starts at logon, has no time limit, restarts on failure and uses pythonw")
    ok(not WEB_LABEL.startswith(PREFIX + ".") and not WEB_UNIT.startswith("takt-") and not WEB_TASK.startswith("\\takt\\"),
       "web: the service names lie outside the job namespaces")

    for be, pre in (("launchd", "dev.takt-web.plist"), ("systemd", "takt.web.service"), ("schtasks", "takt.web.xml")):
        d = tmp / f"web-{be}"
        d.mkdir()
        spec = {"new": mkjob("new", schedule="0 * * * *")}
        up = plan_admin("install", spec, be, d, [], {}, ctx=ctx, web=cmd)
        ok(any(k == "write" and a.name == pre for k, a, _ in up), f"web: install on a web host writes the service ({be})")
        ok(be != "systemd" or any(c[:3] == ["systemctl", "--user", "restart"] and c[3] == "takt.web.service" for k, c, _ in up if k == "run"),
           "web: install starts the systemd service")
        ok(be != "schtasks" or any(k == "run" and c[:2] == ["schtasks", "/Run"] for k, c, _ in up), "web: install starts the Windows task")
        down = plan_admin("install", spec, be, d, [], {}, ctx=ctx, web=False)
        ok(not any(a == d / pre for k, a, _ in down), f"web: a device that is not a web host has nothing to retire ({be})")
        (d / pre).write_text("x")
        ok(installed_ids(be, d, "") == [], f"web: the service file is not listed as a job named web ({be})")
        (d / ("takt-web.service" if be == "systemd" else "takt-web.xml" if be == "schtasks" else "dev.takt.web.plist")).write_text("job")
        retire = plan_admin("install", spec, be, d, [], {}, ctx=ctx, web=False)
        ok(("rm", d / pre, None) in retire, f"web: install on a device that left `web` retires the service ({be})")
        ok(("rm", d / "takt-web.service", None) not in retire and ("rm", d / "takt-web.xml", None) not in retire
           and ("rm", d / "dev.takt.web.plist", None) not in retire, f"web: retiring the service leaves a job named web alone ({be})")
        un = plan_admin("uninstall", {}, be, d, [], {}, web=False)
        ok(("rm", d / pre, None) in un, f"web: uninstall removes the service ({be})")
        ok(("rm", d / pre, None) not in plan_admin("uninstall", {}, be, d, [], {}, ids=["x"], web=False),
           f"web: uninstall of one job leaves the service ({be})")
        ok(("rm", d / pre, None) not in plan_admin("uninstall", {}, be, d, [], {}), f"web: plan_admin without `web` does not touch it ({be})")
    ok(any(c[:2] == ["launchctl", "bootout"] for k, c, _ in plan_web("launchd", ctx, tmp / "web-launchd", None, {WEB_LABEL})),
       "web: a loaded launchd agent is booted out before it is removed")
    ok(any(c[:2] == ["launchctl", "bootout"] for k, c, _ in plan_web("launchd", ctx, tmp / "web-launchd", cmd, {WEB_LABEL})),
       "web: a reinstall boots the loaded launchd agent out first")

    # a live server on 127.0.0.1, with the outside world stubbed
    calls = []
    me = {"self": "kvps", "hosts": {"kmbp": {"python": "p"}, "kwin": {"python": "w"}}, "status": "master", "web": ["kvps"],
          "web_port": 0, "web_writes": False, "web_bind": {}}

    seeded = {"kmbp": {"host": "kmbp", "received": "2026-10-04T20:00:00+00:00", "rows": [
        {"id": "j", "sched": "on", "status": "ok", "last": None, "next": None, "detail": "",
         "record": {"status": "ok", "started": "2026-10-04T19:59:00", "duration_s": 1, "slot": "x", "trigger": "scheduled",
                    "steps": [{"id": "main", "status": "ok", "exit": 0, "duration_s": 1, "failed": []}], "preflight": []}}]}}
    fixed_now = lambda: datetime(2026, 10, 4, 20, 5, tzinfo=timezone.utc)

    def serve(writes, whois=lambda ip: None, state=None):
        app = WebApp(me, tmp / "nospec.toml", state or tmp / "web-st", writes, run=lambda a: (calls.append(a), (0, "ran"))[1],
                     local_rows=lambda: [{"id": "mine", "sched": "on", "status": "ok", "last": None, "next": None, "detail": ""}],
                     whois=whois, now=fixed_now)
        if state is None:
            app.reports = dict(seeded)
        app.quiet = True
        srv = ThreadingHTTPServer(("127.0.0.1", 0), web_handler(app))
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        return srv, f"http://127.0.0.1:{srv.server_address[1]}"

    def http(url, data=None, headers=None):
        req = urllib.request.Request(url, data=data, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    post = lambda base, body, h=None: http(base + "/api/act", json.dumps(body).encode(), {"X-Takt": "1", **(h or {})})
    srv, base = serve(False)
    try:
        code, body = http(base + "/api/status")
        d = json.loads(body)
        by = {(r["host"], r["id"]): r for r in d["rows"]}
        ok(code == 200 and set(by) == {("kvps", "mine"), ("kmbp", "j"), ("kwin", "-")} and by[("kwin", "-")]["status"] == "no report"
           and d["writes"] is False and "record" not in by[("kmbp", "j")]
           and "kmbp reported 5 min ago" in d["reports"] and "kwin no report yet" in d["reports"],
           "web: /api/status has this device's rows and each peer's last report, with its age; a silent peer is a row")
        ok("takt" in http(base + "/")[1] and http(base + "/")[0] == 200, "web: / serves the page")
        n = len(calls)
        code, body = http(base + "/api/show?host=kmbp&id=j")
        ok(code == 200 and body.startswith("j  ok") and "step main" in body and "log tails stay on kmbp" in body and len(calls) == n,
           "web: show of a peer comes from its report; the server runs nothing and logs in nowhere")
        http(base + "/api/show?host=kvps&id=mine")
        ok(calls[-1][1:3] == [str(Path(__file__).resolve()), "show"] or calls[-1][2] == "show", "web: show of this device runs locally")
        n = len(calls)
        ok(http(base + "/api/show?host=nope&id=j")[0] == 400 and http(base + "/api/show?host=kvps&id=a%20b")[0] == 400 and len(calls) == n,
           "web: an unknown device or a bad job id runs nothing")
        ok(http(base + "/api/status", headers={"Host": "evil.example.com"})[0] == 403, "web: a foreign Host header is refused")
        ok(post(base, {"host": "kmbp", "id": "j", "verb": "start"})[0] == 403 and len(calls) == n,
           "web: without --allow-writes a POST starts nothing")
    finally:
        srv.shutdown()
        srv.server_close()
    srv, base = serve(True)
    try:
        ok(json.loads(http(base + "/api/status")[1])["writes"] is True, "web: with --allow-writes the page gets the buttons")
        n = len(calls)
        code, _ = post(base, {"host": "kmbp", "id": "j", "verb": "start"})
        ok(code == 200 and len(calls) == n + 1 and calls[-1][-1].endswith("start j --allow-writes"),
           "web: with --allow-writes a POST runs the verb on that device")
        ok(http(base + "/api/act", json.dumps({"host": "kmbp", "id": "j", "verb": "start"}).encode())[0] == 403
           and post(base, {"host": "kmbp", "id": "j", "verb": "start"}, {"Origin": "http://evil.example.com"})[0] == 403 and len(calls) == n + 1,
           "web: a POST needs the X-Takt header and no foreign Origin (CSRF)")
        ok(post(base, {"host": "kmbp", "id": "j", "verb": "rm"})[0] == 400 and post(base, {"host": "kmbp", "id": "j; rm", "verb": "start"})[0] == 400
           and len(calls) == n + 1, "web: only start, enable and disable run, and only for a plain job id")
    finally:
        srv.shutdown()
        srv.server_close()
    app = WebApp({**me, "status": "none"}, tmp / "x", tmp / "y", False, local_rows=lambda: [{"id": "a"}])
    ok(app.rows()["rows"] == [], "web: settings.status = none shows no rows")
    ok(build_parser().parse_args(["web", "--bind", "127.0.0.1", "--port", "1"]).port == 1
       and "web" in starter_spec() and 'web = ["myvps"]' in starter_spec(), "web: the CLI has `web`, and takt init mentions it")


def check_web_reports(ok, tmp):
    """Peers push their rows to the web host; the tailnet says who is calling."""
    import urllib.error
    net = {"self": "kvps", "hosts": {"kmbp": {}, "kwin": {}}, "status": "master", "web": ["kvps"], "web_port": 0,
           "web_writes": False, "web_bind": {}}
    st = tmp / "reports-st"
    app = WebApp(net, tmp / "nospec.toml", st, False, local_rows=lambda: [],
                 whois=lambda ip: {"100.1.1.2": "kwin", "100.1.1.3": "evil", "100.1.1.4": "kvps"}.get(ip),
                 now=lambda: datetime(2026, 10, 4, 21, 0, tzinfo=timezone.utc))
    row = {"id": "disk-watch", "sched": "on", "status": "partial", "last": "2026-10-04T23:00:42", "next": None,
           "detail": "D: 9.4% free", "record": {"status": "partial", "steps": [], "preflight": []}, "junk": "x" * 10}
    ok(app.receive("100.1.1.2", {"host": "kwin", "rows": [row]}) == (200, "ok") and (st / "reports/kwin.json").exists(),
       "report: the tailnet's kwin may report as kwin, and the report is kept on disk")
    got = {r["id"]: r for r in app.rows()["rows"] if r["host"] == "kwin"}
    ok(got["disk-watch"]["status"] == "partial" and "junk" not in got["disk-watch"] and "record" not in got["disk-watch"],
       "report: the page shows the reported rows, only the known fields")
    ok(app.receive("100.1.1.2", {"host": "kmbp", "rows": []})[0] == 403, "report: a device cannot report as another")
    ok(app.receive("100.9.9.9", {"host": "kwin", "rows": []})[0] == 403, "report: an address the tailnet does not know is refused")
    ok(app.receive("100.1.1.4", {"host": "kvps", "rows": []})[0] == 403 and app.receive("100.1.1.3", {"host": "evil", "rows": []})[0] == 403,
       "report: a tailnet device that is not in the takt-net, or the web host itself, cannot report")
    ok(app.receive("100.1.1.2", {"host": "kwin", "rows": "x"})[0] == 400 and app.receive("100.1.1.2", {})[0] == 400,
       "report: a malformed report is refused")
    again = WebApp(net, tmp / "nospec.toml", st, False, local_rows=lambda: [], whois=lambda ip: None)
    ok(again.reports.get("kwin", {}).get("rows", [{}])[0].get("id") == "disk-watch", "report: reports survive a restart")
    ok(again.show("kwin", "disk-watch")[0] == 200 and again.show("kwin", "nope")[0] == 404, "report: show reads the reported record")

    sent = []

    def post(url, body, timeout=5):
        sent.append((url, json.loads(body), timeout))
        return 200
    res = send_reports({"self": "kwin", "web": ["kvps", "kwin"], "web_port": 8787, "web_bind": {}}, [{"id": "a"}],
                       post=post, finder=lambda peer=None, timeout=None: {"kvps": "100.68.171.58"}.get(peer))
    ok(res == {"kvps": "ok"} and [x[:2] for x in sent] == [("http://100.68.171.58:8787/api/report", {"host": "kwin", "rows": [{"id": "a"}]})]
       and sent[0][2] <= 5,
       "report: a device sends its rows to every web host but itself, at its Tailscale address")

    def down(url, body, timeout=5):
        raise urllib.error.URLError("connection refused")
    res = send_reports({"self": "kwin", "web": ["kvps"], "web_port": 8787, "web_bind": {"kvps": "10.0.0.5"}}, [], post=down)
    ok(res["kvps"].startswith("URLError") , "report: a web host that is down is an error string, never an exception")
    g = globals()
    saved = g["CONFIG"]
    try:
        g["CONFIG"] = tmp / "report-config"
        own = g["CONFIG"] / "jobs.toml"
        ok(should_report(own, {}) and not should_report(tmp / "other.toml", {}) and not should_report(own, {"TAKT_NO_REPORT": "1"}),
           "report: only a run of this device's own jobs file reports, and TAKT_NO_REPORT turns it off")
    finally:
        g["CONFIG"] = saved


def check_pub_review1(ok, tmp):
    """Regressions for the first review of the public repo (2026-10-04)."""
    import contextlib
    import io
    # ownership: overlapping ids, and whole-path matching
    la = tmp / "pr1-la"
    la.mkdir()
    a_spec, b_spec, backup = tmp / "pr1/a.toml", tmp / "pr1/b.toml", tmp / "pr1/b.toml.backup"
    (la / f"{PREFIX}.shared.plist").write_bytes(render_launchd(mkjob("shared", schedule="0 * * * *"), {**CTX, "spec": str(a_spec)}))
    nat = {"shared": (True, True)}
    owned_b = [i for i in ["shared"] if installed_from("launchd", la, i, b_spec)]
    try:
        plan_admin("install", {"shared": mkjob("shared", schedule="0 * * * *")}, "launchd", la, ["shared"], nat,
                   ctx={**CTX, "state": str(tmp / "pr1st"), "spec": str(b_spec)}, loaded={f"{PREFIX}.shared"}, owned=owned_b)
        ok(False, "install: refuses a job id that another jobs file installed")
    except ValueError as e:
        ok("another jobs file" in str(e), "install: refuses a job id that another jobs file installed")
    un = plan_admin("uninstall", {"shared": mkjob("shared")}, "launchd", la, ["shared"], nat, owned=owned_b)
    ok(un == [], "uninstall: leaves a same-named job of another jobs file alone")
    (la / f"{PREFIX}.bk.plist").write_bytes(render_launchd(mkjob("bk", schedule="0 * * * *"), {**CTX, "spec": str(backup)}))
    amp = tmp / "pr1/R&D.toml"
    (la / f"{PREFIX}.amp.plist").write_bytes(render_launchd(mkjob("amp", schedule="0 * * * *"), {**CTX, "spec": str(amp)}))
    sd = tmp / "pr1-sd"
    sd.mkdir()
    spaced = tmp / "pr1/with space.toml"
    (sd / "takt-sp.service").write_text(render_systemd(mkjob("sp", schedule="0 * * * *"), {**CTX, "spec": str(spaced)})[0])
    ok(not installed_from("launchd", la, "bk", b_spec) and installed_from("launchd", la, "amp", amp)
       and installed_from("systemd", sd, "sp", spaced) and not installed_from("systemd", sd, "sp", tmp / "pr1/with"),
       "owned: whole paths, parsed from the unit (no prefix match; & and spaces are fine)")

    # concurrent reports of one device
    net = {"self": "kvps", "hosts": {"kwin": {}}, "status": "master", "web": ["kvps"], "web_port": 0, "web_writes": False, "web_bind": {}}
    app = WebApp(net, tmp / "x.toml", tmp / "pr1-web", False, local_rows=lambda: [], whois=lambda ip: "kwin")
    results, bar = [], threading.Barrier(8)

    def one(i):
        bar.wait()
        results.append(app.receive("100.1.1.2", {"host": "kwin", "rows": [{"id": f"j{i}"}]}))
    ts = [threading.Thread(target=one, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    saved = json.loads((tmp / "pr1-web/reports/kwin.json").read_text())
    ok(results == [(200, "ok")] * 8 and len(saved["rows"]) == 1, "report: 8 concurrent reports of one device all land, one file")

    # catch_up = "skip" with a logon or event trigger
    for kw in ({"run_at_load": True}, {"on_event": "<QueryList/>"}):
        try:
            mkjob("x", schedule="0 3 * * *", catch_up="skip", **kw)
            ok(False, f"spec: catch_up = skip with {list(kw)[0]} is refused")
        except ValueError:
            ok(True, "")

    # web: IPv6 refused, web_writes honoured by a manual start
    try:
        web_bind_addr("::1", {"self": "x", "web_bind": {}})
        ok(False, "web: an IPv6 bind is refused")
    except SystemExit as e:
        ok("IPv6" in str(e), "web: an IPv6 bind is refused")
    try:
        web_settings({"web_bind": {"x": "fd7a::1"}}, "x")
        ok(False, "web: an IPv6 web_bind is refused")
    except ValueError:
        ok(True, "")
    made = []
    g = globals()
    saved_app, saved_srv, saved_net = g["WebApp"], g["ThreadingHTTPServer"], g["net_settings"]

    class Stop(Exception):
        pass

    def fake_srv(*a, **k):
        raise Stop
    try:
        g["WebApp"] = lambda net, spec, state, writes, **k: made.append(writes)
        g["ThreadingHTTPServer"] = fake_srv
        g["net_settings"] = lambda: {"self": "x", "hosts": {}, "status": "master", "web": ["x"], "web_port": 1,
                                     "web_writes": True, "web_bind": {}}
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                web_main(build_parser().parse_args(["web", "--bind", "127.0.0.1"]))
        except Stop:
            pass
    finally:
        g["WebApp"], g["ThreadingHTTPServer"], g["net_settings"] = saved_app, saved_srv, saved_net
    ok(made == [True], "web: a manual `takt web` honours web_writes = true")

    # reports: one deadline for lookup and HTTP
    seen = {}

    def finder(peer=None, timeout=None):
        seen.setdefault("lookup", timeout)
        return "100.1.1.1"

    def post(url, body, timeout=None):
        seen.setdefault("post", timeout)
        return 200
    ticks = iter([0, 0, 7.8, 7.9, 8.6])
    res = send_reports({"self": "kwin", "web": ["a", "b"], "web_port": 1, "web_bind": {}}, [], post=post, finder=finder,
                       budget=8, clock=lambda: next(ticks))
    ok(res["a"] == "ok" and seen["lookup"] <= 3 and seen["post"] <= 5 and res["b"].startswith("skipped"),
       "report: lookup and HTTP share one deadline; a web host after it is skipped")


def check_pub_review2(ok, tmp):
    """Regressions for the second review of the public repo (2026-10-04)."""
    import http.client
    # a case twin installed from another jobs file is foreign too
    la = tmp / "pr2-la"
    la.mkdir()
    (la / f"{PREFIX}.Foo.plist").write_bytes(render_launchd(mkjob("Foo", schedule="0 * * * *"), {**CTX, "spec": str(tmp / "pr2/a.toml")}))
    b = tmp / "pr2/b.toml"
    try:
        plan_admin("install", {"foo": mkjob("foo", schedule="0 * * * *")}, "launchd", la, ["Foo"], {"Foo": (True, True)},
                   ctx={**CTX, "state": str(tmp / "pr2st"), "spec": str(b)}, loaded={f"{PREFIX}.Foo"},
                   owned=[i for i in ["Foo"] if installed_from("launchd", la, i, b)])
        ok(False, "install: refuses foo when another jobs file installed Foo")
    except ValueError:
        ok(True, "")
    # */7 cannot repeat across the hour on Task Scheduler
    tr = schtasks_triggers(parse_cron("*/7 * * * *"))
    ok(len(tr) == 9 and all(r == "PT1H" for _, _, r in tr) and schtasks_triggers(parse_cron("*/20 * * * *")) == [(0, 0, "PT20M")],
       "schtasks: */7 is one hourly trigger per minute (restarts at :00); */20 still repeats")
    # a leading hyphen would be an option in the wrapper command
    try:
        mkjob("-backup")
        ok(False, "spec: an id that starts with - is refused")
    except ValueError:
        ok(True, "")
    # reports: plain types only, on receive and on load; a bad record is an error, not a crash
    net = {"self": "kvps", "hosts": {"kwin": {}}, "status": "master", "web": ["kvps"], "web_port": 0, "web_writes": False, "web_bind": {}}
    st = tmp / "pr2-web"
    app = WebApp(net, tmp / "x.toml", st, False, local_rows=lambda: [], whois=lambda ip: "kwin")
    ok(app.receive("1", {"host": "kwin", "rows": [{"id": "j", "last": {"x": 1}, "status": ["ok"], "record": {"steps": [1]}}]}) == (200, "ok"),
       "report: odd field types are accepted, reduced")
    r = app.reports["kwin"]["rows"][0]
    ok(isinstance(r["last"], str) and isinstance(r["status"], str), "report: a non-scalar field is kept as text")
    ok(app.show("kwin", "j")[0] == 502, "report: a record takt cannot read is a 502, not an exception")
    (st / "reports/kwin.json").write_text(json.dumps({"host": "kwin", "rows": [{"id": "j", "next": [1, 2], "record": "x"}]}))
    (st / "reports/kbad.json").write_text("[1]")
    again = WebApp(net, tmp / "x.toml", st, False, local_rows=lambda: [], whois=lambda ip: None)
    ok(isinstance(again.reports["kwin"]["rows"][0]["next"], str) and again.reports["kwin"]["rows"][0]["record"] is None
       and "kbad" not in again.reports, "report: a stored report is cleaned on load; a broken one is dropped")
    # negative Content-Length is a bad request on both endpoints
    srv = ThreadingHTTPServer(("127.0.0.1", 0), web_handler(app))
    app.quiet = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        codes = []
        for path in ("/api/report", "/api/act"):
            c = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
            c.putrequest("POST", path)
            c.putheader("X-Takt", "1")
            c.putheader("Content-Length", "-1")
            c.endheaders()
            codes.append(c.getresponse().status)
            c.close()
        ok(codes == [400, 400], "web: a negative Content-Length is refused on both POST endpoints")
        ok(web_handler(app).timeout == 10, "web: a stalled request times out")
    finally:
        srv.shutdown()
        srv.server_close()


def check_pub_review3(ok, tmp):
    """Regressions for the third review of the public repo (2026-10-05)."""
    st = tmp / "pr3-st"
    st.mkdir()
    write_record(st, {"id": "a", "slot": "2026-10-04T15:00:00", "pulled_by": "b", "status": "ok", "steps": []})
    ok(pulled_for(st, "a", datetime(2026, 10, 4, 14)) and pulled_for(st, "a", datetime(2026, 10, 4, 15))
       and not pulled_for(st, "a", datetime(2026, 10, 4, 16)),
       "run: a queued run for an older slot is obsolete after a pull-in recorded a newer one")
    try:
        remote_argv("kvps", ["start", "\\--allow-writes"], cfg={})
        ok(False, "remote: a backslash in a forwarded argument is refused")
    except ValueError:
        ok(True, "")
    ok(remote_argv("kwin", ["status"], cfg={"python": "C:\\Py\\python.exe"})[-1].startswith("C:\\Py"),
       "remote: the python setting may still be a Windows path")
    for line in ("/bin/echo hello 1>> log", "/bin/echo hello 3>> log"):
        try:
            split_steps(line)
            ok(False, f"import-cron: {line!r} stays a shell line")
        except ValueError:
            ok(True, "")
    ok(split_steps("/bin/echo hello 2>> err") == [(["/bin/echo", "hello"], None, "err")], "import-cron: 2>> still splits")
    pl = plistlib.dumps({"Label": "com.x.y.z", "Program": "/bin/echo", "ProgramArguments": ["custom-argv0", "hello"]})
    ok(parse_launchd(pl)["steps"][0]["command"] == ["/bin/echo", "hello"], "import-plist: Program is the executable")
    cal = [{"Hour": 1, "Minute": 0}, {"Hour": 1, "Minute": 0}, {"Hour": 2, "Minute": 0}, {"Hour": 2, "Minute": 30}]
    try:
        parse_launchd(plistlib.dumps({"Label": "com.x.y.z", "ProgramArguments": ["/bin/true"], "StartCalendarInterval": cal}))
        ok(False, "import-plist: duplicate calendar dicts cannot hide a missing combination")
    except ValueError:
        ok(True, "")
    big = tmp / "pr3-big.log"
    big.write_text("x" * 200_000 + "\n" + "".join(f"line {i}\n" for i in range(20)))
    ok(tail(big) == [f"line {i}" for i in range(12, 20)] and tail(big, cap=30) == ["line 17", "line 18", "line 19"],
       "show: the log tail comes from the end of the file")
    t = tmp / "pr3-utf8.toml"
    t.write_bytes('[job.u]\ncommand = ["echo", "J\u00f8rgen"]\n'.encode("utf-8"))
    ok(load_spec(t)["u"]["steps"][0]["command"][1] == "J\u00f8rgen", "spec: a jobs file is read as UTF-8")


def check_plans_apply(ok, tmp):
    """Every install plan, with the web service, applies: each write is bytes and lands on disk.
    (The systemd web unit was planned as text and crashed apply_plan on kvps, 2026-10-04.)"""
    spec = {"j": mkjob("j", schedule="0 * * * *")}
    for be in ("launchd", "systemd", "schtasks"):
        d = tmp / f"apply-{be}"
        ctx = {**CTX, "state": str(d / "state")}
        acts = plan_admin("install", spec, be, d / "agents", [], {}, ctx=ctx, loaded=set(),
                          web=["/usr/bin/python3", "/opt/takt/takt.py", "web"])
        writes = [(a, b) for k, a, b in acts if k == "write"]
        bad = [str(a) for a, b in writes if not isinstance(b, bytes)]
        apply_plan(acts, lambda a, i: None)
        ok(not bad and writes and all(a.exists() for a, _ in writes),
           f"install {be}: every planned write (jobs and web service) is bytes and lands ({bad})")


def check_windows_last(ok):
    """Windows only: end_job_leftovers really ends (and awaits) the other processes in the job.
    This check process joins a kill-on-close job, so it runs last."""
    if os.name != "nt" or end_steps_with_wrapper() is not None:
        return
    helper = subprocess.Popen([PY, "-c", "import time; time.sleep(30)"], **NO_WINDOW)
    end_job_leftovers()
    ok(helper.poll() is not None, "windows: a step's leftover is ended, and awaited, before the locks are released")


def check_watch(ok, tmp):
    """kikar's watcher, as a step: edge, thresholds, cooldown, flap digest, failure never read as a
    change, delivery retry, new-items. Runs real wrappers in this process against a fake clock and a
    fake sender."""
    d = tmp / "watch"
    d.mkdir()
    st, sent, fail = d / "st", [], {"err": None}
    real = DELIVER[0]
    # records only what was delivered; fail["err"] makes the next sends fail
    DELIVER[0] = lambda url, title, text, token=None: fail["err"] or sent.append((title.rsplit(": ", 1)[-1], text))
    T = datetime(2026, 10, 7, 12, 0)
    rd = [PY, "-c", "import sys;sys.stdout.write(open(sys.argv[1], encoding='utf-8').read())"]

    def run(j, at):
        run_job({j["id"]: j}, j["id"], st, now=T + timedelta(seconds=at), say=lambda *a: None)
        return read_record(st, j["id"])

    def watcher(jid, **kw):
        vf = d / f"{jid}.v"
        kw.setdefault("notify", "ntfy://t")
        j = mkjob(jid, command=rd + [str(vf)], report="watch", **kw)

        def tick(at, value=None):
            if value is not None:
                vf.write_text(value, encoding="utf-8")
            r = run(j, at)
            return r["steps"][0]["note"] if r["status"] == "ok" else r["status"]
        return j, tick

    told = lambda jid: [t for k, t in sent if k == jid]
    try:
        # 1. edge: a new watch seeds silently, one notice per change, none while it stays
        _, tick = watcher("edge")
        ok(tick(0, "A") == "baseline", "watch: first read seeds silently")
        [tick(60 * i) for i in range(1, 6)]
        ok(told("edge") == [], "watch: no notice without a change")
        ok(tick(360, "B") == "A -> B" and told("edge") == ["A -> B"], "watch: a change is told")
        [tick(60 * i) for i in range(7, 12)]
        ok(len(told("edge")) == 1 and tick(720) == "no change", "watch: a changed value is told once (edge, not level)")
        # 2. thresholds: told on entering the state, re-armed on leaving it
        _, tick = watcher("thr", compare="above", target=10)
        notes = [tick(60 * i, v) for i, v in enumerate(["5", "12", "15", "20", "8", "11", "11"])]
        ok(len(told("thr")) == 2 and notes[3] == "in state", f"watch: above 10 told twice over 7 reads: {notes}")
        _, tick = watcher("hot", compare="above", target=10)
        tick(0, "50")
        ok(told("hot") == ["50 is above 10"], "watch: a value over the line at install is told once")
        # 3. cooldown defers without losing, and A -> B -> A inside it is nothing
        _, tick = watcher("cool", cooldown="30m")
        tick(0, "1"), tick(60, "2")
        ok(tick(120, "3").endswith("cooldown") and len(told("cool")) == 1, "watch: cooldown holds a second notice")
        tick(60 + 1800)
        ok(told("cool")[-1:] == ["2 -> 3"], f"watch: a change held by cooldown is told after it: {told('cool')}")
        tick(1900, "4"), tick(1960, "3"), tick(60 + 3600 + 60)
        ok(len(told("cool")) == 2, "watch: A -> B -> A inside cooldown is not told")
        # 4. flap: past flap_max moves in the window, one digest at its end
        _, tick = watcher("flap", flap_window="10m", flap_max=3)
        tick(0, "up")
        notes = [tick(60 * i, "down" if i % 2 else "up") for i in range(1, 9)]
        ok(len(told("flap")) == 3 and "flapping" in notes, f"watch: 3 notices, then flapping: {notes}")
        [tick(60 * i, "down") for i in range(9, 30)]
        dg = told("flap")[3:]
        ok(len(dg) == 1 and "now down" in dg[0], f"watch: exactly one digest with where it landed: {dg}")
        # 5. failure is never change: each kind fails the step, keeps the last value, alerts once
        #    after notify_after bad runs, and once more when it is back
        bad = {"nonzero": ([PY, "-c", "print('B');raise SystemExit(3)"], {}),
               "timeout": ([PY, "-c", "import time;time.sleep(5);print('B')"], {"timeout": 0.5}),
               "empty": ([PY, "-c", "pass"], {}), "blank": ([PY, "-c", "print('  \\n')"], {}),
               "garbled": ([PY, "-c", "print('<html>502</html>')"], {"expect": "^[A-Z]$"})}
        for kind, (cmd, kw) in bad.items():
            jid = f"fail-{kind}"
            good, tick = watcher(jid, notify_after=2, expect=kw.get("expect"))
            broken = mkjob(jid, command=cmd, report="watch", notify="ntfy://t", notify_after=2, **kw)
            tick(0, "A")
            outs = [run(broken, 60)["status"]]
            ok(told(jid) == [], f"watch {kind}: no alert after 1 bad run with notify_after = 2")
            outs.append(run(broken, 120)["status"])
            last = json.loads((st / "notify" / f"{jid}.json").read_text())["steps"]["main"]["last"]
            ok(outs == ["failed", "failed"] and last == "A", f"watch {kind}: a failed read, last value kept: {outs} {last!r}")
            ok(len(told(jid)) == 1 and told(jid)[0].startswith("failed:"), f"watch {kind}: one alert after 2 bad runs: {told(jid)}")
            ok(tick(180) == "no change" and told(jid)[1:] == ["ok again after 2 bad runs"],
               f"watch {kind}: one recovery notice, same value is no change: {told(jid)}")
            run(broken, 240), run(broken, 300)
            ok(sum(t.startswith("failed:") for t in told(jid)) == 2, f"watch {kind}: a second outage is told again")
        _, tick = watcher("nan", compare="below", target=3)
        tick(0, "7")
        ok(tick(60, "n/a") == "failed" and len(told("nan")) == 1, "watch: below reads a non-number as a failure")
        # 6. a failed delivery does not move what the user was told; a later run sends it
        _, tick = watcher("deliv")
        tick(0, "X")
        fail["err"] = "ntfy down"
        ok(tick(60, "Y").endswith("notify failed") and read_record(st, "deliv")["notify_error"] == "ntfy down",
           "watch: a failed delivery is recorded")
        fail["err"] = None
        ok(tick(120) == "X -> Y" and read_record(st, "deliv")["sent"] == ["X -> Y"], "watch: a failed delivery is retried")
        # 7. new-items: only lines never seen before, in the order they came; cooldown keeps them
        _, tick = watcher("items", compare="new-items")
        ok(tick(0, "c3\nc2\nc1\n") == "baseline" and tick(60) == "no new items", "new-items: seeds silently")
        tick(120, "c5\nc4\nc3\nc2\n"), tick(180)
        ok(told("items") == ["2 new:\nc5\nc4"], f"new-items: told once, only the new lines: {told('items')}")
        _, tick = watcher("icool", compare="new-items", cooldown="1h")
        tick(0, "a1\n"), tick(60, "a2\na1\n")
        ok(tick(120, "a3\na2\n").endswith("cooldown"), "new-items: cooldown holds")
        tick(180, "a4\na3\n"), tick(60 + 3600, "a5\na4\n")
        ok(told("icool") == ["1 new:\na2", "3 new:\na3\na4\na5"], f"new-items: an item off the page in cooldown is kept: {told('icool')}")
        # 8. any job: notify on failure, once per bad streak, and on recovery; nothing without notify
        flag = d / "fail.flag"
        cmd = [PY, "-c", "import os,sys;sys.exit(1 if os.path.exists(sys.argv[1]) else 0)", str(flag)]
        j = mkjob("plain", command=cmd, notify="https://hooks.example/x")
        flag.write_text("")
        run(j, 0), run(j, 60)
        flag.unlink()
        run(j, 120), run(j, 180)
        ok(told("plain") == ["failed: main: failed exit 1", "ok again after 2 bad runs"], f"notify: one alert per bad streak, one recovery: {told('plain')}")
        n = len(sent)
        flag.write_text("")
        run(mkjob("quiet", command=cmd), 0)
        ok(len(sent) == n and not read_record(st, "quiet").get("sent"), "notify: nothing is sent without notify")
        run(mkjob("skip", needs=["exe:takt-no-such-binary"], notify="ntfy://t"), 0)
        ok(len(told("skip")) == 1 and told("skip")[0].startswith("skipped:"), "notify: a skipped job is told")
        # 9. a timeout ends the step and what it started (POSIX: the grandchild too)
        pidf = d / "pid"
        cmd = ([PY, "-c", "import time;time.sleep(30)"] if os.name == "nt"
               else ["/bin/sh", "-c", f"sleep 30 & echo $! > {pidf}; wait"])
        t0 = time.time()
        r = run(mkjob("hang", command=cmd, timeout=0.5), 0)
        ok(r["status"] == "failed" and r["steps"][0]["note"] == "timed out after 0.5s" and time.time() - t0 < 10,
           f"timeout: the step is failed and named: {r['steps'][0]}")
        if os.name != "nt":
            pid, alive = int(pidf.read_text()), True
            for _ in range(40):
                try:
                    os.kill(pid, 0)
                    time.sleep(0.05)
                except OSError:
                    alive = False
                    break
            ok(not alive, "timeout: the step's child is ended too")
        # 10. the spec: bad watch config is refused, durations parse, a watch round-trips
        for kw in ({"report": "watch", "compare": "sometimes"}, {"report": "watch", "compare": "above"},
                   {"report": "watch", "compare": "equals"}, {"report": "watch", "cooldown": "soon"},
                   {"report": "watch", "expect": "("}, {"compare": "changed"}, {"report": "bogus"},
                   {"notify": "smtp://me"}, {"notify_after": 0},
                   {"step": [{"id": "a", "command": ["true"]}, {"id": "a", "command": ["true"]}]}):
            try:
                mkjob("bad", **kw)
                ok(False, f"spec: accepted {kw}")
            except ValueError:
                ok(True, "")
        ok(seconds("10m", "") == 600 and seconds("1.5h", "") == 5400 and seconds(90, "") == 90, "spec: durations")
        w = mkjob("rt", command=["x"], report="watch", compare="below", target=0, cooldown="5m", notify="ntfy://t", notify_after=3)
        ok(load_spec(write_spec(d, w))["rt"] == w, "spec: a watch job round-trips through TOML")
        # 11. the first review (codex): each fix has a check that fails without it
        # a. releasing the notify lock does not end the job's leftovers (Windows: before a pulled-in caller runs)
        hooked = []
        BEFORE_UNLOCK.append(lambda lk: hooked.append(lk.dir.name))
        try:
            run(mkjob("leftover", command=[PY, "-c", "pass"], notify="ntfy://t"), 0)
        finally:
            BEFORE_UNLOCK.pop()
        ok("locks" in hooked and "notify" not in hooked, f"review: the notify lock skips the leftover cleanup: {hooked}")
        # b. a child that outlives its shell holds the pipe: the drain after a timeout is bounded
        if os.name != "nt":
            pidf, drain = d / "orphan.pid", DRAIN_S
            globals()["DRAIN_S"] = 0.3
            t0 = time.time()
            try:
                r = run(mkjob("orphan", command=["/bin/sh", "-c", f"sleep 30 & echo $! > {pidf}; exit 0"],
                              report="watch", timeout=0.5), 0)
            finally:
                globals()["DRAIN_S"] = drain
                try:
                    os.kill(int(pidf.read_text()), 9)
                except (OSError, ValueError):
                    pass
            ok(time.time() - t0 < 5 and r["status"] == "failed" and r["steps"][0]["note"] == "timed out after 0.5s",
               f"review: a pipe held by an orphan does not hang the wrapper ({time.time() - t0:.1f}s)")
        # c. a failure alert that was never delivered is told with the recovery; a retried recovery keeps the count
        flag = d / "rev.flag"
        cmd = [PY, "-c", "import os,sys;sys.exit(1 if os.path.exists(sys.argv[1]) else 0)", str(flag)]
        j = mkjob("lost", command=cmd, notify="ntfy://t")
        flag.write_text("")
        fail["err"] = "ntfy down"
        run(j, 0)
        fail["err"] = None
        flag.unlink()
        run(j, 60)
        ok(told("lost") == ["failed: main: failed exit 1; ok again after 1 bad runs"],
           f"review: an undelivered alert is told with the recovery: {told('lost')}")
        j = mkjob("late", command=cmd, notify="ntfy://t")
        flag.write_text("")
        run(j, 0), run(j, 60)
        flag.unlink()
        fail["err"] = "ntfy down"
        run(j, 120)
        fail["err"] = None
        run(j, 180), run(j, 240)
        ok(told("late") == ["failed: main: failed exit 1", "ok again after 2 bad runs"],
           f"review: a retried recovery keeps the count, and is sent once: {told('late')}")
        # d. the flap digest waits for the cooldown, and counts the moves that started the flapping
        _, tick = watcher("flapcool", cooldown="1h", flap_window="10m", flap_max=1)
        tick(0, "A"), tick(60, "B")
        ok(tick(120, "A") == "flapping" and tick(720) == "flapping" and told("flapcool") == ["A -> B"],
           f"review: no digest inside the cooldown: {told('flapcool')}")
        ok(tick(60 + 3600) == "flap digest" and told("flapcool")[1:2] == ["moved 2 times while flapping; was B, now A"],
           f"review: the digest after the cooldown counts the moves: {told('flapcool')}")
        # e. a URL that urllib cannot build is refused at load, and deliver never raises
        for bad_url in ("https://[", "https://", "http:///x"):
            try:
                mkjob("url", notify=bad_url)
                ok(False, f"review: notify {bad_url!r} accepted")
            except ValueError:
                ok(True, "")
        ok(isinstance(real("http://[", "t", "x"), str), "review: deliver returns an error for a bad URL, it does not raise")
        # f. nan and inf are not numbers for above and below
        _, tick = watcher("finite", compare="above", target=10, notify_after=5)  # no failure alerts here
        notes = [tick(60 * i, v) for i, v in enumerate(["12", "nan", "12", "inf", "12"])]
        ok(notes[1] == notes[3] == "failed" and told("finite") == ["12 is above 10"],
           f"review: nan and inf are broken reads, the threshold stays armed: {notes} {told('finite')}")
    finally:
        DELIVER[0] = real


def check_update(ok, tmp):
    """The update job every jobs file gets, settings.notify_token, and `takt update`: it replaces the
    file only when the new one starts and loads this host's jobs file, and it leaves a git checkout."""
    d = tmp / "update"
    d.mkdir()
    spec = d / "jobs.toml"
    old_env = os.environ.pop("TAKT_NO_UPDATE_JOB", None)
    try:
        spec.write_text('[settings]\npython = "/usr/bin/python3"\n\n[job.a]\ncommand = ["true"]\n')
        j = load_spec(spec).get(UPDATE_ID)
        ok(j and j["schedule"] == UPDATE_SCHEDULE and j["steps"][0]["command"][-3:] == ["update", "--spec", str(spec.resolve())]
           and j["steps"][0]["command"][0] == "/usr/bin/python3", f"update: every jobs file gets the update job: {j and j['steps']}")
        spec.write_text('[settings]\nupdate = false\n\n[job.a]\ncommand = ["true"]\n')
        ok(UPDATE_ID not in load_spec(spec), "update: settings.update = false leaves it out")
        spec.write_text('[settings]\nupdate = "30 3 * * *"\n\n[job.a]\ncommand = ["true"]\n')
        ok(load_spec(spec)[UPDATE_ID]["schedule"] == "30 3 * * *", "update: settings.update sets its schedule")
        spec.write_text('[job.takt-update]\nschedule = "0 5 * * *"\ncommand = ["mine"]\n')
        ok(load_spec(spec)[UPDATE_ID]["steps"][0]["command"] == ["mine"], "update: a job of the same id in the file wins")
        spec.write_text('[settings]\nupdate = 5\n\n[job.a]\ncommand = ["true"]\n')
        try:
            load_spec(spec)
            ok(False, "update: settings.update = 5 accepted")
        except ValueError:
            ok(True, "")
    finally:
        if old_env is not None:
            os.environ["TAKT_NO_UPDATE_JOB"] = old_env
    # notify_token: every job gets the path, and deliver sends it as a bearer token, read at send time
    tok = d / "ntfy.token"
    tok.write_text("tk_test123\n")
    spec.write_text(f'[settings]\nnotify_token = {json.dumps(str(tok))}\n\n[job.a]\ncommand = ["true"]\nnotify = "https://n.example/t"\n')
    ok(load_spec(spec)["a"]["notify_token"] == str(tok), "notify_token: the jobs get it from settings")
    seen, real_open = [], urllib.request.urlopen

    class Resp:
        def read(self):
            return b""
    urllib.request.urlopen = lambda req, timeout=None: (seen.append(dict(req.header_items())), Resp())[1]
    try:
        err = deliver("https://n.example/t", "takt h: a", "x", str(tok))
        missing = deliver("https://n.example/t", "takt h: a", "x", str(d / "no-such-token"))
    finally:
        urllib.request.urlopen = real_open
    ok(err is None and seen and seen[0].get("Authorization") == "Bearer tk_test123",
       f"notify_token: sent as a bearer token: {seen}")
    ok(isinstance(missing, str) and len(seen) == 1, "notify_token: a missing token file is a failed send, not a crash")
    # takt update, against a copy of this file and a fake GitHub
    me = Path(__file__).read_bytes()
    spec.write_text('[job.a]\ncommand = ["true"]\n')
    def copy(sub):
        (d / sub).mkdir()
        f = d / sub / "takt.py"
        f.write_bytes(me)
        return f
    quiet = lambda *a: None
    newer = me + b"\n# newer\n"
    f = copy("git")
    (d / "git" / ".git").mkdir()
    ok(update(spec, fetch=lambda u: newer, script=f, say=quiet) == 0 and f.read_bytes() == me,
       "update: a git checkout is left to git")
    f = copy("same")
    ok(update(spec, fetch=lambda u: me, script=f, say=quiet) == 0 and f.read_bytes() == me, "update: the same file is up to date")
    f = copy("down")
    def offline(u):
        raise OSError("no network")
    ok(update(spec, fetch=offline, script=f, say=quiet) == 1 and f.read_bytes() == me, "update: a failed fetch keeps the file")
    f = copy("broken")
    # no jobs file: only the start check stands between a broken download and the installed takt
    ok(update(d / "no-such-jobs.toml", fetch=lambda u: b"raise SystemExit(3)\n", script=f, say=quiet) == 1
       and f.read_bytes() == me, "update: a new file that does not start is not installed")
    rejects = me.replace(b"def load_spec(path) -> dict:\n", b"def load_spec(path) -> dict:\n    raise ValueError('new code rejects this spec')\n", 1)
    f = copy("rejects")
    ok(update(spec, fetch=lambda u: rejects, script=f, say=quiet) == 1 and f.read_bytes() == me,
       "update: a new file that cannot load this jobs file is not installed")
    f = copy("good")
    os.chmod(f, 0o755)
    urls = []
    ok(update(spec, "v9", fetch=lambda u: (urls.append(u), newer)[1], script=f, say=quiet) == 0 and f.read_bytes() == newer
       and (os.name == "nt" or f.stat().st_mode & 0o777 == 0o755) and not list((d / "good").glob(".takt.*")),
       "update: a good new file replaces the old one, keeps its mode, and leaves no temp file")
    ok(urls == [UPDATE_URL.format(ref="v9")], f"update: the ref picks the URL: {urls}")


def check_examples(ok, tmp):
    io = tmp / "init.toml"
    io.write_text(starter_spec())
    spec, py = load_spec(io), tomllib.loads(io.read_text())["settings"]["python"]
    ok(set(spec) == {"hello"} and spec["hello"]["steps"][0]["command"][0] == py,
       "init: the starter spec loads and its job uses the pinned python")
    ok("/Cellar/" not in py, "init: python is the stable path, not a versioned Homebrew Cellar path")
    try:
        init_spec(io)
        ok(False, "init: refuses to overwrite")
    except SystemExit:
        ok(io.read_text() == starter_spec(), "init: refuses to overwrite")
    ex = HERE / "examples"
    if not ex.is_dir():
        return  # an installed or pushed copy: the examples live in the repo
    for p in ex.glob("jobs.*.toml"):
        ok(load_spec(p) and "python" in tomllib.loads(p.read_text())["settings"], f"examples/{p.name}: loads, pins python")
    spec = load_spec(ex / "jobs.toml")
    r, y = spec["token-refresh"], spec["ingest"]
    ok(set(r["lock"]) & set(y["lock"]) and "token-refresh" in y["after"],
       "examples/jobs.toml: the colliding pair shares a lock and ingest is after the refresh")
    ok(all(d["Minute"] == 0 for j in spec.values() for d in
           (lambda v: [v] if isinstance(v, dict) else v)(plistlib.loads(render_launchd(j, CTX))["StartCalendarInterval"])),
       "examples/jobs.toml: every slot is minute 0, no offset to dodge a collision")
    ok([s["id"] for s in y["steps"]] == ["index", "ingest"] and y["steps"][1]["report"] == "json-failed-sources",
       "examples/jobs.toml: the ingest chain reports failed sources from its last step")

if __name__ == "__main__":
    sys.exit(main())

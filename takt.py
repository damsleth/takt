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
    takt --check                               offline self-test, no auth, no network

Docs: https://github.com/damsleth/takt/tree/main/docs
"""
from __future__ import annotations

import argparse
import json
import os
import plistlib
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from itertools import product
from pathlib import Path

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
ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
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


def prev_slot(f, now: datetime, days=32):
    t = now.replace(second=0, microsecond=0)
    for _ in range(days * 1440):
        if matches(f, t):
            return t
        t -= timedelta(minutes=1)
    return None


# ------------------------------------------------------------------- the spec

STEP_KEYS = ("command", "stdout", "stderr", "report")


def _argv(c):
    return ["/bin/sh", "-c", c] if isinstance(c, str) else list(c)


def _norm_step(s, sid):
    return {"id": s.get("id", sid), "command": _argv(s["command"]),
            "stdout": s.get("stdout"), "stderr": s.get("stderr"), "report": s.get("report")}


def normalize(jid, j) -> dict:
    if not ID_RE.match(jid):
        raise ValueError(f"job id {jid!r}: use letters, digits, - and _")
    steps = [_norm_step(s, f"step{i + 1}") for i, s in enumerate(j.get("step", []))]
    if "command" in j:
        steps = [_norm_step(j, "main")]
    if not steps:
        raise ValueError(f"job {jid}: no command or step")
    job = {"id": jid, "schedule": j.get("schedule"), "run_at_load": bool(j.get("run_at_load")),
           "catch_up": j.get("catch_up", "run-once"), "lock": list(j.get("lock", [])),
           "after": list(j.get("after", [])), "needs": list(j.get("needs", [])),
           "wants": list(j.get("wants", [])), "bundle": j.get("bundle"),
           "lock_timeout": int(j.get("lock_timeout", 900)), "replaces": list(j.get("replaces", [])),
           "steps": steps}
    if job["schedule"]:
        parse_cron(job["schedule"])
    if job["catch_up"] not in ("run-once", "skip"):
        raise ValueError(f"job {jid}: catch_up must be run-once or skip")
    return job


def load_spec(path) -> dict:
    raw = tomllib.loads(Path(path).read_text())
    spec = {jid: normalize(jid, j) for jid, j in raw.get("job", {}).items()}
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
    single = len(j["steps"]) == 1 and j["steps"][0]["id"] == "main"
    for s in j["steps"]:
        if not single:
            out += ["", f"[[{base}.step]]", f"id = {toml_val(s['id'])}"]
        for k in STEP_KEYS:
            if s.get(k):
                out.append(f"{k} = {toml_val(s[k])}")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------- state, locks

def state_dir(a=None) -> Path:
    return Path(getattr(a, "state", None) or os.environ.get("TAKT_STATE")
                or Path.home() / ".local/state/takt")


def read_record(state: Path, jid):
    try:
        return json.loads((state / f"{jid}.json").read_text())
    except (OSError, ValueError):
        return None


def write_record(state: Path, rec):
    state.mkdir(parents=True, exist_ok=True)
    tmp = state / f".{rec['id']}.json.tmp"
    tmp.write_text(json.dumps(rec, indent=1) + "\n")
    os.replace(tmp, state / f"{rec['id']}.json")


def _trylock(f):
    if fcntl:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)


class Locks:
    """Named flocks, taken in sorted order so two jobs can never deadlock.
    flock dies with the process, so a crashed job cannot wedge the lock."""

    def __init__(self, state: Path, names, timeout):
        self.dir, self.names, self.timeout, self.fds, self.blocked_on = state / "locks", sorted(names), timeout, [], None

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
        self.waited = round(time.time() - t0, 2)
        return self

    def __exit__(self, *a):
        for f in self.fds:
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


def owa_status():
    cmd = os.environ.get("TAKT_OWA_PIGGY", "owa-piggy")
    try:
        r = subprocess.run([cmd, "status"], capture_output=True, text=True, timeout=30)
        return parse_owa_status(r.stdout)
    except (OSError, subprocess.SubprocessError):
        return None


def check_need(need, cache):
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
            rows.append({"need": n, "hard": hard, "ok": ok, "detail": detail})
    return rows


# ------------------------------------------------------------------------ run

def yaams_failed(out: str):
    """Last JSON line of a step's stdout -> (failed sources, error code)."""
    for line in reversed(out.strip().splitlines()):
        if line.startswith("{"):
            try:
                err = json.loads(line).get("error") or {}
            except ValueError:
                return [], None
            return list(err.get("failed_sources") or []), err.get("code")
    return [], None


def _open_log(path):
    return open(os.path.expanduser(path), "ab") if path else None


def run_step(step):
    t0 = time.time()
    rec = {"id": step["id"], "exit": None, "failed": [], "note": None}
    out_f, err_f = _open_log(step["stdout"]), _open_log(step["stderr"])
    cap = step["report"] == "json-failed-sources"
    try:
        r = subprocess.run(step["command"], stdout=subprocess.PIPE if cap else (out_f or None),
                           stderr=err_f or None, **NO_WINDOW)
        rec["exit"] = r.returncode
        if cap:
            text = r.stdout.decode(errors="replace")
            if out_f:
                out_f.write(r.stdout)
            rec["failed"], code = yaams_failed(text)
            rec["note"] = code
    except OSError as e:
        rec["exit"], rec["note"] = 127, f"{step['command'][0]}: {e.strerror}"
    finally:
        for f in (out_f, err_f):
            if f:
                f.close()
    rec["duration_s"] = round(time.time() - t0, 2)
    rec["status"] = "partial" if rec["failed"] else ("ok" if rec["exit"] == 0 else "failed")
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


def slot_of(job, now):
    f = parse_cron(job["schedule"]) if job["schedule"] else None
    return (prev_slot(f, now) if f else None) or now.replace(second=0, microsecond=0)


def run_job(spec, jid, state: Path, now=None, scheduled=False, dry=False, say=print):
    """Returns the process exit code: 0 ok or deliberately skipped, 1 partial/failed,
    2 preflight skip, 75 lock timeout."""
    now = now or datetime.now()
    job = spec[jid]
    slot = slot_of(job, now)
    trig = "scheduled" if scheduled else "manual"
    if scheduled and job["catch_up"] == "skip" and job["schedule"] and (now - slot).total_seconds() > SKIP_AFTER:
        say(f"{jid}: skipped, missed slot {slot:%F %R} and catch_up=skip")
        return 0
    deps = chain_of(spec, jid)
    names = lock_names(spec, jid)
    pre = preflight(job)
    if dry:
        say(f"{jid} slot {slot:%F %R} trigger {trig}")
        say(f"  locks: {', '.join(sorted(names))}")
        say(f"  after: {', '.join(deps) or '-'} (pulled in only if due this slot and not yet run)")
        for r in pre:
            say(f"  {'needs' if r['hard'] else 'wants'} {r['need']}: {'ok' if r['ok'] else 'MISSING'}: {r['detail']}")
        for s in job["steps"]:
            say(f"  step {s['id']}: {shlex.join(s['command'])[:110]}")
        return 0
    try:
        locks = Locks(state, names, job["lock_timeout"]).__enter__()
    except TimeoutError as e:
        rec = {"id": jid, "slot": slot.isoformat(), "trigger": trig, "pulled_by": None,
               "started": datetime.now().isoformat(timespec="seconds"), "duration_s": 0,
               "status": "lock-timeout", "exit": 75, "steps": [], "preflight": pre,
               "note": f"lock {e} held for more than {job['lock_timeout']}s"}
        write_record(state, rec)
        say(f"{jid}: lock-timeout ({rec['note']})")
        return 75
    try:
        last = read_record(state, jid)  # re-read: a job holding the lock may have pulled us in
        if scheduled and last and last.get("pulled_by") and last.get("slot") == slot.isoformat():
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
            if dslot and dslot >= slot and not (lr and lr.get("slot") == dslot.isoformat()):
                order.append((d, dslot))
        for d, dslot in order:
            code = _run_locked(spec[d], dslot, "pulled", jid, state, say)
            say(f"{jid}: pulled in {d} first (exit {code})")
        rec_code = _run_locked(job, slot, trig, None, state, say, waited=locks.waited,
                               blocked_on=locks.blocked_on, pulled=[d for d, _ in order], pre=pre)
    finally:
        locks.__exit__()
    return rec_code


def _run_locked(job, slot, trig, pulled_by, state, say, waited=0, blocked_on=None, pulled=(), pre=None):
    pre = preflight(job) if pre is None else pre
    failed = [r for r in pre if r["hard"] and not r["ok"]]
    if failed:
        rec = {"id": job["id"], "slot": slot.isoformat(), "trigger": trig, "pulled_by": pulled_by,
               "started": datetime.now().isoformat(timespec="seconds"), "duration_s": 0,
               "status": "skipped", "exit": 2, "steps": [], "preflight": pre,
               "note": "skipped: " + "; ".join(f"needs {r['need']} ({r['detail']})" for r in failed)}
        write_record(state, rec)
        say(f"{job['id']}: {rec['note']}")
        return 2
    rec = _execute(job, slot, trig, pulled_by, pre)
    rec["waited_s"], rec["blocked_on"], rec["pulled_in"] = waited, blocked_on, list(pulled)
    write_record(state, rec)
    say(f"{job['id']}: {rec['status']} in {rec['duration_s']}s (waited {waited}s)")
    return rec["exit"]


# -------------------------------------------------------------------- backends

def sd_quote(a):
    return f'"{a}"' if re.search(r"\s", a) else a


def wrapper_argv(ctx, jid):
    return [ctx["python"], ctx["script"], "run", jid, "--spec", ctx["spec"], "--scheduled"]


def launchd_schedule(f):
    """cron fields -> ('interval', 60) | ('cal', [dicts])."""
    if all(v is None for v in f):
        return ("interval", 60)
    keys = [("Month", f[3]), ("Day", f[2]), ("Weekday", f[4]), ("Hour", f[1]), ("Minute", f[0])]
    keys = [(k, v) for k, v in keys if v is not None]
    return ("cal", [dict(zip([k for k, _ in keys], combo)) for combo in product(*[v for _, v in keys])])


def render_launchd(job, ctx) -> bytes:
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
        if len(dicts) != len(list(product(*sets.values()))):
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
    """-> (service text, timer text)."""
    unit = f"takt-{job['id']}"
    argv = " ".join(sd_quote(a) for a in wrapper_argv(ctx, job["id"]))
    service = (f"[Unit]\nDescription=takt job {job['id']}\n\n[Service]\nType=oneshot\n"
               f"Environment=PATH={ctx['path']}\nExecStart={argv}\n")
    timer = [f"[Unit]\nDescription=takt timer {job['id']}\n\n[Timer]"]
    if job["schedule"]:
        f = fields_to_cron(parse_cron(job["schedule"])).split()
        dow = ""
        if f[4] != "*":
            names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
            dow = ",".join(names[int(x)] for x in f[4].split(",")) + " "
        timer.append(f"OnCalendar={dow}*-{sd_field(f[3], 1, 1)}-{sd_field(f[2], 1, 1)} "
                     f"{sd_field(f[1], 0, 2)}:{sd_field(f[0], 0, 2)}:00")
    if job["run_at_load"]:
        timer.append("OnActiveSec=5s")
    if job["catch_up"] == "run-once":
        timer.append("Persistent=true")
    timer.append(f"Unit={unit}.service\n\n[Install]\nWantedBy=timers.target\n")
    return service, "\n".join(timer)


TS_NS = "http://schemas.microsoft.com/windows/2004/02/mit/task"
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
        if len(mi) > 1 and mi[0] == 0 and mi == list(range(0, 60, mi[1])):
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
            if b == "launchd":
                files = {f"{PREFIX}.{j['id']}.plist": render_launchd(j, ctx)}
            elif b == "systemd":
                svc, tmr = render_systemd(j, ctx)
                files = {f"takt-{j['id']}.service": svc, f"takt-{j['id']}.timer": tmr}
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


def split_steps(cmd):
    """Shell line -> steps, when it is only argv, `;`, `>>`/`>` and `2>>`/`2>`.
    Anything fancier becomes one `sh -c` step with the text kept verbatim."""
    lex = shlex.shlex(cmd, posix=True, punctuation_chars=True)
    lex.whitespace_split = True
    toks = list(lex)
    steps, argv, out, err = [], [], None, None
    i = 0
    while i < len(toks):
        t = toks[i]
        if t in (">>", ">"):
            if i + 1 >= len(toks):
                raise ValueError("dangling redirect")
            if argv and argv[-1] == "2":
                argv.pop()
                err = toks[i + 1]
            else:
                out = toks[i + 1]
            i += 2
            continue
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
        if not s or re.match(r"^[A-Z_]+=", s):
            continue
        if s.startswith("#"):
            if cron_like(s.lstrip("#").strip()):
                skipped.append(i + 1)
            continue
        parts = s.split(None, 1) if s.startswith("@") else s.split(None, 5)
        sched, cmd = (parts[0], parts[1]) if s.startswith("@") else (" ".join(parts[:5]), parts[5])
        sched = ALIASES.get(sched, sched)
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
        try:
            raw_steps = split_steps(cmd)
        except ValueError:
            raw_steps = [(["/bin/sh", "-c", re.sub(r"\s#\s*[\w.-]+\s*$", "", cmd)], None, None)]
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


def plan_install(spec, ctx, agents_dir: Path, crontab_text: str, be="launchd"):
    """Ordered actions. Nothing here touches the machine."""
    acts = [("mkdir", Path(ctx["state"]) / "log", None)]  # launchd will not create a log dir
    uid = _uid()
    # Retire the old schedulers first: run_at_load fires a reseed the moment a new plist is
    # bootstrapped, and it must not meet a still-armed old job on the same Edge dir.
    new_cron, cron_changed = crontab_text.splitlines(), False
    for j in spec.values():
        for rep in j["replaces"]:
            kind, _, arg = rep.partition(":")
            if kind == "launchd" and be == "launchd":
                old = agents_dir / f"{arg}.plist"
                acts.append(("run", ["launchctl", "bootout", f"gui/{uid}/{arg}"], None))
                acts.append(("move", old, Path(ctx["state"]) / "retired" / old.name))
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
            p = agents_dir / f"{PREFIX}.{jid}.plist"
            acts.append(("write", p, render_launchd(j, ctx)))
            acts.append(("run", ["launchctl", "bootstrap", f"gui/{uid}", str(p)], None))
        elif be == "systemd":
            svc, tmr = render_systemd(j, ctx)
            acts.append(("write", agents_dir / f"takt-{jid}.service", svc.encode()))
            acts.append(("write", agents_dir / f"takt-{jid}.timer", tmr.encode()))
        else:
            x = agents_dir / f"takt-{jid}.xml"
            acts.append(("write", x, render_schtasks(j, ctx).encode("utf-16")))  # schtasks wants UTF-16
            acts.append(("run", ["schtasks", "/Create", "/TN", task_name(jid), "/XML", str(x), "/F"], None))
    if be == "systemd":
        acts.append(("run", ["systemctl", "--user", "daemon-reload"], None))
        acts += [("run", ["systemctl", "--user", "enable", "--now", f"takt-{jid}.timer"], None) for jid in spec]
    return acts


def plan_uninstall(ids, agents_dir: Path, be):
    """Stop and remove what install registered. ponytail: does not un-retire `replaces`;
    the retired plist and crontab.bak are kept under state for doing that by hand."""
    acts = []
    for jid in ids:
        if be == "launchd":
            acts.append(("run", ["launchctl", "bootout", f"gui/{_uid()}/{PREFIX}.{jid}"], None))
            acts.append(("rm", agents_dir / f"{PREFIX}.{jid}.plist", None))
        elif be == "systemd":
            acts.append(("run", ["systemctl", "--user", "disable", "--now", f"takt-{jid}.timer"], None))
            acts += [("rm", agents_dir / f"takt-{jid}.{x}", None) for x in ("timer", "service")]
        else:
            acts.append(("run", ["schtasks", "/Delete", "/TN", task_name(jid), "/F"], None))
            acts.append(("rm", agents_dir / f"takt-{jid}.xml", None))
    if be == "systemd":
        acts.append(("run", ["systemctl", "--user", "daemon-reload"], None))
    return acts


def describe(act):
    kind, a, b = act
    if kind == "write":
        return f"write {a} ({len(b)} bytes)"
    if kind in ("mkdir", "rm"):
        return f"{kind} {a}"
    if kind == "move":
        return f"move {a} -> {b}"
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
    # State is an enum name (Ready/Running/Disabled), not localized text like schtasks /Query
    return ["powershell", "-NoProfile", "-Command", "Get-ScheduledTask -TaskPath '\\takt\\' "
            "-ErrorAction SilentlyContinue | ForEach-Object { $_.TaskName + '|' + $_.State }"]


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


def native_states(spec, be, agents_dir: Path):
    try:
        r = subprocess.run(native_query(be, list(spec)), capture_output=True, text=True, timeout=30, **NO_WINDOW)
        return parse_native(be, r.stdout, list(spec), agents_dir)
    except (OSError, subprocess.SubprocessError):
        return {}


def next_slot(f, now: datetime, days=32):
    t = now.replace(second=0, microsecond=0)
    for _ in range(days * 1440):
        t += timedelta(minutes=1)
        if matches(f, t):
            return t
    return None


def detail_of(r):
    if not r:
        return ""
    detail = []
    if r.get("note") and r["status"] in ("skipped", "lock-timeout"):
        detail.append(r["note"])
    for s in r["steps"]:
        if s["status"] != "ok":
            detail.append(f"{s['id']}: {s['status']} exit {s['exit']}"
                          + (f" failed sources: {', '.join(s['failed'])}" if s["failed"] else ""))
    for p in r.get("preflight", []):
        if not p["ok"] and not p["hard"]:
            detail.append(f"warn {p['need']}: {p['detail']}")
    if r.get("waited_s", 0) >= 1:
        detail.append(f"waited {r['waited_s']}s on lock {r.get('blocked_on')}")
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


def show(spec, state, jid):
    """Last record of one job and the tail of its logs: the TUI preview."""
    if jid not in spec:
        print(f"no job {jid!r} here")
        return 1
    r = read_record(state, jid)
    if not r:
        print(f"{jid}: never run")
    else:
        print(f"{jid}  {r['status']}  started {r['started']}  {r['duration_s']}s  slot {r['slot']}  {r['trigger']}"
              + (f" by {r['pulled_by']}" if r.get("pulled_by") else ""))
        if r.get("note"):
            print(f"  note: {r['note']}")
        for s in r["steps"]:
            print(f"  step {s['id']:14} {s['status']:8} exit {s['exit']}  {s['duration_s']}s"
                  + (f"  failed: {', '.join(s['failed'])}" if s["failed"] else ""))
        for p in r.get("preflight", []):
            print(f"  {'needs' if p['hard'] else 'wants'} {p['need']:12} {'ok' if p['ok'] else 'MISSING'}  {p['detail']}")
    logs = [state / "log" / f"{jid}.log"] + [Path(os.path.expanduser(s[k])) for s in spec[jid]["steps"]
                                              for k in ("stdout", "stderr") if s[k] and s[k] != "/dev/null"]
    for p in dict.fromkeys(logs):
        if p.is_file():
            print(f"\n== {p}")
            print("\n".join(p.read_text(errors="replace").splitlines()[-8:]))
    return 0


# -------------------------------------------------------------------- remote hosts

REMOTE_DIR = ".local/share/takt"  # relative to the ssh login dir, the same on sh and pwsh
SAFE_ARG = re.compile(r"[\w./:\\=,@+-]+")


def hosts():
    """`local` plus every host with a `jobs.<host>.toml` in the config dir."""
    return ["local"] + sorted(p.name.split(".")[1] for p in CONFIG.glob("jobs.*.toml"))


def host_settings(host):
    p = CONFIG / f"jobs.{host}.toml"
    if not p.exists():
        sys.exit(f"no spec for host {host!r}: create {p}")
    return tomllib.loads(p.read_text()).get("settings", {})


def remote_argv(host, args, cfg=None):
    """ssh argv that runs takt on `host` with `args`. The login shell may be sh or pwsh and
    ssh joins with spaces, so nothing is quoted: every token must be a plain word."""
    cfg = host_settings(host) if cfg is None else cfg
    cmd = [cfg.get("python", "python3"), f"{REMOTE_DIR}/takt.py", *args]
    bad = [x for x in cmd if not SAFE_ARG.fullmatch(x)]
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
        return subprocess.run(["scp", "-q", "-r", str(Path(t) / ".local"), str(Path(t) / ".config"),
                               f"{host}:"]).returncode


def on_host(host, rest):
    """`takt --host H <cmd> ...`: H's own copy of takt runs the command, so units are rendered
    with H's paths and the scheduler is H's. install also pushes; push is gated like install."""
    host_settings(host)
    cmd, writes = (rest[0] if rest else ""), "--allow-writes" in rest
    if cmd in ("push", "install"):
        if not writes:
            print(f"PLAN push {Path(__file__).name} to {host}:{REMOTE_DIR}/ and jobs.{host}.toml to "
                  f"{host}:.config/takt/jobs.toml", flush=True)
        elif push(host):
            return 1
        if cmd == "push":
            print("" if writes else "\nnothing written. Re-run with --allow-writes.")
            return 0
    return subprocess.run(remote_argv(host, rest)).returncode


def remote_rows(host):
    down = lambda why: [{"host": host, "id": "-", "sched": "-", "status": "unreachable", "last": None,
                         "next": None, "detail": why}]
    try:
        r = subprocess.run(remote_argv(host, ["status", "--json"]), capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as e:
        return down(str(e))
    try:
        rows = json.loads(r.stdout)
    except ValueError:
        return down((r.stderr.strip().splitlines() or [f"no takt there? takt --host {host} push --allow-writes"])[-1])
    for x in rows:
        x["host"] = host
    return rows


def ui(allow_writes):
    """fzf over every host's status: preview = `show`, keys run the admin verbs on that host."""
    if not shutil.which("fzf"):
        sys.exit("takt ui needs fzf")
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
    out += ["", "# Every 15 minutes, append a timestamp to a log. Replace it with a real job.",
            "[job.hello]", 'schedule = "*/15 * * * *"',
            f"command = {toml_val([py, '-c', 'import datetime; print(datetime.datetime.now().isoformat())'])}",
            'stdout = "~/.local/state/takt/hello.log"', ""]
    return "\n".join(out)


def init_spec(path: Path):
    if path.exists():
        sys.exit(f"{path} exists. Edit it, or remove it first.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(starter_spec())
    print(f"wrote {path}\nnext: takt install (shows the plan), then takt install --allow-writes")
    return 0


def make_ctx(a):
    """Render context. `[settings] path/python` pin what the scheduler gets; a captured shell PATH
    can carry throwaway entries (fnm multishells), so the spec should pin it."""
    cfg = tomllib.loads(Path(a.spec).read_text()).get("settings", {})
    return {"python": cfg.get("python") or os.path.realpath(sys.executable),
            "script": str(Path(__file__).resolve()), "spec": str(Path(a.spec).resolve()),
            "path": cfg.get("path") or os.environ.get("PATH", DEFAULT_PATH), "state": str(state_dir(a))}


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["--host"] and len(argv) > 1:
        if argv[1] != "local":
            return on_host(argv[1], argv[2:])
        argv = argv[2:]
    if not argv and sys.stdin.isatty() and sys.stdout.isatty():
        return ui(False)
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
    st.add_argument("-A", "--all-hosts", action="store_true", help="this machine and every jobs.<host>.toml host")
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
    sub.add_parser("push", help="with --host H: copy takt.py and jobs.H.toml to H")
    common(sub.add_parser("init", help="write a starter jobs.toml (never overwrites)"))
    a = ap.parse_args(argv)

    if a.check:
        return self_check()
    if a.cmd == "ui":
        return ui(a.allow_writes)
    if a.cmd == "push":
        sys.exit("push needs a host: takt --host <host> push --allow-writes")
    if a.cmd == "init":
        return init_spec(Path(a.spec))
    if a.cmd == "run":
        spec = load_spec(a.spec)
        if a.id not in spec:
            sys.exit(f"unknown job {a.id!r}; have: {', '.join(spec)}")
        now = datetime.fromisoformat(a.now) if a.now else None
        return run_job(spec, a.id, state_dir(a), now, a.scheduled, a.dry_run)
    be = backend()
    adir = Path(getattr(a, "agents_dir", None) or default_dir(be, state_dir(a)))
    if a.cmd == "status":
        spec = load_spec(a.spec) if Path(a.spec).exists() else {}  # a controller may only have hosts
        rows = status_rows(spec, state_dir(a), native_states(spec, be, adir))
        if a.all_hosts:
            with ThreadPoolExecutor() as ex:
                rows += [x for rs in ex.map(remote_rows, hosts()[1:]) for x in rs]
        if a.json:
            print(json.dumps(rows, indent=1))
        else:
            print("\n".join(fmt_rows(rows)))
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
        for c in cmds:
            p = subprocess.run(c, capture_output=True, text=True, **NO_WINDOW)
            if p.returncode:
                print(f"{a.id}: {a.cmd} failed: {(p.stderr or p.stdout).strip() or p.returncode}")
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
        text = Path(a.file).read_text() if a.file else subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True).stdout
        jobs, skipped, warns = import_cron(text)
        for w in warns:
            print(f"# warning: {w}", file=sys.stderr)
        print(f"# {len(jobs)} live jobs; commented-out cron lines ignored: {len(skipped)}", file=sys.stderr)
        print("\n".join(toml_job(j) for j in jobs))
        return 0
    if a.cmd in ("install", "uninstall"):
        spec = load_spec(a.spec)
        if a.cmd == "install":
            cron = "" if be == "schtasks" else subprocess.run(["crontab", "-l"], capture_output=True, text=True).stdout
            acts, check = plan_install(spec, make_ctx(a), adir, cron, be), True
        else:
            ids = a.ids or list(spec)
            acts, check = plan_uninstall(ids, adir, be), False  # stopping what is not running is fine
        for x in acts:
            print(("DO   " if a.allow_writes else "PLAN ") + describe(x))
        if not a.allow_writes:
            print("\nnothing written. Re-run with --allow-writes to perform the plan above.")
            return 0
        apply_plan(acts, lambda argv_, inp: subprocess.run(argv_, input=inp, check=check, **NO_WINDOW))
        return 0
    ap.print_help()
    return 0


# ----------------------------------------------------------------------- check

def self_check():
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
        check_examples(ok, tmp)
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
                                 "--spec", "/opt/takt/jobs.toml", "--scheduled"], "launchd runs through the wrapper")
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
               "yaams-ingest --spec /opt/takt/jobs.toml --scheduled\n"), "systemd service golden")
    ok(tmr == ("[Unit]\nDescription=takt timer yaams-ingest\n\n[Timer]\nOnCalendar=*-*-* 00/2:00:00\n"
               "Persistent=true\nUnit=takt-yaams-ingest.service\n\n[Install]\nWantedBy=timers.target\n"),
       "systemd timer golden (0 */2)")
    ok("OnCalendar=*-*-* *:00/15:00" in render_systemd(mkjob("q", schedule="*/15 * * * *"), c)[1], "systemd */15")
    t2 = render_systemd(mkjob("w", schedule="30 6 * * 1,2", catch_up="skip", run_at_load=True), c)[1]
    ok("OnCalendar=Mon,Tue *-*-* 06:30:00" in t2 and "Persistent" not in t2 and "OnActiveSec=5s" in t2,
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


SCHTASKS_GOLDEN = '<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task" version="1.2">\n  <RegistrationInfo>\n    <Description>takt job q</Description>\n  </RegistrationInfo>\n  <Triggers>\n    <CalendarTrigger>\n      <Repetition>\n        <Interval>PT15M</Interval>\n        <Duration>P1D</Duration>\n        <StopAtDurationEnd>false</StopAtDurationEnd>\n      </Repetition>\n      <StartBoundary>2026-01-01T00:00:00</StartBoundary>\n      <Enabled>true</Enabled>\n      <ScheduleByDay>\n        <DaysInterval>1</DaysInterval>\n      </ScheduleByDay>\n    </CalendarTrigger>\n  </Triggers>\n  <Settings>\n    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n    <StartWhenAvailable>false</StartWhenAvailable>\n    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n  </Settings>\n  <Actions>\n    <Exec>\n      <Command>/usr/bin/python3</Command>\n      <Arguments>/opt/takt/takt.py run q --spec /opt/takt/jobs.toml --scheduled</Arguments>\n    </Exec>\n  </Actions>\n</Task>\n'

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
    ok((d / "st/log").is_dir() and [c[0][:2] for c in calls] == [["launchctl", "bootout"], ["crontab", "-"], ["launchctl", "bootstrap"]],
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
       + [["systemctl", "--user", "enable", "--now", f"takt-{j}.timer"] for j in "ab"],
       "install systemd: units written, daemon-reload before enable --now")
    calls = []
    apply_plan(plan_uninstall(["a"], ud, "systemd"), lambda a, i: calls.append(a))
    ok(calls == [["systemctl", "--user", "disable", "--now", "takt-a.timer"], ["systemctl", "--user", "daemon-reload"]]
       and not (ud / "takt-a.timer").exists() and (ud / "takt-b.timer").exists(), "uninstall systemd: stops, removes only that job")
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

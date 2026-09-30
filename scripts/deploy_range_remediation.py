"""Root-only immutable release transaction with compatible timed recovery.

Build from a clean committed tree. Cutover preserves risk policy and funded
range mode; recovery uses the corrected code with ALL entry admissions blocked.
Neither path restores an old database. Acceptance checks readiness before
and after an encrypted backup and disarms recovery last.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1]
TRADER_UNIT = Path("/etc/systemd/system/lnmbot.service")
DASHBOARD_UNIT = Path("/etc/systemd/system/lnmbot-dashboard.service")
ENV = Path("/etc/lnmbot/trader.env")
DB = Path("/var/lib/lnmbot/lnmarkets.sqlite")
STATE = Path("/root/.lnmbot-remediation-checkpoint")
TIMER = "lnmbot-remediation-recovery"
SEEDS = {
    "STRATEGY_BREAKOUT_SEED_DAILY_PATH": "lnmarkets_btc_1d_2019-09-09_2026-09-13.parquet",
    "STRATEGY_BREAKOUT_SEED_CAMPAIGN_PATH": "btc-close-range-lnm-live-seed-2026-09-13.json",
    "STRATEGY_RANGE_SEED_DAILY_PATH": "lnmarkets_btc_1d_2019-09-09_2026-09-13.parquet",
}


def run(*args, capture=False, cwd=None, env=None):
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True)
    if result.returncode:
        # Commands may parse protected configuration. Never echo their output.
        raise RuntimeError(
            f"{Path(args[0]).name} failed (exit {result.returncode}); checkpoint retained"
        )
    return result.stdout.strip() if capture else None


def git(*args):
    return run("git", "-c", f"safe.directory={SOURCE}", "-C", str(SOURCE), *args, capture=True)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def current(unit):
    for line in unit.read_text().splitlines():
        if line.startswith("WorkingDirectory="):
            return line.split("=", 1)[1].rstrip("/")
    raise ValueError("unit lacks immutable working directory")


def normalize_env(text, release, *, recovery=False):
    """Repoint by role, validating byte identity instead of a prior-release name."""
    lines = []
    seen = set()
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if key == "LIVE_ENTRIES_ENABLED":
            continue
        if key in SEEDS and sep:
            original = Path(value.strip().strip("\"'"))
            target = (
                release
                / "config/seeds"
                / (original.name if key == "STRATEGY_RANGE_SEED_DAILY_PATH" else SEEDS[key])
            )
            if not original.is_file() or not target.is_file() or digest(original) != digest(target):
                raise ValueError(f"{key}: candidate seed differs from accepted input")
            line = f"{key}={target}"
            seen.add(key)
        lines.append(line)
    if seen != set(SEEDS):
        raise ValueError("missing seed role in protected configuration")
    ref = json.loads(
        (release / "config/seeds/btc-close-range-lnm-paper-reference-2026-09-13.json").read_text()
    )
    if (
        digest(release / "config/seeds" / SEEDS["STRATEGY_BREAKOUT_SEED_DAILY_PATH"])
        != ref["candles_sha256"]
    ):
        raise ValueError("daily historical reference hash differs")
    if (
        digest(release / "config/seeds" / SEEDS["STRATEGY_BREAKOUT_SEED_CAMPAIGN_PATH"])
        != ref["seed_sha256"]
    ):
        raise ValueError("campaign historical reference hash differs")
    lines.append(f"LIVE_ENTRIES_ENABLED={'false' if recovery else 'true'}")
    return "\n".join(lines) + "\n"


def install(source, target, mode="0644", group="root"):
    run("install", "-o", "root", "-g", group, "-m", mode, str(source), str(target))


def backup():
    run("systemctl", "start", "lnmbot-backup.service")
    if (
        run("systemctl", "show", "lnmbot-backup.service", "-p", "Result", "--value", capture=True)
        != "success"
    ):
        raise RuntimeError("encrypted backup failed")


def require_readiness(checkpoint):
    for service, path in [
        ("lnmbot", checkpoint["trader"]),
        ("lnmbot-dashboard", checkpoint["dashboard"]),
    ]:
        run("systemctl", "is-active", "--quiet", service + ".service")
        unit = TRADER_UNIT if service == "lnmbot" else DASHBOARD_UNIT
        if current(unit) != path:
            raise RuntimeError("service release differs from candidate")
        if (
            run(
                "systemctl",
                "show",
                service + ".service",
                "-p",
                "NRestarts",
                "--value",
                capture=True,
            )
            != "0"
        ):
            raise RuntimeError("service restarted during acceptance")
    with urllib.request.urlopen("http://127.0.0.1:8082/readyz", timeout=45) as response:
        report = json.load(response)
    if (
        not report.get("ready")
        or not report.get("entries_enabled")
        or set(report["owners"])
        != {"ma_cross_primary", "btc_close_range_v1", "btc_impulse_range_v1"}
    ):
        raise RuntimeError("trader readiness assertions failed")
    with urllib.request.urlopen("http://127.0.0.1:8082/signals?tf=range", timeout=45) as response:
        page = response.read().decode()
    if "tf=range" not in page or "Range" not in page:
        raise RuntimeError("dashboard range route assertion failed")
    return report


def accept(checkpoint_dir, *, readiness=require_readiness, backup_fn=backup, disarm=None):
    if disarm is None:
        run("systemctl", "is-active", "--quiet", TIMER + ".timer")
    checkpoint = json.loads((checkpoint_dir / "candidate.json").read_text())
    before = readiness(checkpoint)
    if disarm is None:
        run("systemctl", "start", "lnmbot-readiness.service")
        run("systemctl", "is-active", "--quiet", "lnmbot-readiness.timer")
    backup_fn()
    after = readiness(checkpoint)
    (checkpoint_dir / "acceptance.json").write_text(
        json.dumps({"before": before, "after": after}, indent=2)
    )
    if disarm is None:
        run("systemctl", "stop", TIMER + ".timer")
    else:
        disarm()
    return after


def build(tag):
    commit = git("rev-parse", "HEAD")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise ValueError("commit reviewed changes before building")
    if not tag.endswith(commit[:12]):
        raise ValueError("tag must end with the 12-character source commit")
    with tempfile.TemporaryDirectory(prefix="lnmbot-release-") as tmp:
        work = Path(tmp)
        archive = work / "source.tar"
        run(
            "git",
            "-c",
            f"safe.directory={SOURCE}",
            "-C",
            str(SOURCE),
            "archive",
            "-o",
            str(archive),
            commit,
        )
        paths = [
            Path("/usr/local/lib/lnmbot") / tag,
            Path("/usr/local/lib/lnmbot-dashboard") / tag,
            Path("/usr/local/lib/lnmbot") / (tag + "-recovery"),
        ]
        for dest in paths:
            dest.mkdir(mode=0o755, exist_ok=False)
            with tarfile.open(archive) as tar:
                members = [
                    m
                    for m in tar.getmembers()
                    if m.name.split("/")[0]
                    in {"src", "scripts", "config", "pyproject.toml", "uv.lock", "README.md"}
                ]
                tar.extractall(dest, members=members, filter="data")
            build_env = os.environ | {
                "UV_PYTHON": "/usr/bin/python3",
                "UV_PYTHON_DOWNLOADS": "never",
                "UV_CACHE_DIR": str(work / "uv-cache"),
            }
            run(
                "/home/james/.local/bin/uv",
                "sync",
                "--frozen",
                "--no-dev",
                "--no-editable",
                "-q",
                cwd=dest,
                env=build_env,
            )
            run("chown", "-R", "root:root", str(dest))
            run("chmod", "-R", "u=rwX,go=rX", str(dest))
            run("chmod", "-R", "a-w", str(dest))
            manifest = {
                str(p.relative_to(dest)): digest(p)
                for p in dest.rglob("*")
                if p.is_file() and ".venv" not in p.parts and "__pycache__" not in p.parts
            }
            (work / (dest.parent.name + "-" + dest.name + ".json")).write_text(
                json.dumps(manifest, sort_keys=True)
            )
        checkpoint = Path("/data/security-backups") / (
            "lnmbot-remediation-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        )
        checkpoint.mkdir(mode=0o700)
        shutil.copyfile(archive, checkpoint / "source.tar")
        run(
            "git",
            "-c",
            f"safe.directory={SOURCE}",
            "-C",
            str(SOURCE),
            "bundle",
            "create",
            str(checkpoint / "source.bundle"),
            "HEAD",
        )
        for p in work.glob("*.json"):
            shutil.copyfile(p, checkpoint / p.name)
        (checkpoint / "candidate.json").write_text(
            json.dumps(
                dict(
                    commit=commit,
                    archive_sha256=digest(archive),
                    trader=str(paths[0]),
                    dashboard=str(paths[1]),
                    recovery=str(paths[2]),
                ),
                indent=2,
            )
        )
        STATE.write_text(str(checkpoint))
    print(f"Built candidate and compatible recovery; checkpoint {checkpoint}")


def verify_runtime(checkpoint, candidate):
    for role in ("trader", "dashboard", "recovery"):
        dest = Path(candidate[role])
        manifest = json.loads(
            (checkpoint / (dest.parent.name + "-" + dest.name + ".json")).read_text()
        )
        for name, expected in manifest.items():
            if digest(dest / name) != expected:
                raise ValueError("runtime manifest differs")
        for item in (dest, *dest.rglob("*")):
            if not item.is_symlink() and (item.stat().st_uid != 0 or item.stat().st_mode & 0o222):
                raise ValueError("runtime ownership or immutability differs")


def cutover(checkpoint):
    candidate = json.loads((checkpoint / "candidate.json").read_text())
    verify_runtime(checkpoint, candidate)
    trader, dashboard, recovery = (Path(candidate[k]) for k in ("trader", "dashboard", "recovery"))
    for unit in (TRADER_UNIT, DASHBOARD_UNIT):
        shutil.copyfile(unit, checkpoint / (unit.name + ".before"))
    shutil.copyfile(ENV, checkpoint / "trader.env.before")
    with sqlite3.connect(DB) as source, sqlite3.connect(checkpoint / "state.before.sqlite") as copy:
        source.backup(copy)
        if copy.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("checkpoint database integrity failed")
    for label, dest, is_recovery in [("candidate", trader, False), ("recovery", recovery, True)]:
        (checkpoint / f"trader.env.{label}").write_text(
            normalize_env(ENV.read_text(), dest, recovery=is_recovery)
        )
        unit_text = TRADER_UNIT.read_text().replace(current(TRADER_UNIT), str(dest))
        (checkpoint / f"lnmbot.service.{label}").write_text(unit_text)
    (checkpoint / "lnmbot-dashboard.service.candidate").write_text(
        DASHBOARD_UNIT.read_text().replace(current(DASHBOARD_UNIT), str(dashboard))
    )
    verify = checkpoint / "verify"
    verify.mkdir()
    shutil.copyfile(checkpoint / "lnmbot.service.candidate", verify / "lnmbot.service")
    shutil.copyfile(
        checkpoint / "lnmbot-dashboard.service.candidate", verify / "lnmbot-dashboard.service"
    )
    run(
        "systemd-analyze",
        "verify",
        str(verify / "lnmbot.service"),
        str(verify / "lnmbot-dashboard.service"),
    )
    recovery_script = checkpoint / "recover.sh"
    recovery_script.write_text(f"""#!/bin/bash
set -euo pipefail
# Compatible code only, current database, all entries blocked; owned exits remain managed.
install -o root -g lnmbot -m 0640 '{checkpoint}/trader.env.recovery' '{ENV}'
install -o root -g root -m 0644 '{checkpoint}/lnmbot.service.recovery' '{TRADER_UNIT}'
install -o root -g root -m 0644 '{checkpoint}/lnmbot-dashboard.service.candidate' '{DASHBOARD_UNIT}'
systemctl daemon-reload
systemctl restart lnmbot.service lnmbot-dashboard.service
""")
    recovery_script.chmod(0o700)
    backup()
    # Arm and verify before the first live configuration/unit mutation.
    run("systemd-run", "--unit=" + TIMER, "--on-active=30m", str(recovery_script))
    run("systemctl", "is-active", "--quiet", TIMER + ".timer")
    install(checkpoint / "trader.env.candidate", ENV, "0640", "lnmbot")
    install(checkpoint / "lnmbot.service.candidate", TRADER_UNIT)
    install(checkpoint / "lnmbot-dashboard.service.candidate", DASHBOARD_UNIT)
    readiness_unit = checkpoint / "lnmbot-readiness.service"
    readiness_unit.write_text(
        (dashboard / "config/systemd/lnmbot-readiness.service")
        .read_text()
        .replace("@DASHBOARD_RELEASE@", str(dashboard))
    )
    install(readiness_unit, Path("/etc/systemd/system/lnmbot-readiness.service"))
    install(
        dashboard / "config/systemd/lnmbot-readiness.timer",
        Path("/etc/systemd/system/lnmbot-readiness.timer"),
    )
    run("systemctl", "daemon-reload")
    run("systemctl", "restart", "lnmbot.service", "lnmbot-dashboard.service")
    run("systemctl", "enable", "--now", "lnmbot-readiness.timer")
    print("Cutover complete; compatible recovery remains armed until checked acceptance.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["build", "cutover", "accept"])
    parser.add_argument("--tag")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("run as root using the accepted sudo procedure")
    os.umask(0o077)
    if args.phase == "build":
        if not args.tag:
            parser.error("--tag required")
        build(args.tag)
    else:
        checkpoint = Path(STATE.read_text().strip())
        if args.phase == "cutover":
            cutover(checkpoint)
        else:
            report = accept(checkpoint)
            print(json.dumps(report))


if __name__ == "__main__":
    main()

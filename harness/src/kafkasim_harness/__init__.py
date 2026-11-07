"""kafkasim harness: real Apache Kafka in Docker ⇄ simulator calibration."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCENARIOS = ROOT / "contracts" / "scenarios"

# scenario slug → signature keys that must match (others are reported, not gating)
CALIBRATED: dict[str, list[str]] = {
    "single-cluster-broker-failure": ["lost_acked_records", "leadership_left_failed_node", "isr_complete_at_end", "writes_resumed"],
    "min-insync-replicas-not-enough-replicas": ["lost_acked_records", "not_enough_replicas_seen", "writes_resumed"],
    "zombie-leader-sigstop": ["leadership_left_failed_node"],
    "unclean-leader-election": [],
}


def sim_log(manifest_path: Path, out: Path) -> dict:
    subprocess.run(["npx", "tsx", str(ROOT / "packages/engine/scripts/run.ts"), str(manifest_path), str(out)], check=True, cwd=ROOT)
    return json.loads(out.read_text())


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="kafkasim-harness")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record", help="run a scenario on real Kafka and write an EventLog")
    r.add_argument("scenario", help="slug from contracts/scenarios or a path")
    r.add_argument("--out", type=Path)
    r.add_argument("--keep", action="store_true", help="leave containers running")
    c = sub.add_parser("calibrate", help="run calibrated scenarios on real Kafka and the sim; compare outcomes")
    c.add_argument("--only", nargs="*", help="subset of slugs")
    c.add_argument("--out-dir", type=Path, default=ROOT / "apps/web/public/replays")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from .runner import run_scenario
    from .signature import compare, signature

    def load(s: str) -> tuple[str, Path, dict]:
        path = Path(s) if Path(s).exists() else SCENARIOS / f"{s}.json"
        return path.stem, path, json.loads(path.read_text())

    work = Path(tempfile.mkdtemp(prefix="kafkasim-"))
    if args.cmd == "record":
        slug, _, manifest = load(args.scenario)
        out = args.out or ROOT / "apps/web/public/replays" / f"{slug}.real.json"
        run_scenario(manifest, out, work, keep=args.keep)
        print(json.dumps(signature(json.loads(out.read_text())), indent=2))
        return

    slugs = args.only or list(CALIBRATED)
    report = ["## kafkasim calibration (real Apache Kafka vs simulator)", ""]
    all_ok = True
    index = []
    args.out_dir.mkdir(parents=True, exist_ok=True)
    for slug in slugs:
        _, path, manifest = load(slug)
        real_path = args.out_dir / f"{slug}.real.json"
        try:
            real = run_scenario(manifest, real_path, work / slug)
            sim = sim_log(path, work / f"{slug}.sim.json")
            ok, rows = compare(signature(sim), signature(real), CALIBRATED.get(slug, []))
        except Exception as e:  # one broken scenario must not hide the others' results
            logging.exception("scenario %s failed", slug)
            ok, rows = False, [f"| harness error | {type(e).__name__}: {e} | | ❌ |"]
        all_ok &= ok
        section = [f"### {manifest['name']} {'✅' if ok else '❌'}", "", "| outcome | sim | real Kafka | |", "|---|---|---|---|", *rows, ""]
        print("\n".join(section), flush=True)
        report += section
        if real_path.exists():
            index.append({"file": real_path.name, "name": f"{manifest['name']} — real Kafka 4.2"})
            (args.out_dir / "index.json").write_text(json.dumps(index, indent=2))
    text = "\n".join(report)
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(text + "\n")
    sys.exit(0 if all_ok else 1)

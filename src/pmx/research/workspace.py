"""The research workspace (`research/`, private, git-ignored) and each experiment's lifecycle:

  proposed ─approve─► approved ─run─► discovery ─advance─► validation ─advance─► holdout
           ─close─► promoted | dead

Approval and each advance lock the experiment — a hash of `spec.md` + `run.py`, with copies under `locks/` — and runs
refuse anything else, so what was approved is what ran, and a spec provably came before its results. `amend`
re-locks after a change and is counted in every report; the holdout runs once. Every evaluated variant is appended
to `trials.csv`."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pmx.research import spec as specs
from pmx.research.windows import STAGES, Stage, Windows

TEMPLATES = Path(__file__).parent / "templates"
WORKSPACE_FILES = ("CLAUDE.md", "README.md", "AGENDA.md", "FINDINGS.md", "windows.toml")
EXPERIMENT_FILES = ("spec.md", "run.py", "notes.md")
TRIAL_FIELDS = ("at", "experiment", "stage", "variant", "hash", "events", "trades", "edge_c", "ci_low_c", "ci_high_c",
                "passed")
SLUG = re.compile(r"[a-z0-9][a-z0-9-]{1,60}")


class ResearchError(RuntimeError):
    """A refused lifecycle step; the message says why and what to do."""


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class Experiment:
    def __init__(self, path: Path) -> None:
        self.path, self.name = path, path.name
        self.spec_path, self.run_path = path / "spec.md", path / "run.py"
        self.state_path, self.results, self.locks = path / "state.json", path / "results", path / "locks"

    @property
    def state(self) -> dict[str, Any]:
        return json.loads(self.state_path.read_text())

    def save(self, state: dict[str, Any]) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2) + "\n")
        tmp.replace(self.state_path)

    @property
    def status(self) -> str:
        return self.state["status"]

    def spec(self) -> specs.Spec:
        return specs.load(self.spec_path)

    def fingerprint(self) -> str:
        """What a lock pins: the spec and the code that runs it."""
        h = hashlib.sha256(self.spec_path.read_bytes())
        h.update(b"\0")
        h.update(self.run_path.read_bytes())
        return h.hexdigest()

    def lock(self, stage: Stage, state: dict[str, Any]) -> str:
        fp = self.fingerprint()
        state.setdefault("locks", {})[stage] = {"hash": fp, "at": now_iso()}
        self.locks.mkdir(exist_ok=True)
        for f in (self.spec_path, self.run_path):
            shutil.copyfile(f, self.locks / f"{stage}-{fp[:12]}-{f.name}")
        return fp

    def check_lock(self, stage: Stage) -> str:
        """The current fingerprint, if it matches the stage's lock."""
        lock = self.state.get("locks", {}).get(stage)
        fp = self.fingerprint()
        if not lock:
            raise ResearchError(f"{self.name}: {stage} isn't locked (approve, or advance, locks it)")
        if lock["hash"] != fp:
            raise ResearchError(f"{self.name}: spec.md or run.py changed since {stage} was locked at {lock['at']} "
                                f"(copies in locks/); undo the change, or record it with `pmx research amend "
                                f"{self.name} --reason …` (counted in every report)")
        return fp


class Workspace:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.experiments_dir = root / "experiments"
        self.trials_path = root / "trials.csv"

    def exists(self) -> bool:
        return (self.root / "windows.toml").exists()

    def require(self) -> Workspace:
        if not self.exists():
            raise ResearchError(f"no research workspace at {self.root}; create it with `pmx research init`")
        return self

    def windows(self) -> Windows:
        return Windows.load(self.root / "windows.toml")

    def init(self) -> list[Path]:
        if self.exists():
            raise ResearchError(f"{self.root} is already a research workspace")
        self.root.mkdir(parents=True, exist_ok=True)
        made = []
        for name in WORKSPACE_FILES:
            dest = self.root / name
            if not dest.exists():
                shutil.copyfile(TEMPLATES / name, dest)
                made.append(dest)
        for d in ("experiments", "explore"):
            (self.root / d).mkdir(exist_ok=True)
        with self.trials_path.open("w", newline="") as f:
            csv.writer(f).writerow(TRIAL_FIELDS)
        return made + [self.trials_path]

    def all(self) -> list[Experiment]:
        if not self.experiments_dir.is_dir():
            return []
        return [Experiment(p) for p in sorted(self.experiments_dir.iterdir()) if (p / "state.json").exists()]

    def find(self, name: str) -> Experiment:
        exact = self.experiments_dir / name
        if (exact / "state.json").exists():
            return Experiment(exact)
        hits = [e for e in self.all() if e.name.endswith("-" + name) or e.name == name]
        if len(hits) == 1:
            return hits[0]
        if not hits:
            raise ResearchError(f"no experiment {name!r}; `pmx research status` lists them")
        raise ResearchError(f"{name!r} matches {', '.join(e.name for e in hits)}; use the full name")

    def new(self, slug: str, *, today: date | None = None, title: str = "") -> Experiment:
        if not SLUG.fullmatch(slug):
            raise ResearchError(f"slug {slug!r}: lowercase letters, digits and dashes (e.g. weather-longshot)")
        name = f"{(today or datetime.now(UTC).date()).isoformat()}-{slug}"
        path = self.experiments_dir / name
        if path.exists():
            raise ResearchError(f"{name} exists")
        path.mkdir(parents=True)
        for f in EXPERIMENT_FILES:
            text = (TEMPLATES / "experiment" / f).read_text()
            (path / f).write_text(text.replace("{{title}}", title or slug.replace("-", " ").capitalize())
                                  .replace("{{name}}", name))
        e = Experiment(path)
        e.save({"status": "proposed", "created": now_iso(), "locks": {}, "amendments": [], "runs": []})
        return e

    # ---- lifecycle -------------------------------------------------------------------------------------------

    def approve(self, e: Experiment) -> None:
        if e.status != "proposed":
            raise ResearchError(f"{e.name} is {e.status}, not proposed")
        e.spec()  # parses, or says why not
        st = e.state
        st |= {"status": "approved", "approved": now_iso()}
        e.lock("discovery", st)  # what was approved is what runs
        e.save(st)

    def amend(self, e: Experiment, reason: str) -> str:
        st = e.state
        stage = "discovery" if st["status"] == "approved" else st["status"]
        if stage not in STAGES:
            raise ResearchError(f"{e.name} is {stage}: nothing is locked to amend")
        if not reason.strip():
            raise ResearchError("amend needs --reason: what changed and why")
        old = st["locks"].get(stage, {}).get("hash")
        e.spec()
        if e.fingerprint() == old:
            raise ResearchError(f"{e.name}: nothing changed since the {stage} lock")
        new = e.lock(stage, st)
        st["amendments"].append({"at": now_iso(), "stage": stage, "reason": reason.strip(), "from": old, "to": new})
        e.save(st)
        return new

    def advance(self, e: Experiment) -> Stage:
        st = e.state
        runs = {r["stage"] for r in st["runs"]}
        if st["status"] == "discovery":
            if "discovery" not in runs:
                raise ResearchError(f"{e.name}: run discovery first")
            s = e.spec()
            if s.grid and any(k not in s.params for k in s.grid):
                raise ResearchError(f"{e.name}: set `params` in spec.md to the one variant validation will test "
                                    f"(every grid key: {', '.join(s.grid)})")
            e.lock("validation", st)
            st["status"] = "validation"
        elif st["status"] == "validation":
            if "validation" not in runs:
                raise ResearchError(f"{e.name}: run validation first")
            fp = e.check_lock("validation")
            st["locks"]["holdout"] = {"hash": fp, "at": now_iso()}
            st["status"] = "holdout"
        else:
            raise ResearchError(f"{e.name} is {st['status']}: advance moves discovery → validation → holdout")
        e.save(st)
        return st["status"]

    def close(self, e: Experiment, verdict: str, note: str, *, force: bool = False) -> None:
        st = e.state
        if verdict not in ("promoted", "dead"):
            raise ResearchError("close --as promoted | dead")
        if st["status"] in ("promoted", "dead"):
            raise ResearchError(f"{e.name} is already {st['status']}")
        if verdict == "promoted" and not any(r["stage"] == "holdout" for r in st["runs"]) and not force:
            raise ResearchError(f"{e.name}: promotion needs a holdout run (--force records an exception)")
        st |= {"status": verdict, "closed": {"at": now_iso(), "note": note, "forced": force}}
        e.save(st)
        lines = [f"\n## {e.name} — {verdict} ({st['closed']['at'][:10]})", "", note.strip() or "(no note)", ""]
        for r in st["runs"]:
            best = r.get("summary", "")
            lines.append(f"- {r['stage']} {r['at'][:10]}: {best}")
        lines.append(f"- trials: {self.trial_count(e.name)} · amendments: {len(st['amendments'])}"
                     + (" · closed with --force" if force else ""))
        with (self.root / "FINDINGS.md").open("a") as f:
            f.write("\n".join(lines) + "\n")

    # ---- trials ----------------------------------------------------------------------------------------------

    def log_trials(self, rows: list[dict[str, Any]]) -> None:
        new = not self.trials_path.exists()
        with self.trials_path.open("a", newline="") as f:
            w = csv.DictWriter(f, TRIAL_FIELDS)
            if new:
                w.writeheader()
            w.writerows(rows)

    def trials(self) -> list[dict[str, str]]:
        if not self.trials_path.exists():
            return []
        with self.trials_path.open(newline="") as f:
            return list(csv.DictReader(f))

    def trial_count(self, experiment: str | None = None) -> int:
        """Distinct (experiment, stage, spec+code version, variant) evaluated: re-running the same thing isn't a new
        trial; the same variant on a new stage's data is."""
        return len({(t["experiment"], t["stage"], t["hash"], t["variant"]) for t in self.trials()
                    if experiment is None or t["experiment"] == experiment})

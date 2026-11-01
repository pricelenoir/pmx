# Research workspace

Private: hypotheses, specs, results. Not part of the pmx repo (git-ignored); `pmx ops backup` copies it.
The process is `docs/RESEARCH.md` in pmx; how an agent works here is `CLAUDE.md`.

```
AGENDA.md      ranked hypotheses, each with its mechanism
FINDINGS.md    one entry per closed experiment, dead ones included
windows.toml   discovery / validation / holdout dates
trials.csv     every variant ever evaluated (written by pmx research run)
explore/       free-form notebooks on discovery data; never evidence
experiments/   <date>-<slug>/: spec.md · run.py · notes.md · state.json · results/ · locks/
```

```
pmx research new SLUG          # draft an experiment (proposed)
pmx research approve NAME      # you, in a terminal: locks spec.md + run.py
pmx research run NAME          # the current stage
pmx research advance NAME      # discovery → validation → holdout
pmx research amend NAME --reason …   # record a change after a lock
pmx research close NAME --as dead|promoted --note …
pmx research status
```

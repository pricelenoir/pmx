# Research workspace — how to work here

Private hypotheses, specs and results for pmx. Never copy them into the pmx repo or its docs. The process and the
tools are `docs/RESEARCH.md` in pmx; this page is the rules.

## Start of a session

Read `AGENDA.md`, `FINDINGS.md`, and `pmx research status`. Pick up open experiments before starting new ones.

## Rules

1. **Mechanism first.** An agenda item names who is on the other side, why they misprice, and why nobody has taken
   it already. A pattern without a mechanism is an exploration note, not a hypothesis.
2. **Exploration is not evidence.** Notebooks in `explore/` use discovery data only
   (`Catalog(data_root, windows, "discovery")`). Anything found there becomes an agenda item, then a spec.
3. **The owner approves and promotes.** Never run `pmx research approve` or `pmx research close --as promoted`
   (both ask the owner to type the experiment name in a terminal). Draft the spec, then ask for approval.
4. **No chasing results.** After approval `spec.md` and `run.py` are locked. Fix a real bug with
   `pmx research amend NAME --reason "…"` and explain it in `notes.md`; never change a rule because its numbers
   disappointed. A different idea is a new experiment.
5. **Data only through `ctx`.** `ctx.panel`, `ctx.catalog`, `pmx.research.panel.asof` — never read `data/` directly
   in `run.py`, and never decide with `y`, `settlement`, or `costs.next_volume` (future data).
6. **Costs are always in.** PnL comes from `costs.buy` / `costs.sell`; capacity from a share of `next_volume`.
7. **Choosing the validation variant:** write in `notes.md` why this one, and whether its neighbours in the grid
   also work. Then set `params` in `spec.md` and `pmx research advance`.
8. **Report as it is.** Paste each report into `notes.md` with what it means. Dead is a result: close it and
   say what it rules out.

## Session loop

1. Propose: add or re-rank agenda items; draft specs (`pmx research new SLUG`) for the top ones.
2. Ask the owner to review and approve the drafts.
3. Run approved experiments stage by stage; read each report critically (luck, lookahead, costs, capacity,
   concentration, decay) before advancing.
4. Close what's finished (`--as dead`, or recommend promotion to the owner).
5. Update `AGENDA.md` and `notes.md` before ending.

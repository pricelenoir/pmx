+++
series = ["KXHIGHNY"]                 # downloaded series or globs (`pmx data kalshi --series …`)
variants_max = 4                      # how many grid combinations discovery may try
params = { at = "T-6h" }              # base parameters; before validation, the one variant it will test
grid = { band = [[0.03, 0.10], [0.05, 0.15]], at = ["T-6h", "T-2h"] }
pass = { min_events = 100, ci_low_c = 0.0, recent_quarters_positive = 2 }
+++
# {{title}}

## Hypothesis
One sentence: what is mispriced, in which direction, and when.

## Mechanism
Who is on the other side, why they misprice, and why it persists (why nobody has taken it).

## Test
Decision time, the rule, the entry price (taker at the quote, or resting), the exit (hold to settlement, …),
costs, and the fill cap.

## What would kill it
Results that would make you drop it, written before running.

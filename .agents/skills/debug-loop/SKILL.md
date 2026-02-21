name: debug-loop
description: A repeatable debugging loop: reproduce, isolate, test, fix, verify, summarize.

## When to use
- tests are failing
- a script errors on the cluster
- results look suspicious (e.g., intercept shifts, flat lines, join explosions)

## Steps
1) Reproduce with the smallest command possible.
2) Capture the failure:
   - exact command
   - stack trace / error output
   - input files and config
3) Isolate:
   - identify the minimal file/function responsible
   - add temporary logging (row counts, summary stats)
4) Add a check:
   - a small unit test OR an invariant check in the script
5) Fix and re-run:
   - run the same command again
   - run the most relevant test suite
6) Summarize:
   - root cause
   - fix
   - what you verified
   - any follow-up risks

## Don’ts
- Don’t “fix” by changing definitions unless explicitly asked.
- Don’t add heavy dependencies just to debug.


---
name: updating-docs
description: Use when documenting repo work so iteration notes go into a new standalone docs note and codex chat entry, not canonical reference docs like docs/data-products.md unless schemas/contracts changed.
---

# Updating Docs

Use this skill when the user asks to "write up what we did" or summarize an analysis / coding iteration.

## Default rule

- Create a new standalone note under `docs/notes/` (dated filename).
- Add a short summary and pointer in the relevant `codex-plans/*.md` chat log.
- Do not put iteration notes into canonical docs (`docs/data-products.md`, etc.) unless the change is a true schema / contract / path documentation change.

## Canonical doc guardrail

Before editing a canonical docs file, verify at least one is true:

- product path or config key changed
- producer script changed for that documented product
- primary key or schema changed
- column definitions / semantics changed
- existing reference text is factually wrong

If none apply, create a new note instead.

## Minimal workflow

1. Check `git diff` / `git diff --cached` first so you do not mix unrelated edits.
2. Write a short note with:
   - what changed
   - why
   - what was run / verified
   - open questions / next steps
3. Add a pointer in the codex chat file.
4. Stage docs by explicit path.
5. Exclude `scratch/` unless explicitly requested.

## Recovery rule

If iteration notes were added to a canonical doc by mistake:

- move them into a new standalone note
- revert only that note-level addition from the canonical doc
- mention the correction in the codex chat summary

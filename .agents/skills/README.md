# Repo Skills for Codex / CLI agents

These skills are small, reusable workflows that help the agent behave consistently in this repo.

How to use:
- Keep this directory in your repo at `.agents/skills/`.
- Invoke by name in your prompt (e.g., "Use the `join-safety` skill..."), or let Codex auto-select.
- Customize any SKILL.md to match your exact commands, paths, and data products.

Repo-wide expectation:
- **Fail loud, not gracefully.** A silent fallback (`NA` fill, skipped section, empty placeholder) is a hidden bug. Required data missing → hard `stop()`. See the `fail-loud` skill.
- This applies to pipeline scripts **and** QMD/Quarto reports. A rendered document with blank columns or missing sections is worse than a render failure, because it looks correct.
- Optional features (behind a config flag, an explicitly optional data product) are the only acceptable exception — but they must still log a visible warning, never silently vanish.

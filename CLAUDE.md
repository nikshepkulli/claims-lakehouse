# CLAUDE.md — claims-lakehouse

Open-source portfolio project by Nikshep Kulli: a multi-cloud lakehouse on synthetic healthcare claims. Target: show Senior Data Engineer skills (Spark/Databricks, Snowflake, AWS+Azure, streaming, IaC, data quality, lineage, ML/GenAI enablement).

## Rules

- Ticket first; PR says `Closes #N`; stacked PRs name their dependency. Plan lives in milestones M0-M7.
- Tests and docs in the same PR as code. No task is done without both.
- All data is synthetic and labelled. Never commit real data, PHI, or credentials.
- Be honest in docs: say what runs locally, what ran on a real cloud, and what did not. Never claim a service was used if the code does not run on it.
- One ADR per architectural choice (docs/adr).
- Run `make lint test` before every commit once the Makefile exists (ticket #2/#3).

## Running log

- 2026-11-05: repo created, 32 tickets across 8 milestones filed; scaffold PR for #1.

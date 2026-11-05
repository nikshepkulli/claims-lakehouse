# Contributing

This repo is built ticket-first, the way a production data team works.

1. Every change starts as a GitHub issue with acceptance criteria. Pick one from the [milestones](../../milestones), or open one first.
2. Branch from `main`: `feat/<issue>-<slug>` (or `fix/`, `chore/`).
3. Open a PR using the template. It must say `Closes #N`. Stacked PRs name their base and dependency.
3b. A workflow (`pr-issue-link`) fails PRs whose body has no `Closes #N` / `Part of #N` and comments the PR link on the issue automatically.
4. CI must be green: lint, tests, SQL lint, Terraform validate.
5. Tests and docs ship in the same PR as the code. A change is not done until both exist.
6. Architectural choices get an ADR in `docs/adr/`.

All data in this repo is synthetic. Never commit real data, PHI, or credentials.

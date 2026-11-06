# ADR-0005: Identifiers become keyed HMAC tokens in gold, not unkeyed hashes

- **Status:** Accepted
- **Date:** 2026-11-06
- **Tickets:** #71 (also touches #25, #8, #14)

## Context

Gold must let analysts join claims to members and providers without seeing the source ids. The first design (#25) hashed `member_id` and `provider_id` with plain SHA-256. The generated source ids are sequential, so anyone can hash every possible id and look the result up: an unkeyed hash of a low-entropy identifier is a lookup table, not protection. The same weakness applies to hashes of birth year, sex, state and plan.

## Options considered

### A. Keep unkeyed SHA-256
- Pros: no secret to manage; trivially reproducible in Spark and Snowflake.
- Cons: reversible by enumeration. Rejected.

### B. Random surrogate ids stored in a mapping table
- Pros: not derivable from the source id at all.
- Cons: the mapping table is itself the sensitive asset, every rebuild needs a lookup, and the same person gets different ids if the table is lost or rebuilt.

### C. Keyed HMAC-SHA256 token, key outside the repo
- Pros: deterministic (same key, same value, same token in every table, so joins and orphan checks still work); not reversible by enumeration without the key; no lookup table; the key id inside the token (`tok_<key_id>_<32 hex>`) makes rotation visible and checkable.
- Cons: Spark SQL has no HMAC primitive, so the key travels inside the task closure (readable by anyone who can read job binaries on the cluster); one key covers the whole id space; rotation means rebuilding gold.

## Decision

C. `member_id` and `provider_id` are tokenised by the gold job (`lakehouse.governance.tokenise.Tokeniser`): HMAC-SHA256 over `domain || 0x00 || value`, truncated to 128 bits. The key comes from `LAKEHOUSE_PSEUDONYM_KEY` (at least 32 characters) and the gold job refuses to run without it. Surrogate keys for members and providers are derived from the token. Unkeyed hashes are kept only where they protect nothing (`diagnosis_sk` over a public ICD-10 code) or are never exposed as an id (`record_hash` for SCD2 change detection, dropped for analysts).

## Consequences

- Rotation is a rebuild of gold from silver into a new root, because HMAC cannot be reversed. Runbook and column-by-column decisions: [privacy-and-access.md](../privacy-and-access.md).
- Every member and provider surrogate key moves on rotation; downstream consumers must reload.
- CI and `make pipeline-e2e` use a fake, public, test-only key. No real key exists in the repo.
- Unresolved, stated in the privacy doc: key-in-closure exposure, a single key for all rows, `claim_id` and `line_id` not tokenised, quasi-identifier re-identification not measured. Snowflake policies that rely on the tokens are validated offline only, never executed.
- This is a design for synthetic data. It is not a HIPAA compliance claim.

## What would change my mind

A requirement for per-tenant keys or for tokens to be reversible by an authorised party (then B, or a vault-based tokenisation service), or a move to a platform with a native HMAC primitive so the key need not ship in task code.

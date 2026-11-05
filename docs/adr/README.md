# Architecture Decision Records

Short records of the decisions that shape this repo and the reasoning behind them. New ADRs start from [template.md](template.md); superseded ADRs stay in place with their status updated.

| ADR | Title | Status | Tickets |
|-----|-------|--------|---------|
| [0001](0001-medallion-layout.md) | Medallion layout: bronze / silver / gold | Accepted | #6, #7, #8 |
| [0002](0002-delta-vs-iceberg.md) | Table format: Delta Lake, with Iceberg interop at the edge | Accepted | #7, #10, #14 |
| [0003](0003-cloud-agnostic-boundary.md) | Cloud-agnostic boundary: portable core, native edges | Accepted | #18, #19, #20 |

See also [../architecture.md](../architecture.md) for the planned platform diagram.

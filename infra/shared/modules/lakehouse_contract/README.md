# lakehouse_contract

Resource-free Terraform module that pins the **output contract** both clouds must meet
(ADR-0003, issue #20). Each environment wraps its cloud module with it, so a drift in
output shape fails `terraform validate`/`plan` instead of surfacing in pipeline config.

| Input | Type | Notes |
|---|---|---|
| `cloud` | `aws` or `azure` | Selects the expected URI scheme |
| `layer_uris` | map(string) | Exactly `bronze`, `silver`, `gold` |
| `principals` | map(any) | Exactly `loader`, `transformer`, `analyst`, each with `id` and `principal_id` |

| Output | Shape |
|---|---|
| `layer_uris` | `{bronze, silver, gold} => s3a://... or abfss://...` |
| `principals` | `{loader, transformer, analyst} => {id, principal_id}` |
| `cloud`, `scheme` | strings |

Principal semantics differ by design: on AWS both attributes are the IAM role ARN; on Azure
`id` is the managed identity resource ID and `principal_id` its Entra object ID.
Variable names on the two storage modules are intentionally not renamed (see
`docs/multi-cloud.md`, "Interface mapping").

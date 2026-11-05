# Runbook

Operational procedures. Cloud steps below were written from documentation and have **not** been run
against a real account (see [cd.md](cd.md#untested-no-cloud-account-attached)).

## Failed apply and drift

### Apply failed

1. Open the failed `apply` job. Note which resource failed and the provider error.
2. Check state is intact: `terraform -chdir=infra/<cloud>/envs/<env> state list` (init with the same remote backend args the workflow uses).
3. Fix the cause (permissions, quota, name collision, typo) in a PR; merge; re-run **Terraform apply**. The new plan only contains what remains.
4. Permission errors (`AccessDenied`, `AuthorizationFailed`): the least-privilege deploy policy in `infra/<cloud>/bootstrap/oidc` is missing an action. Add it there, re-apply the bootstrap by hand, retry. Do not widen to `*`.
5. Stuck lock (`Error acquiring the state lock`) after a cancelled run: confirm no apply is running, then `terraform force-unlock <LOCK_ID>` (AWS: DynamoDB lock item; Azure: blob lease on the state file).
6. "Saved plan is stale": state changed between plan and apply. Re-run the workflow.

### Roll back a bad change

1. `git revert <merge-commit>` on a branch, PR, review the plan comment (it should show the inverse), merge.
2. Run **Terraform apply** for the cloud/environment; approve.
3. Data is not restored by Terraform: use S3 object versions or Azure blob soft delete; cancel a scheduled KMS deletion with `aws kms cancel-key-deletion --key-id <id>` then `enable-key`.

### Drift (console change, or `plan` shows unexpected diffs)

1. A PR plan comment or a manual apply run shows changes nobody committed. Treat as drift.
2. Decide: the console change is wrong, or the code is. If wrong console change: run Terraform apply to reconcile (the plan shows the revert). If the code is wrong: update code to match, PR, plan shows no changes.
3. Never `terraform apply` from a laptop against shared state; use the workflow so the approval trail exists.
4. Find who changed it: CloudTrail (AWS) / Activity Log (Azure).

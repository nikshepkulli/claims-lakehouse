#!/usr/bin/env bash
# Static security-invariant checks for the lakehouse_warehouse module.
# Needed because there is no Snowflake account in CI and Terraform 1.5 has no
# mock providers. Complements tests/snowflake/test_snowflake_code.py.
# Usage: bash infra/snowflake/tests/static_checks.sh
set -euo pipefail
M="$(cd "$(dirname "$0")/../modules/lakehouse_warehouse" && pwd)"
E="$(cd "$(dirname "$0")/../envs/dev" && pwd)"
fail=0
need() { # description, pattern, file
  if grep -Eq "$2" "$M/$3"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi
}
forbid_all() { # description, pattern  (searches module + env, comments excluded)
  if cat "$M"/*.tf "$E"/*.tf | grep -Ev '^[[:space:]]*#' | grep -Eiq "$2"; then echo "FAIL $1"; fail=1; else echo "ok   $1"; fi
}
need "resource monitor suspends at 100"      'suspend_trigger[[:space:]]*=[[:space:]]*100' main.tf
need "resource monitor cancels at 110"       'suspend_immediate_trigger[[:space:]]*=[[:space:]]*110' main.tf
need "resource monitor notifies"             'notify_triggers' main.tf
need "every warehouse has a monitor"         'resource_monitor[[:space:]]*=' main.tf
need "warehouses start suspended"            'initially_suspended[[:space:]]*=[[:space:]]*true' main.tf
need "warehouses have statement timeout"     'statement_timeout_in_seconds' main.tf
need "schemas use managed access"            'with_managed_access[[:space:]]*=[[:space:]]*"true"' main.tf
need "functional roles roll up to SYSADMIN"  'functional_to_sysadmin' roles.tf
need "table grants are FUTURE grants"        'future[[:space:]]*\{' grants.tf
need "external stage is opt-in"              'count[[:space:]]*=[[:space:]]*var.enable_external_stage' external_stage.tf
need "stage uses a storage integration"      'storage_integration[[:space:]]*=' external_stage.tf
need "provider pinned (module)"              'version[[:space:]]*=[[:space:]]*"~> 2\.' versions.tf
forbid_all "no grants to PUBLIC"             '"PUBLIC"'
forbid_all "no ALL PRIVILEGES"               'all_privileges|"ALL PRIVILEGES"'
forbid_all "no OWNERSHIP / MANAGE GRANTS"    '"OWNERSHIP"|"MANAGE GRANTS"'
forbid_all "no WITH GRANT OPTION"            'with_grant_option[[:space:]]*=[[:space:]]*true'
forbid_all "no passwords / keys in code"     '(password|private_key|aws_secret_key|aws_key_id)[[:space:]]*='
forbid_all "no account identifiers in code"  '(account|account_name|organization_name)[[:space:]]*='
forbid_all "no AWS access key ids"           'AKIA[0-9A-Z]{16}'
forbid_all "no credentials block on stage"   'credentials[[:space:]]*\{'
# Analysts only inherit read-only access roles.
if awk '/ANALYST[[:space:]]*=[[:space:]]*\[/' "$M/roles.tf" | grep -Eq '_RW'; then echo "FAIL analyst inherits an RW role"; fail=1; else echo "ok   analyst inherits only RO roles"; fi
# Analyst warehouse keeps a short statement timeout (<= 300s default).
if grep -Eq 'statement_timeout_seconds[[:space:]]*=[[:space:]]*300' "$M/variables.tf"; then echo "ok   analyst statement timeout 300s"; else echo "FAIL analyst statement timeout"; fail=1; fi
exit $fail

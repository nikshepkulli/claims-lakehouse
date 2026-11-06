#!/usr/bin/env bash
# Container entrypoint for orchestration/airflow/docker-compose.yml.
#   standalone                       airflow standalone (UI on $AIRFLOW_PORT, admin password in the log)
#   test <YYYY-MM-DD> [...]          airflow dags test claims_daily <date>, in order, stop on failure
#   backfill <start> <end>           airflow dags backfill claims_daily -s <start> -e <end>
#   sh <cmd...>                      run any command with the environment prepared
set -euo pipefail
cd /work
unset SPARK_HOME
C=/cache
PYV=$(python3 -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')
AIRFLOW_VERSION=2.10.5
export PATH=$C/bin:$PATH
export PYTHONPATH=/work/src:/work/orchestration/airflow:$C/spy:$C/air

hash=$(cat requirements-airflow.txt requirements-spark.txt requirements-claimsgen.txt requirements-dbt.txt | md5sum | cut -c1-12)
if [ ! -f "$C/ok-$hash" ]; then
  echo ">> installing dependencies into $C (first run takes a few minutes)"
  rm -rf $C/spy $C/air $C/dbt $C/bin $C/ok-*
  mkdir -p $C/bin $C/home
  PIP="pip install -q --disable-pip-version-check"
  # Spark + claimsgen. The image has Python 3.10 but requirements pin numpy 2.4.6 (needs 3.11):
  # relax it like mk/streaming_spark.mk does (issue #76). Local-only workaround.
  cat requirements-spark.txt requirements-claimsgen.txt | grep -v '^-r' | grep -v '^pytest' \
    | sed 's/^numpy==.*/numpy<2.3/' | sed 's/^pyarrow==.*/pyarrow<19/' > /tmp/spark-req.txt
  $PIP --target $C/spy -r /tmp/spark-req.txt
  # Airflow with the official constraints for this Python version.
  $PIP --target $C/air -r requirements-airflow.txt \
    -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt"
  # dbt in its own path: its jinja/protobuf pins must not leak into Airflow's. dbt-core 1.9.x is
  # the newest line that installs on Python 3.10 (requirements-dbt.txt pins 1.12 for CI on 3.12).
  $PIP --target $C/dbt "dbt-core>=1.9,<1.10" "dbt-duckdb>=1.9,<1.10"
  ln -sf "$(command -v python3)" $C/bin/python
  printf '#!/usr/bin/env bash\nexec python3 -m airflow "$@"\n' > $C/bin/airflow
  printf '#!/usr/bin/env bash\nexport PYTHONPATH=%s/dbt\nexec python3 -c "import sys; from dbt.cli.main import cli; sys.exit(cli())" "$@"\n' $C > $C/bin/dbt
  chmod +x $C/bin/airflow $C/bin/dbt
  touch "$C/ok-$hash"
fi

airflow db migrate >/dev/null 2>&1 || airflow db init
airflow pools set claims_spark 1 "Spark layers share one slot (local[*] uses the whole box)" >/dev/null

cmd=${1:-standalone}; shift || true
case "$cmd" in
  standalone) exec airflow standalone ;;
  test) for d in "$@"; do echo ">> airflow dags test claims_daily $d"; airflow dags test claims_daily "$d"; done ;;
  backfill) exec airflow dags backfill claims_daily -s "$1" -e "$2" ;;
  sh) exec "$@" ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac

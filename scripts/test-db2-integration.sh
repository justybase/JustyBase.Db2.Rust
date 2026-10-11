#!/usr/bin/env bash
# Local Db2 integration tests for justybase-db2.
#
# Starts a throwaway IBM Db2 Community container, waits until it really accepts
# SQL, runs the live integration tests against THAT container only, and removes
# the container (and its volumes) afterwards, also on failure or Ctrl-C.
#
# Usage: scripts/test-db2-integration.sh [--full|--parity|--parity-full]
#   (default)  small smoke suite: connect, DML/types/NULLs, parameters,
#              transactions, result shape, error propagation
#   --full     every existing live test of this repository that needs only the
#              one database started here (takes several minutes)
#   --parity   pinned IBM .NET + JCC workers and server canonical cases (smoke)
#   --parity-full  full public cross-driver case manifest
#
# Environment overrides (all optional):
#   DB2_IT_IMAGE          Db2 image (default below; must not be a floating tag)
#   DB2_IT_READY_TIMEOUT  seconds to wait for Db2 (default 480)
#   DB2_IT_REPORT_DIR     optional directory for redacted console output
#   DB2_IT_KEEP_CONTAINER if set to 1, the Db2 container is left running on exit
#                         for post-mortem inspection (logs, db2diag) instead of
#                         being removed by cleanup. The container name is printed
#                         at startup; remove it manually afterwards.
#
# This script never reads .env files or DB2_* variables from the caller's
# environment: they are cleared before the tests run, so the tests can only
# reach the container started by this invocation.
set -Eeuo pipefail

SLUG="justybase-db2"
# Pinned Db2 LUW 12.1.5 Community image from the IBM Container Registry. The
# default image is verified against REQUIRED_DIGEST; an override must pin its own
# digest (image@sha256:... or DB2_IT_EXPECTED_DIGEST) or the run fails closed.
DEFAULT_IMAGE="icr.io/db2_community/db2:12.1.5.0"
REQUIRED_DIGEST="sha256:2de8151713c261843868c5c3411b57be6ae79d99d70a5b3022337836776bfda6"
IMAGE="${DB2_IT_IMAGE:-$DEFAULT_IMAGE}"
READY_TIMEOUT="${DB2_IT_READY_TIMEOUT:-480}"
# Captured before the DB2_* reset below, which clears DB2_IT_REPORT_DIR too.
REPORT_DIR_INPUT="${DB2_IT_REPORT_DIR:-}"
DB_NAME="testdb"
DB_USER="db2inst1"

# Expected digest: the image's own @sha256, an explicit override, or the default
# digest when the default image is used. Empty means a tag-only override.
case "$IMAGE" in
  *@sha256:*) IMAGE_DIGEST="${IMAGE##*@}" ;;
  *) IMAGE_DIGEST="" ;;
esac
if [ -n "${DB2_IT_EXPECTED_DIGEST:-}" ]; then
  EXPECTED_DIGEST="$DB2_IT_EXPECTED_DIGEST"
elif [ -n "$IMAGE_DIGEST" ]; then
  EXPECTED_DIGEST="$IMAGE_DIGEST"
elif [ -z "${DB2_IT_IMAGE:-}" ]; then
  EXPECTED_DIGEST="$REQUIRED_DIGEST"
else
  EXPECTED_DIGEST=""
fi

SUITE="smoke"
for arg in "$@"; do
  case "$arg" in
    --full) SUITE="full" ;;
    --parity) SUITE="parity-smoke" ;;
    --parity-full) SUITE="parity-full" ;;
    -h|--help) sed -n '2,/^set -Eeuo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) printf '[db2-it] FAIL: unknown argument: %s (see --help)\n' "$arg" >&2; exit 2 ;;
  esac
done

log()  { printf '[db2-it] %s\n' "$*"; }
warn() { printf '[db2-it] WARN: %s\n' "$*" >&2; }
die()  { printf '[db2-it] FAIL: %s\n' "$*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="startup"
CONTAINER=""
WORKDIR=""
PASSWORD=""
EVENTS_PID=""
EVENTS_LOG=""

# ---------------------------------------------------------------- cleanup ---
# Runs on every exit path. The original exit status is preserved; a container
# that could not be removed turns an otherwise green run into a failure.
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  set +e
  local cleanup_failed=0

  if [ "$rc" -ne 0 ]; then
    if [ "$rc" -eq 130 ] || [ "$rc" -eq 143 ]; then
      log "Interrupted during stage: $STAGE"
    else
      printf '[db2-it] FAIL: stage "%s" failed (exit %s)\n' "$STAGE" "$rc" >&2
      if [ -n "$CONTAINER" ]; then
        { echo "[db2-it] ---- docker ps -a ----"
          docker ps -a
          if docker inspect "$CONTAINER" >/dev/null 2>&1; then
            echo "[db2-it] ---- docker inspect $CONTAINER (state) ----"
            docker inspect -f 'Status={{.State.Status}} OOMKilled={{.State.OOMKilled}} ExitCode={{.State.ExitCode}} Error={{.State.Error}} StartedAt={{.State.StartedAt}} FinishedAt={{.State.FinishedAt}}' "$CONTAINER" 2>&1
          else
            echo "[db2-it] ---- container $CONTAINER no longer exists (removed externally?) ----"
          fi
          echo "[db2-it] ---- docker logs $CONTAINER (last 200 lines) ----"
          docker logs --tail 200 "$CONTAINER" 2>&1 | redact || echo "[db2-it] (no logs: container is gone)"
          if [ -n "${EVENTS_LOG:-}" ] && [ -f "$EVENTS_LOG" ]; then
            echo "[db2-it] ---- docker events for $CONTAINER ----"
            tail -n 50 "$EVENTS_LOG" >&2 || true
            if [ -n "${TEST_LOG:-}" ] && [ -d "$(dirname "$TEST_LOG")" ]; then
              cp "$EVENTS_LOG" "${TEST_LOG%.log}.docker-events.log" 2>/dev/null || true
            fi
          fi
        } >&2
      fi
    fi
    if [[ "$SUITE" == parity-* ]] && [ -n "${DB2_PARITY_REPORT:-}" ] && [ ! -f "$DB2_PARITY_REPORT" ]; then
      mkdir -p "$(dirname "$DB2_PARITY_REPORT")"
      {
        echo "Db2 parity qualification"
        echo "Rust commit: ${DB2_PARITY_COMMIT:-unknown}"
        echo "Db2: ${DB2_PARITY_DB_VERSION:-unknown}"
        echo "IBM .NET package: Net.IBM.Data.Db2-lnx ${DB2_PARITY_DOTNET_VERSION:-10.0.0.300}"
        echo "IBM JCC: ${DB2_PARITY_JCC_VERSION:-12.1.5.0}"
        echo "Java: ${DB2_PARITY_JAVA_VERSION:-unknown}"
        echo ".NET: ${DB2_PARITY_DOTNET_RUNTIME:-unknown}"
        echo "Overall: FAIL (stage: $STAGE)"
        if [ -n "${TEST_LOG:-}" ] && [ -f "$TEST_LOG" ]; then
          sed -n 's/.*case \([a-zA-Z0-9_]*\):.*/Failed case: \1/p' "$TEST_LOG" | sort -u
        fi
      } > "$DB2_PARITY_REPORT"
    fi
  fi

  # Always stop the background docker-events tap (success or failure).
  if [ -n "${EVENTS_PID:-}" ] && kill -0 "$EVENTS_PID" 2>/dev/null; then kill "$EVENTS_PID" 2>/dev/null || true; fi

  if [ -n "$CONTAINER" ]; then
    if [ "${DB2_IT_KEEP_CONTAINER:-0}" = "1" ]; then
      log "Keeping Db2 container $CONTAINER for inspection (DB2_IT_KEEP_CONTAINER=1); remove it with: docker rm -f -v $CONTAINER"
    else
      log "Removing Db2 container $CONTAINER..."
      docker rm -f -v "$CONTAINER" >/dev/null 2>&1
      if docker inspect "$CONTAINER" >/dev/null 2>&1; then
        warn "container $CONTAINER still exists; remove it with: docker rm -f -v $CONTAINER"
        cleanup_failed=1
      fi
    fi
  fi
  if [ -n "$WORKDIR" ]; then rm -rf "$WORKDIR"; fi

  if [ "$cleanup_failed" -ne 0 ] && [ "$rc" -eq 0 ]; then rc=1; fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

redact() { if [ -n "$PASSWORD" ]; then sed "s/$PASSWORD/[REDACTED]/g"; else cat; fi; }

# -------------------------------------------------------------- pre-flight ---
STAGE="pre-flight"
log "Checking environment..."
[ "$(uname -s)" = "Linux" ] || die "Linux is required (found $(uname -s))."
case "$(uname -m)" in
  x86_64|amd64) ;;
  *) die "x86_64 is required: the IBM Db2 Community image is linux/amd64 only (found $(uname -m))." ;;
esac
command -v docker >/dev/null 2>&1 || die "Docker CLI not found. Install Docker Engine and make sure 'docker' is on PATH."
docker info >/dev/null 2>&1 || die "Docker daemon is not reachable. Start Docker and check that your user may run 'docker info'."
case "$IMAGE" in
  *:latest|*:) die "DB2_IT_IMAGE must be pinned to an exact version, not a floating tag: $IMAGE" ;;
  *:*) ;;
  *) die "DB2_IT_IMAGE must include an explicit tag: $IMAGE" ;;
esac
[ -n "$EXPECTED_DIGEST" ] || die "DB2_IT_IMAGE override must pin a digest (image@sha256:...) or set DB2_IT_EXPECTED_DIGEST."
command -v cargo >/dev/null 2>&1 || die "cargo not found. Install the Rust toolchain from rust-toolchain / rust-version in Cargo.toml."
# Db2 needs several GB of free RAM and disk; fail early with a clear message
# instead of losing the container mid-startup (OOM or no space looks exactly
# like "the container disappeared").
if command -v free >/dev/null 2>&1; then
  mem_avail_mb="$(free -m | awk '/^Mem:/{print $7}')"
  log "Available memory: ${mem_avail_mb:-unknown} MiB"
  case "${mem_avail_mb:-}" in ''|*[!0-9]*) ;;
    *) [ "$mem_avail_mb" -ge 4096 ] || warn "less than 4 GiB available memory (${mem_avail_mb} MiB); Db2 startup may fail or be killed" ;;
  esac
fi
docker_root="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo /var/lib/docker)"
disk_avail_kb="$(df -k "$docker_root" 2>/dev/null | awk 'NR==2{print $4}')"
log "Docker root $docker_root available disk: ${disk_avail_kb:-unknown} KiB"
case "${disk_avail_kb:-}" in ''|*[!0-9]*) ;;
  *) [ "$disk_avail_kb" -ge 10485760 ] || die "less than 10 GiB free disk under $docker_root; free space before running Db2" ;;
esac
# Stale throwaway containers from previously interrupted runs do not collide
# (random names and host ports), but warn so they can be reaped.
stale="$(docker ps -a --filter "label=db2-it.repo=$SLUG" --format '{{.Names}} ({{.Status}})' 2>/dev/null || true)"
if [ -n "$stale" ]; then
  warn "stale $SLUG containers from previous runs:"
  printf '%s\n' "$stale" >&2
  warn "reap them with: docker ps -a --filter label=db2-it.repo=$SLUG -q | xargs -r docker rm -f -v"
fi

# ----------------------------------------------------------------- startup ---
RUN_TAG="$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
CONTAINER="${SLUG}-db2-it-${RUN_TAG}"
SCHEMA="IT_$(printf '%s' "$RUN_TAG" | tr 'a-f' 'A-F')"
PASSWORD="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/${SLUG}-db2-it.XXXXXX")"

STAGE="image pull"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  log "Pulling $IMAGE (first run only; the image is several GB)..."
  docker pull "$IMAGE" >/dev/null || die "could not pull $IMAGE"
fi

STAGE="image digest verification"
actual_digests="$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$IMAGE" 2>/dev/null || true)"
case "$actual_digests" in
  *"$EXPECTED_DIGEST"*) log "Image digest verified: $EXPECTED_DIGEST" ;;
  *) die "Db2 image digest mismatch for $IMAGE: expected $EXPECTED_DIGEST, found: ${actual_digests:-<none>}" ;;
esac

STAGE="container start"
log "Starting IBM Db2 ($IMAGE)..."
# The password goes through an env file (mode 600) so it never appears in a
# process list. The file lives in WORKDIR and is removed by cleanup.
umask 077
cat > "$WORKDIR/db2.env" <<EOF
LICENSE=accept
DB2INSTANCE=$DB_USER
DB2INST1_PASSWORD=$PASSWORD
DBNAME=$DB_NAME
BLU=false
ENABLE_ORACLE_COMPATIBILITY=false
UPDATEAVAIL=NO
TO_CREATE_SAMPLEDB=false
REPODB=false
IS_OSXFS=false
PERSISTENT_HOME=false
HADR_ENABLED=false
ETCD_ENDPOINT=
ETCD_USERNAME=
ETCD_PASSWORD=
EOF
# CONTAINER is already set, so cleanup also removes a container that docker
# created but failed to start.
docker run --detach --name "$CONTAINER" --privileged \
    --label "db2-it.repo=$SLUG" --label "db2-it.run=$RUN_TAG" \
    --publish 127.0.0.1::50000 --env-file "$WORKDIR/db2.env" "$IMAGE" >/dev/null \
  || die "docker run failed"
log "Container: $CONTAINER"
# Background event tap: if the container dies or is removed mid-run, the log
# shows who did it (die/oom/kill/destroy) and when. Killed in cleanup().
EVENTS_LOG="$WORKDIR/db2-events.log"
docker events --filter "container=$CONTAINER" >"$EVENTS_LOG" 2>&1 &
EVENTS_PID=$!

PORT_LINE="$(docker port "$CONTAINER" 50000/tcp | head -n 1)"
HOST_PORT="${PORT_LINE##*:}"
case "$HOST_PORT" in ''|*[!0-9]*) die "could not determine the mapped host port (got '$PORT_LINE')" ;; esac
log "Host port: $HOST_PORT"

# --------------------------------------------------------------- readiness ---
# Ready = the image's setup finished, the database accepts a real SQL
# statement, and the mapped TCP port accepts connections.
db2_sql() {
  docker exec "$CONTAINER" su - "$DB_USER" -c \
    "db2 connect to $DB_NAME >/dev/null && db2 -x \"$1\" ; rc=\$?; db2 terminate >/dev/null 2>&1; exit \$rc"
}
tcp_open() { (exec 3<>"/dev/tcp/127.0.0.1/$HOST_PORT") 2>/dev/null; }

STAGE="waiting for Db2"
log "Waiting for Db2 (timeout ${READY_TIMEOUT}s)..."
started=$SECONDS
next_progress=30
ready=0
setup_seen=0
createdb_attempted=0
while [ $((SECONDS - started)) -lt "$READY_TIMEOUT" ]; do
  state="$(docker inspect -f '{{.State.Status}}' "$CONTAINER" 2>/dev/null || echo gone)"
  if [ "$state" != "running" ]; then
    detail="$(docker inspect -f 'OOMKilled={{.State.OOMKilled}} ExitCode={{.State.ExitCode}} Error={{.State.Error}}' "$CONTAINER" 2>/dev/null || echo '(container gone: removed externally while Db2 was starting?)')"
    die "Db2 container stopped unexpectedly (state: $state; $detail). Rerun with DB2_IT_KEEP_CONTAINER=1 to inspect, and check 'docker events' / dmesg for OOM kills."
  fi
  # NOTE: do NOT use `docker logs ... | grep -q` here. With `set -o pipefail`,
  # grep -q closes the pipe on first match, docker logs dies with SIGPIPE
  # (141), and the pipeline evaluates as failed even though the text is
  # present. That made readiness wait out the full timeout and then delete a
  # healthy container. Capture first, match second (no pipe, no race).
  container_logs="$(docker logs "$CONTAINER" 2>&1 || true)"
  if [[ "$container_logs" == *"Setup has completed"* ]]; then
    if [ "$setup_seen" -eq 0 ]; then setup_seen=$SECONDS; fi
    if db2_sql "VALUES 1" >/dev/null 2>&1 && tcp_open; then ready=1; break; fi
    # The community image sometimes prints "Setup has completed" while its
    # internal CREATE DATABASE was silently skipped (no testdb, SQL1013N on
    # connect). After 120 s of failing SQL with setup done, create the
    # database explicitly once with the same codeset/territory and keep polling.
    if [ "$createdb_attempted" -eq 0 ] && [ $((SECONDS - setup_seen)) -ge 120 ]; then
      createdb_attempted=1
      log "Setup completed without database $DB_NAME; creating it explicitly..."
      docker exec "$CONTAINER" su - "$DB_USER" -c \
        "db2 create db $DB_NAME using codeset utf-8 territory us collate using identity" >/dev/null 2>&1 || true
      docker exec "$CONTAINER" su - "$DB_USER" -c \
        "db2 activate db $DB_NAME" >/dev/null 2>&1 || true
    fi
  fi
  elapsed=$((SECONDS - started))
  if [ "$elapsed" -ge "$next_progress" ]; then log "  still waiting... ${elapsed}s"; next_progress=$((next_progress + 30)); fi
  sleep 3
done
[ "$ready" -eq 1 ] || die "Db2 did not become ready within ${READY_TIMEOUT} seconds."
log "Db2 is ready ($((SECONDS - started))s)"

# ---------------------------------------------------------------- fixtures ---
STAGE="preparing fixtures"
log "Preparing fixtures (schema $SCHEMA)..."
db2_sql "CREATE SCHEMA $SCHEMA" >/dev/null || die "could not create schema $SCHEMA"
# Same pipefail/SIGPIPE avoidance as in the readiness loop: capture first,
# match second (db2 output here is one line, but the pattern is identical).
schema_check="$(db2_sql "SELECT COUNT(*) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = '$SCHEMA'" || echo __DB2SQL_FAILED__)"
[[ "$schema_check" =~ ^[[:space:]]*1[[:space:]]*$ ]] \
  || die "schema $SCHEMA was not created"
# Minimal P1BENCH.INT_1M corpus for --full discard/block-probe suites, which
# only fetch the first 1000/5000 ordered rows. The throwaway container starts
# empty, so create 5000 deterministic rows here instead of failing later with
# "P1BENCH.INT_1M missing".
db2_sql "CREATE SCHEMA P1BENCH" >/dev/null 2>&1 || true
db2_sql "CREATE TABLE P1BENCH.INT_1M (ID INTEGER NOT NULL PRIMARY KEY)" >/dev/null \
  || die "could not create table P1BENCH.INT_1M"
db2_sql "INSERT INTO P1BENCH.INT_1M WITH N(N) AS (SELECT 1 FROM SYSIBM.SYSDUMMY1 UNION ALL SELECT N+1 FROM N WHERE N < 5000) SELECT N FROM N" >/dev/null \
  || die "could not populate P1BENCH.INT_1M (5000 rows)"

# ------------------------------------------------------------- test run ------
# Clear everything that could point the tests at some other database, then set
# exactly one connection description: the container started above.
while IFS= read -r name; do unset "$name"; done < <(compgen -e | grep '^DB2_' || true)
export DB2_TEST_DSN="db2://${DB_USER}:${PASSWORD}@127.0.0.1:${HOST_PORT}/${DB_NAME}"
export DB2_LIVE_TEST_HOST="127.0.0.1"
export DB2_LIVE_TEST_PORT="$HOST_PORT"
export DB2_LIVE_TEST_DATABASE="$DB_NAME"
export DB2_LIVE_TEST_USER="$DB_USER"
export DB2_LIVE_TEST_PASSWORD="$PASSWORD"
export DB2_IT_SCHEMA="$SCHEMA"
export DB2_TEST_USER="$DB_USER"
export DB2_TEST_PASSWORD="$PASSWORD"
export DB2_TEST_JDBC_URL="jdbc:db2://127.0.0.1:${HOST_PORT}/${DB_NAME}"
export DB2_TEST_DOTNET_CONNECTION_STRING="Server=127.0.0.1:${HOST_PORT};Database=${DB_NAME};UID=${DB_USER};PWD=${PASSWORD};"
export DB2_PARITY_CASES="$REPO_ROOT/tests/parity/cases.tsv"
export DB2_PARITY_TIER="smoke"
export DB2_PARITY_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
export DB2_PARITY_DB_VERSION="$(db2_sql "SELECT SERVICE_LEVEL FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS T" 2>/dev/null | tr -d '[:space:]' || true)"
if [ -z "$DB2_PARITY_DB_VERSION" ]; then DB2_PARITY_DB_VERSION="$(docker exec "$CONTAINER" su - "$DB_USER" -c 'db2level' 2>/dev/null | sed -n 's/.*DB2 code release "\([^"]*\)".*/\1/p' | head -n1)"; fi
export DB2_PARITY_JAVA_VERSION="$(java -version 2>&1 | head -n1)"
export DB2_PARITY_DOTNET_RUNTIME="$(dotnet --version)"
# Pinned IBM provider versions, kept next to the .NET/JCC worker invocations
# below. Exported here so failure reports and logs reference them even when a
# worker stage never runs.
export DB2_PARITY_DOTNET_VERSION=10.0.0.300 DB2_PARITY_JCC_VERSION=12.1.5.0
# Debug test threads default to a 2 MiB stack, which overflows in
# live_row_import_pipes_progressive_blob_with_bounded_memory (deep but bounded
# recursion on the progressive-LOB path; release builds are unaffected).
# Run the live gate with a larger minimum stack until that path is iterative.
export RUST_MIN_STACK=67108864

STAGE="running tests"
log "Running integration tests ($SUITE suite)..."
cd "$REPO_ROOT"
if [ -n "$REPORT_DIR_INPUT" ]; then
  mkdir -p "$REPORT_DIR_INPUT"
  REPORT_DIR="$(cd "$REPORT_DIR_INPUT" && pwd)"
else
  # Keep the default log outside WORKDIR: cleanup() removes WORKDIR on every
  # exit, which previously deleted the very log the PASS line printed.
  REPORT_DIR="$REPO_ROOT/target/db2-it-results"
  mkdir -p "$REPORT_DIR"
fi
RESULT_NAME="${RUN_TAG}-${SUITE}"
TEST_LOG="$REPORT_DIR/${RESULT_NAME}.log"
if [ "$SUITE" = "full" ]; then
  set +e
  cargo test --locked --test live --test isolation --test parameter_boundaries --test discard_live --test block_probe_live -- --ignored --test-threads=1 2>&1 | redact | tee "$TEST_LOG"
  rc=("${PIPESTATUS[@]}")
  set -e
  [ "${rc[0]}" -eq 0 ] || exit "${rc[0]}"
elif [ "$SUITE" = "smoke" ]; then
  set +e
  cargo test --locked --test db2_smoke -- --ignored --test-threads=1 2>&1 | redact | tee "$TEST_LOG"
  rc=("${PIPESTATUS[@]}")
  set -e
  [ "${rc[0]}" -eq 0 ] || exit "${rc[0]}"
else
  command -v dotnet >/dev/null 2>&1 || die ".NET 10 SDK is required for parity workers."
  command -v java >/dev/null 2>&1 || die "Java 17 is required for parity workers."
  command -v mvn >/dev/null 2>&1 || die "Maven is required for parity workers."
  if [ "$SUITE" = "parity-full" ]; then export DB2_PARITY_TIER=full; fi
  mkdir -p "$REPO_ROOT/target/db2-parity"
  export DB2_PARITY_OUTPUT="$REPO_ROOT/target/db2-parity/rust.jsonl"
  export DB2_PARITY_REPORT="$REPO_ROOT/target/db2-parity/report.txt"
  STAGE="running Rust parity cases"
  set +e
  cargo test --locked --test parity -- --ignored --test-threads=1 2>&1 | redact | tee "$TEST_LOG"
  rc=("${PIPESTATUS[@]}")
  set -e
  [ "${rc[0]}" -eq 0 ] || exit "${rc[0]}"
  STAGE="running IBM .NET parity worker"
  dotnet restore tools/parity/dotnet/Parity.csproj --locked-mode
  # The -lnx NuGet package deploys a Linux clidriver next to the app; its
  # lib/ must be on the loader path or IBM.Data.Db2 fails with
  # DllNotFoundException for libdb2.so (same failure class as the Windows
  # package's missing db2app64.dll).
  DOTNET_CLIDRIVER="$REPO_ROOT/tools/parity/dotnet/bin/Debug/net10.0/clidriver"
  export DB2_CLI_DRIVER_INSTALL_PATH="$DOTNET_CLIDRIVER"
  export LD_LIBRARY_PATH="$DOTNET_CLIDRIVER/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  dotnet run --no-restore --project tools/parity/dotnet/Parity.csproj -- --all > "$REPO_ROOT/target/db2-parity/dotnet.jsonl" 2> >(redact >&2)
  STAGE="running IBM JCC parity worker"
  MAVEN_REPO="${DB2_PARITY_MAVEN_REPO:-$REPO_ROOT/target/db2-parity/m2}"
  mvn -q -Dmaven.repo.local="$MAVEN_REPO" -f tools/parity/jdbc/pom.xml package
  JCC_JAR="$MAVEN_REPO/com/ibm/db2/jcc/12.1.5.0/jcc-12.1.5.0.jar"
  java -cp "tools/parity/jdbc/target/classes:$JCC_JAR" org.justybase.parity.Worker --all > "$REPO_ROOT/target/db2-parity/jcc.jsonl" 2> >(redact >&2)
  STAGE="comparing canonical results"
  python3 tools/parity/compare.py "$REPO_ROOT/target/db2-parity/rust.jsonl" "$REPO_ROOT/target/db2-parity/dotnet.jsonl" "$REPO_ROOT/target/db2-parity/jcc.jsonl" | tee -a "$TEST_LOG"
  cat "$REPO_ROOT/target/db2-parity/report.txt" >> "$TEST_LOG"
fi

log "PASS (log: $TEST_LOG)"

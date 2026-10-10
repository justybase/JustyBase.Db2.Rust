#!/usr/bin/env bash
# Local Db2 integration tests for justybase-db2.
#
# Starts a throwaway IBM Db2 Community container, waits until it really accepts
# SQL, runs the live integration tests against THAT container only, and removes
# the container (and its volumes) afterwards, also on failure or Ctrl-C.
#
# Usage: scripts/test-db2-integration.sh [--full]
#   (default)  small smoke suite: connect, DML/types/NULLs, parameters,
#              transactions, result shape, error propagation
#   --full     every existing live test of this repository that needs only the
#              one database started here (takes several minutes)
#
# Environment overrides (all optional):
#   DB2_IT_IMAGE          Db2 image (default below; must not be a floating tag)
#   DB2_IT_READY_TIMEOUT  seconds to wait for Db2 (default 480)
#   DB2_IT_REPORT_DIR     optional directory for redacted console output
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
          echo "[db2-it] ---- docker logs $CONTAINER (last 200 lines) ----"
          docker logs --tail 200 "$CONTAINER" 2>&1 | redact
        } >&2
      fi
    fi
  fi

  if [ -n "$CONTAINER" ]; then
    log "Removing Db2 container $CONTAINER..."
    docker rm -f -v "$CONTAINER" >/dev/null 2>&1
    if docker inspect "$CONTAINER" >/dev/null 2>&1; then
      warn "container $CONTAINER still exists; remove it with: docker rm -f -v $CONTAINER"
      cleanup_failed=1
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
  [ "$state" = "running" ] || die "Db2 container stopped unexpectedly (state: $state)."
  if docker logs "$CONTAINER" 2>&1 | grep -q 'Setup has completed'; then
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
db2_sql "SELECT COUNT(*) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = '$SCHEMA'" | grep -qx '[[:space:]]*1' \
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
# Debug test threads default to a 2 MiB stack, which overflows in
# live_row_import_pipes_progressive_blob_with_bounded_memory (deep but bounded
# recursion on the progressive-LOB path; release builds are unaffected).
# Run the live gate with a larger minimum stack until that path is iterative.
export RUST_MIN_STACK=67108864

STAGE="running tests"
log "Running integration tests ($SUITE suite)..."
cd "$REPO_ROOT"
REPORT_DIR_INPUT="${DB2_IT_REPORT_DIR:-}"
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
else
  set +e
  cargo test --locked --test db2_smoke -- --ignored --test-threads=1 2>&1 | redact | tee "$TEST_LOG"
  rc=("${PIPESTATUS[@]}")
  set -e
  [ "${rc[0]}" -eq 0 ] || exit "${rc[0]}"
fi

log "PASS (log: $TEST_LOG)"

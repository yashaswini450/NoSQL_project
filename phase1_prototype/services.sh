#!/usr/bin/env bash
set -euo pipefail
STATE_ROOT="${NASA_STATE_DIR:-$HOME/.local/share/nasa-phase1}"
PG_BIN=/usr/lib/postgresql/16/bin
mkdir -p "$STATE_ROOT"/{postgres,mongo-data,logs}
case "${1:-start}" in
start)
  if ! test -f "$STATE_ROOT/postgres/PG_VERSION"; then
    "$PG_BIN/initdb" -D "$STATE_ROOT/postgres" --auth-local=trust --auth-host=scram-sha-256 --username=nasa --encoding=UTF8 --locale=C > "$STATE_ROOT/logs/initdb.log"
  fi
  if ! "$PG_BIN/pg_ctl" -D "$STATE_ROOT/postgres" status >/dev/null 2>&1; then
    "$PG_BIN/pg_ctl" -D "$STATE_ROOT/postgres" -o "-p 55432 -h 127.0.0.1 -k $STATE_ROOT" -l "$STATE_ROOT/logs/postgres.log" start
  fi
  # Private local database; TCP requires a password, local socket is restricted by the user's directory.
  "$PG_BIN/psql" -h "$STATE_ROOT" -p 55432 -U nasa -d postgres -v ON_ERROR_STOP=1 -c "ALTER ROLE nasa PASSWORD 'nasa_local_demo';" >/dev/null
  "$PG_BIN/psql" -h "$STATE_ROOT" -p 55432 -U nasa -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='nasa_reporting'" | grep -q 1 || "$PG_BIN/createdb" -h "$STATE_ROOT" -p 55432 -U nasa nasa_reporting
  if ! "$STATE_ROOT/venv/bin/python" -c 'from pymongo import MongoClient; MongoClient("mongodb://127.0.0.1:27018",serverSelectionTimeoutMS=1500).admin.command("ping")' >/dev/null 2>&1; then
    "$STATE_ROOT/mongodb/bin/mongod" --dbpath "$STATE_ROOT/mongo-data" --port 27018 --bind_ip 127.0.0.1 --wiredTigerCacheSizeGB 1 --logpath "$STATE_ROOT/logs/mongodb.log" --fork
  fi
  echo 'MongoDB: 127.0.0.1:27018 | PostgreSQL: 127.0.0.1:55432/nasa_reporting'
  ;;
stop)
  "$PG_BIN/pg_ctl" -D "$STATE_ROOT/postgres" stop -m fast || true
  if test -f "$STATE_ROOT/mongo-data/mongod.lock"; then
    mongo_pid=$(cat "$STATE_ROOT/mongo-data/mongod.lock")
    test -z "$mongo_pid" || kill -TERM "$mongo_pid"
  fi
  ;;
*) echo 'Usage: bash services.sh start|stop' >&2; exit 2;;
esac

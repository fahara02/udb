#!/usr/bin/env bash
# scripts/playground.sh — Start the UDB local playground (Linux / macOS)
# Phase 7.3 of the UDB Critical Feature Implementation Plan.
#
# Usage:
#   ./scripts/playground.sh          # Start all services
#   ./scripts/playground.sh down     # Stop and remove containers
#   ./scripts/playground.sh logs     # Tail logs
#   ./scripts/playground.sh status   # Show service status
#   ./scripts/playground.sh reset    # Nuke volumes and restart

set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "$0")/.." && pwd)/docker-compose.playground.yml"
DC="docker compose -f $COMPOSE_FILE"

cmd="${1:-up}"

case "$cmd" in
  up|start)
    echo "Starting UDB playground…"
    $DC up -d --build
    echo ""
    echo "Services:"
    $DC ps
    echo ""
    echo "PostgreSQL : localhost:5432 (udb/udb/udb_dev)"
    echo "Redis      : localhost:6379"
    echo "Qdrant     : http://localhost:6333"
    echo "MinIO API  : http://localhost:9000  (minioadmin/minioadmin)"
    echo "MinIO UI   : http://localhost:9001"
    echo "Kafka      : localhost:9094"
    echo "UDB gRPC   : localhost:50051"
    ;;

  down|stop)
    echo "Stopping UDB playground…"
    $DC down
    ;;

  logs)
    $DC logs -f "${2:-udb}"
    ;;

  status|ps)
    $DC ps
    ;;

  reset)
    echo "Resetting UDB playground (all volumes will be deleted)…"
    $DC down -v
    $DC up -d --build
    ;;

  smoke|test)
    "$(dirname "$0")/smoke_test.sh"
    ;;

  *)
    echo "Unknown command: $cmd"
    echo "Usage: $0 {up|down|logs|status|reset|smoke}"
    exit 1
    ;;
esac

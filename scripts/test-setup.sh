#!/bin/bash
# SoftProbe OTLP Backend - Test Infrastructure Setup Script
#
# This script helps set up and verify the test environment for integration tests.
# It can be used for local development, CI/CD, or troubleshooting.
#
# Usage:
#   ./scripts/test-setup.sh [command]
#
# Commands:
#   setup       - Start local test infrastructure (MinIO + Iceberg REST)
#   teardown    - Stop local test infrastructure
#   verify      - Verify infrastructure is running correctly
#   reset       - Reset test infrastructure (teardown + setup)
#   logs        - Show logs from test services
#   help        - Show this help message

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
info() { echo -e "${BLUE}ℹ${NC} $1"; }
success() { echo -e "${GREEN}✅${NC} $1"; }
warning() { echo -e "${YELLOW}⚠${NC} $1"; }
error() { echo -e "${RED}❌${NC} $1"; }

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Function to check if docker-compose is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
        echo "Please install Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi

    if ! docker compose version &> /dev/null && ! command -v docker-compose &> /dev/null; then
        error "docker-compose is not available"
        echo "Please install docker-compose or use Docker with Compose V2"
        exit 1
    fi

    success "Docker is available"
}

# Function to start test infrastructure
setup_infrastructure() {
    info "Starting test infrastructure..."

    check_docker

    cd "$PROJECT_ROOT"

    info "Starting MinIO and Iceberg REST services..."
    if docker compose version &> /dev/null; then
        docker compose up -d minio iceberg-rest
    else
        docker-compose up -d minio iceberg-rest
    fi

    info "Waiting for services to be healthy..."
    sleep 5

    # Wait for MinIO
    info "Checking MinIO health..."
    for i in {1..30}; do
        if curl -sf http://localhost:9002/minio/health/live > /dev/null 2>&1; then
            success "MinIO is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            error "MinIO failed to start after 60 seconds"
            exit 1
        fi
        echo -n "."
        sleep 2
    done

    # Wait for Iceberg REST
    info "Checking Iceberg REST health..."
    for i in {1..30}; do
        if curl -sf http://localhost:8181/v1/config > /dev/null 2>&1; then
            success "Iceberg REST is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            error "Iceberg REST failed to start after 60 seconds"
            exit 1
        fi
        echo -n "."
        sleep 2
    done

    echo ""
    success "Test infrastructure is ready!"
    echo ""
    echo "Services available:"
    echo "  🗄️  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    echo "  🗄️  MinIO API: http://localhost:9002"
    echo "  📊 Iceberg REST: http://localhost:8181"
    echo ""
    echo "To run tests:"
    echo "  cd softprobe-otlp-backend"
    echo "  make test-local"
}

# Function to stop test infrastructure
teardown_infrastructure() {
    info "Stopping test infrastructure..."

    cd "$PROJECT_ROOT"

    if docker compose version &> /dev/null; then
        docker compose down -v
    else
        docker-compose down -v
    fi

    success "Test infrastructure stopped and volumes removed"
}

# Function to verify infrastructure is running
verify_infrastructure() {
    info "Verifying test infrastructure..."

    local all_ok=true

    # Check MinIO
    if curl -sf http://localhost:9002/minio/health/live > /dev/null 2>&1; then
        success "MinIO is running (http://localhost:9002)"
    else
        error "MinIO is not running"
        all_ok=false
    fi

    # Check Iceberg REST
    if curl -sf http://localhost:8181/v1/config > /dev/null 2>&1; then
        success "Iceberg REST is running (http://localhost:8181)"

        # Get catalog config
        info "Catalog configuration:"
        curl -s http://localhost:8181/v1/config | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8181/v1/config
    else
        error "Iceberg REST is not running"
        all_ok=false
    fi

    echo ""

    if [ "$all_ok" = true ]; then
        success "All services are running correctly"
        echo ""
        echo "Ready to run tests with:"
        echo "  cd softprobe-otlp-backend && make test-local"
        return 0
    else
        error "Some services are not running"
        echo ""
        echo "To start services, run:"
        echo "  ./scripts/test-setup.sh setup"
        return 1
    fi
}

# Function to reset infrastructure
reset_infrastructure() {
    info "Resetting test infrastructure..."
    teardown_infrastructure
    sleep 2
    setup_infrastructure
}

# Function to show logs
show_logs() {
    cd "$PROJECT_ROOT"

    info "Showing logs from test services..."
    echo ""

    if docker compose version &> /dev/null; then
        docker compose logs -f minio iceberg-rest
    else
        docker-compose logs -f minio iceberg-rest
    fi
}

# Function to show help
show_help() {
    echo "SoftProbe OTLP Backend - Test Infrastructure Setup"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  setup       - Start local test infrastructure (MinIO + Iceberg REST)"
    echo "  teardown    - Stop local test infrastructure"
    echo "  verify      - Verify infrastructure is running correctly"
    echo "  reset       - Reset test infrastructure (teardown + setup)"
    echo "  logs        - Show logs from test services"
    echo "  help        - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 setup     # Start services"
    echo "  $0 verify    # Check if services are running"
    echo "  $0 logs      # View service logs"
    echo ""
    echo "After setup, run tests with:"
    echo "  cd softprobe-otlp-backend"
    echo "  make test-local"
}

# Main command dispatcher
case "${1:-help}" in
    setup)
        setup_infrastructure
        ;;
    teardown)
        teardown_infrastructure
        ;;
    verify)
        verify_infrastructure
        ;;
    reset)
        reset_infrastructure
        ;;
    logs)
        show_logs
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac

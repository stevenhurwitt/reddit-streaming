#!/bin/bash
"""
Helper script to run curation tests on Docker Compose Spark cluster.

Usage:
  ./run_tests_docker.sh [test|diagnose|curation] [--job news] [--limit 100]

Examples:
  ./run_tests_docker.sh diagnose                    # Run full diagnostics
  ./run_tests_docker.sh test postgres              # Test PostgreSQL only
  ./run_tests_docker.sh test jdbc                  # Test JDBC driver
  ./run_tests_docker.sh test full                  # Full integration test
  ./run_tests_docker.sh curation --job news        # Run actual curation job
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SPARK_CONTAINER="reddit-spark-master"
POSTGRES_HOST="reddit-postgres"
POSTGRES_PORT="5432"
WORKSPACE="/opt/workspace/local_jobs"
JOB="news"
LIMIT="100"

print_header() {
    echo -e "${BLUE}═══════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check if Docker Compose services are running
check_services() {
    print_header "Checking Docker Services"
    
    local spark_running=$(docker ps | grep -c $SPARK_CONTAINER || true)
    local postgres_running=$(docker ps | grep -c reddit-postgres || true)
    
    if [ $spark_running -eq 0 ]; then
        print_error "Spark container ($SPARK_CONTAINER) is not running"
        echo "Start services with: docker-compose up -d"
        exit 1
    fi
    print_success "Spark container is running"
    
    if [ $postgres_running -eq 0 ]; then
        print_error "PostgreSQL container is not running"
        echo "Start services with: docker-compose up -d"
        exit 1
    fi
    print_success "PostgreSQL container is running"
    
    echo ""
}

# Test PostgreSQL directly
test_postgres() {
    print_header "Testing PostgreSQL Connection (Direct)"
    
    docker exec -it $SPARK_CONTAINER python $WORKSPACE/test_postgres_direct.py \
        --host $POSTGRES_HOST \
        --port $POSTGRES_PORT
}

# Test Spark JDBC
test_jdbc() {
    print_header "Testing Spark JDBC"
    
    docker exec -it $SPARK_CONTAINER python $WORKSPACE/test_spark_jdbc.py \
        --host $POSTGRES_HOST \
        --port $POSTGRES_PORT
}

# Full integration test
test_full() {
    print_header "Testing Full Integration (S3 → Delta → PostgreSQL)"
    
    docker exec -it $SPARK_CONTAINER python $WORKSPACE/test_curation_postgres.py \
        --job $JOB \
        --db-host $POSTGRES_HOST \
        --db-port $POSTGRES_PORT \
        --limit $LIMIT
}

# Run diagnostics
run_diagnostics() {
    print_header "Running Comprehensive Diagnostics"
    
    docker exec -it $SPARK_CONTAINER python $WORKSPACE/diagnose_postgres_write.py \
        --job $JOB \
        --db-host $POSTGRES_HOST \
        --db-port $POSTGRES_PORT
}

# Run actual curation job on cluster
run_curation_job() {
    print_header "Running Curation Job on Spark Cluster"
    
    docker exec -it $SPARK_CONTAINER bash -c "
        cd $WORKSPACE
        python run_curation_job.py \
            --job $JOB \
            --spark-master spark://spark-master:7077
    "
}

# Run curation using spark-submit for distributed processing
run_curation_spark_submit() {
    print_header "Running Curation Job via spark-submit (Distributed)"
    
    docker exec -it $SPARK_CONTAINER bash -c "
        cd $WORKSPACE
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 2g \
            --executor-memory 2g \
            --num-executors 2 \
            run_curation_job.py --job $JOB
    "
}

# Run curation using spark-submit with streaming
run_curation_spark_submit_streaming() {
    print_header "Running Curation Job via spark-submit (Streaming)"
    
    docker exec -it $SPARK_CONTAINER bash -c "
        cd $WORKSPACE
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode cluster \
            --driver-memory 2g \
            --executor-memory 2g \
            --num-executors 4 \
            --driver-cores 2 \
            --executor-cores 2 \
            --conf spark.driver.maxResultSize=0 \
            run_curation_job.py --job $JOB
    "
}

# Interactive shell in container
shell() {
    print_header "Entering Spark Container Shell"
    print_info "Run: cd /opt/workspace/local_jobs && python test_postgres_direct.py --host reddit-postgres"
    docker exec -it $SPARK_CONTAINER bash
}

# Monitor Spark cluster
monitor() {
    print_header "Spark Cluster Information"
    
    echo "Spark Master UI: http://localhost:8085"
    echo ""
    
    docker exec $SPARK_CONTAINER bash -c "
        curl -s http://spark-master:8080/api/v1/applications | python -m json.tool 2>/dev/null || echo 'Could not fetch cluster info'
    "
}

# Show logs
show_logs() {
    print_header "Docker Compose Logs"
    
    docker-compose logs --tail=100 $SPARK_CONTAINER
}

# Parse arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 [test|diagnose|curation|shell|monitor|logs] [--job JOB] [--limit LIMIT]"
    echo ""
    echo "Commands:"
    echo "  diagnose         Run comprehensive diagnostics"
    echo "  test postgres    Test PostgreSQL connection"
    echo "  test jdbc        Test Spark JDBC driver"
    echo "  test full        Run full integration test"
    echo "  curation         Run actual curation job (local mode)"
    echo "  submit           Run curation job via spark-submit (cluster mode)"
    echo "  shell            Open interactive shell in container"
    echo "  monitor          Show Spark cluster info"
    echo "  logs             Show container logs"
    echo ""
    echo "Examples:"
    echo "  $0 diagnose --job news"
    echo "  $0 test full --job technology --limit 1000"
    echo "  $0 curation --job news"
    echo "  $0 submit --job news"
    exit 1
fi

# Parse command
COMMAND=$1
shift

# Parse additional arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --job)
            JOB="$2"
            shift 2
            ;;
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Check services first
check_services

# Execute command
case $COMMAND in
    diagnose)
        run_diagnostics
        ;;
    test)
        case $1 in
            postgres)
                test_postgres
                ;;
            jdbc)
                test_jdbc
                ;;
            full)
                test_full
                ;;
            *)
                echo "Unknown test: $1"
                echo "Valid tests: postgres, jdbc, full"
                exit 1
                ;;
        esac
        ;;
    curation)
        run_curation_job
        ;;
    submit)
        run_curation_spark_submit
        ;;
    submit-streaming)
        run_curation_spark_submit_streaming
        ;;
    shell)
        shell
        ;;
    monitor)
        monitor
        ;;
    logs)
        show_logs
        ;;
    *)
        echo "Unknown command: $COMMAND"
        exit 1
        ;;
esac

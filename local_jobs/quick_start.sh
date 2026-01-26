#!/bin/bash
# quick_start.sh - Quick setup and test of local cron jobs

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "=========================================="
echo "Reddit Streaming - Local Cron Job Setup"
echo "=========================================="
echo ""

# Step 1: Initial setup
echo "[1/4] Running initial setup..."
bash "$SCRIPT_DIR/setup_local_jobs.sh"
echo ""

# Step 2: Check credentials
echo "[2/4] Checking AWS credentials..."
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
if [ -f "$PROJECT_ROOT/aws_access_key.txt" ] && [ -f "$PROJECT_ROOT/aws_secret.txt" ]; then
    echo "✓ AWS credentials found"
else
    echo "⚠ AWS credentials not found in project root"
    echo "  You'll need to copy them before jobs can run:"
    echo "    cp /path/to/aws_access_key.txt $PROJECT_ROOT/"
    echo "    cp /path/to/aws_secret.txt $PROJECT_ROOT/"
fi
echo ""

# Step 3: Test a job
echo "[3/4] Testing a curation job (this may take a minute)..."
echo "Running: news curation job..."
source "$SCRIPT_DIR/venv/bin/activate"
cd "$SCRIPT_DIR"
python3 run_curation_job.py --job news 2>&1 | head -20 || true
echo ""
echo "✓ Test complete (check logs/ directory for full output)"
echo ""

# Step 4: Setup cron
echo "[4/4] Ready to configure cron jobs?"
echo ""
echo "To set up automated cron scheduling, run:"
echo "  bash $SCRIPT_DIR/setup_cron.sh"
echo ""
echo "This will schedule jobs to run daily at midnight UTC:"
echo "  • news"
echo "  • technology"
echo "  • ProgrammerHumor"
echo "  • worldnews"
echo ""
echo "For more details, see: $SCRIPT_DIR/README.md"

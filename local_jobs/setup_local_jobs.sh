#!/bin/bash
# setup_local_jobs.sh - Install dependencies for local cron-based job execution
# Run this once before setting up cron jobs

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Setting up local curation jobs..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "ERROR: uv is not installed"
    echo "Install uv from: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi

echo "✓ uv found: $(uv --version)"

# Create virtual environment
VENV_DIR="$SCRIPT_DIR/.venv"
if [ -d "$VENV_DIR" ]; then
    echo "Virtual environment already exists at $VENV_DIR"
else
    echo "Creating virtual environment..."
    uv venv "$VENV_DIR"
    echo "✓ Virtual environment created"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"
echo "✓ Virtual environment activated"

# Install dependencies
echo "Installing dependencies..."
uv pip install boto3 pyspark delta-spark

echo "✓ Dependencies installed successfully"

# Create logs directory
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
echo "✓ Logs directory created at $LOG_DIR"

# Verify the setup works
echo ""
echo "Verifying setup..."
python3 -c "import boto3; import pyspark; import delta; print('✓ All imports successful')"

echo ""
echo "Setup complete! Next steps:"
echo ""
echo "1. Copy AWS credentials to the project root:"
echo "   - aws_access_key.txt"
echo "   - aws_secret.txt"
echo ""
echo "2. Configure cron jobs by running:"
echo "   bash $SCRIPT_DIR/setup_cron.sh"
echo ""
echo "3. Test a job manually:"
echo "   source $VENV_DIR/bin/activate"
echo "   python3 $SCRIPT_DIR/run_curation_job.py --job news"

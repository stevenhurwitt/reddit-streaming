#!/bin/bash
# Test script to verify virtual environment and dependencies inside container

echo "=== Virtual Environment Test ==="
echo ""

# Check if running inside container
if [ -f /.dockerenv ]; then
    echo "✓ Running inside Docker container"
else
    echo "⚠ Not running inside Docker container"
fi

echo ""
echo "Working directory: $(pwd)"
echo ""

# Check for virtual environment
if [ -f /opt/workspace/.venv/bin/activate ]; then
    echo "✓ Virtual environment found at /opt/workspace/.venv"
    source /opt/workspace/.venv/bin/activate
    echo "✓ Virtual environment activated"
else
    echo "✗ Virtual environment NOT found at /opt/workspace/.venv"
    echo ""
    echo "To create it, run:"
    echo "  python3 -m venv /opt/workspace/.venv"
    echo "  source /opt/workspace/.venv/bin/activate"
    echo "  pip install -r /opt/workspace/redditStreaming/src/reddit/requirements_polars.txt"
    exit 1
fi

echo ""
echo "Python environment:"
echo "  Python path: $(which python)"
echo "  Python version: $(python --version)"
echo "  Pip version: $(pip --version)"

echo ""
echo "Checking required packages for Polars streaming:"

required_packages=(
    "polars"
    "deltalake"
    "confluent_kafka"
    "kafka"
    "s3fs"
    "yaml"
    "boto3"
    "praw"
)

all_installed=true

for package in "${required_packages[@]}"; do
    if python -c "import $package" 2>/dev/null; then
        version=$(python -c "import $package; print(getattr($package, '__version__', 'unknown'))" 2>/dev/null)
        echo "  ✓ $package ($version)"
    else
        echo "  ✗ $package - NOT INSTALLED"
        all_installed=false
    fi
done

echo ""
if [ "$all_installed" = true ]; then
    echo "✓ All required packages installed!"
    echo ""
    echo "You can now run:"
    echo "  python run_multi_polars.py"
    exit 0
else
    echo "⚠ Some packages are missing. Install with:"
    echo "  pip install -r /opt/workspace/redditStreaming/src/reddit/requirements_polars.txt"
    exit 1
fi

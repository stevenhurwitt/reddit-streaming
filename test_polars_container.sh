#!/bin/bash
# Test Polars environment inside the jupyterlab container

echo "Testing Polars environment inside container..."
echo ""

docker exec -it reddit-jupyterlab bash /opt/workspace/redditStreaming/test_polars_env.sh

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "✓ Environment is ready for Polars streaming!"
else
    echo ""
    echo "⚠ Environment setup needed. To fix:"
    echo ""
    echo "1. Enter the container:"
    echo "   docker exec -it reddit-jupyterlab bash"
    echo ""
    echo "2. Create and activate virtual environment:"
    echo "   cd /opt/workspace"
    echo "   python3 -m venv .venv"
    echo "   source .venv/bin/activate"
    echo ""
    echo "3. Install dependencies:"
    echo "   pip install -r redditStreaming/src/reddit/requirements_polars.txt"
    echo ""
    echo "4. Exit and test again:"
    echo "   exit"
    echo "   ./test_polars_container.sh"
fi

exit $exit_code

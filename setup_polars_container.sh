#!/bin/bash
# Setup Polars environment inside the jupyterlab container

echo "Setting up Polars environment in container..."
echo ""

# Run setup commands inside container
docker exec -it reddit-jupyterlab bash -c '
cd /opt/workspace

# Create virtual environment if it doesn'\''t exist
if [ ! -d .venv ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate and install dependencies
echo ""
echo "Installing dependencies..."
source .venv/bin/activate

pip install --upgrade pip setuptools wheel

echo ""
echo "Installing Polars requirements..."
pip install -r redditStreaming/src/reddit/requirements_polars.txt

echo ""
echo "Verifying installation..."
python -c "import polars; print(\"✓ Polars version:\", polars.__version__)"
python -c "import deltalake; print(\"✓ DeltaLake version:\", deltalake.__version__)"
python -c "from confluent_kafka import Consumer; print(\"✓ Confluent Kafka imported\")"
python -c "import s3fs; print(\"✓ S3FS version:\", s3fs.__version__)"

echo ""
echo "✓ Setup complete!"
'

echo ""
echo "Testing environment..."
./test_polars_container.sh

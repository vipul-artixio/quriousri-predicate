#!/bin/bash
# predicateAutomate/setup.sh
# Setup script for Predicate Automate

set -e

echo "================================="
echo "Predicate Automate Setup"
echo "================================="
echo ""

# Check Python version
echo "[1/5] Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Found Python $python_version"

# Create virtual environment
echo ""
echo "[2/5] Creating virtual environment..."
if [ -d "venv" ]; then
    echo "Virtual environment already exists, skipping..."
else
    python3 -m venv venv
    echo "✓ Virtual environment created"
fi

# Activate virtual environment
echo ""
echo "[3/5] Activating virtual environment..."
source venv/bin/activate
echo "✓ Activated"

# Install dependencies
echo ""
echo "[4/5] Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "✓ Dependencies installed"

# Setup environment file
echo ""
echo "[5/5] Setting up environment file..."
if [ -f ".env" ]; then
    echo ".env file already exists, skipping..."
else
    if [ -f "../.env.development" ]; then
        cp ../.env.development .env
        echo "✓ Copied .env.development to .env"
    else
        echo "⚠ Warning: No .env file found. Please create one manually."
    fi
fi

# Create output directories
echo ""
echo "Creating output directories..."
mkdir -p usa_drug/output
mkdir -p singapore_drug/output
echo "✓ Output directories created"

echo ""
echo "================================="
echo "Setup Complete!"
echo "================================="
echo ""
echo "Next steps:"
echo "1. Activate virtual environment: source venv/bin/activate"
echo "2. Run a module: python app.py usa_drug"
echo "3. Or run all: python app.py all"
echo "4. Or use Docker: docker-compose up usa-drug-fetcher"
echo ""
echo "For more info, see README.md"
echo ""


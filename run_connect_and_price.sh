#!/bin/bash
# Wrapper script to run connect_and_price.py with the venv's Python

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python3"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ ERROR: Virtual environment not found!"
    echo "   Expected: $VENV_PYTHON"
    exit 1
fi

"$VENV_PYTHON" "$SCRIPT_DIR/connect_and_price.py" "$@"


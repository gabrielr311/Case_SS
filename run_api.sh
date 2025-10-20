#!/bin/bash

# Name of the screen session
SESSION="fast_api"

# Path to your virtual environment
VENV="/home/user/case_ss/.venv/bin/activate"

source $VENV

# Path to your FastAPI app
APP="/home/user/case_ss/api.py"

# Activate virtual environment and run FastAPI in a detached screen
screen -dmS $SESSION bash -c "uvicorn api:app --host 0.0.0.0 --port 8000"

echo "FastAPI started in screen session '$SESSION'."

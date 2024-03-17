#!/bin/bash

# The path to the Python script you want to run repeatedly
PYTHON_SCRIPT="stream_bn.scripts.market_maker_main"
LOG_PATH="./stream_bn/logs/program_log.txt"
BASH_SCRIPT_PATH="./stream_bn/logs/program_scheduled_run_log.txt"

# Wipe out the file's content
truncate -s 0 "$LOG_PATH"


# Loop indefinitely
while true; do
    # Define the range for the random timeout
    MIN=250
    MAX=300
    # Generate a random number within the range for the timeout duration
    RANGE=$((MAX - MIN + 1))
    RANDOM_NUMBER=$(($RANDOM % RANGE + MIN))

    # Use the generated random number as the timeout duration
    timeout $RANDOM_NUMBER python -m $PYTHON_SCRIPT
    echo "Program restarted at: $(date +"%Y-%m-%d %H:%M:%S")" >> $BASH_SCRIPT_PATH
done

#!/bin/bash
set -e 
echo "Starting the application..."
python src/api.py 
tail -f /dev/null
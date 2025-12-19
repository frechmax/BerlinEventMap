#!/usr/bin/env python3
"""
Script to automatically fetch events for a specific date or today
"""
import subprocess
import sys
import os
from datetime import datetime

def validate_date(date_string):
    """Validate date format YYYY-MM-DD"""
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def main():
    # Get date from command line argument or use today's date
    if len(sys.argv) > 1:
        date = sys.argv[1]
        if not validate_date(date):
            print(f"Error: Invalid date format '{date}'")
            print("Please use YYYY-MM-DD format")
            return 1
    else:
        date = datetime.now().strftime("%Y-%m-%d")
    
    # Build the command with the specified date
    output_file = f"RA_{date}_events.csv"
    
    # Use the virtual environment's Python executable
    venv_python = os.path.join(os.path.dirname(__file__), '.venv', 'Scripts', 'python.exe')
    if os.path.exists(venv_python):
        python_exe = venv_python
    else:
        python_exe = "python"
    
    command = [
        python_exe,
        "RA_event_fetcher.py",
        "34",
        date,
        date,
        "-o",
        output_file
    ]
    
    print(f"Running command for {date}...")
    print(f"Command: {' '.join(command)}")
    print()
    
    # Execute the command
    result = subprocess.run(command)
    return result.returncode

if __name__ == "__main__":
    exit(main())

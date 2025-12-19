#!/usr/bin/env python3
"""
RA_run_today.py - Run RA Event Fetcher for today's date.

Wrapper script to automatically fetch Resident Advisor events for a specific
date or today by default.
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path


def validate_date(date_string: str) -> bool:
    """
    Validate date format YYYY-MM-DD.
    
    Args:
        date_string: The date string to validate.
        
    Returns:
        True if valid, False otherwise.
    """
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_python_executable() -> str:
    """
    Get the appropriate Python executable path.
    
    Checks for a virtual environment first, falls back to system Python.
    
    Returns:
        Path to the Python executable.
    """
    venv_python = Path(__file__).parent / ".venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def main() -> int:
    """
    Main entry point.
    
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    # Get date from command line argument or use today's date
    # Optionally accept output folder as second argument
    target_date = None
    output_folder = None
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        if not validate_date(target_date):
            print(f"Error: Invalid date format '{target_date}'")
            print("Please use YYYY-MM-DD format (e.g., 2025-12-19)")
            return 1
        if len(sys.argv) > 2:
            output_folder = sys.argv[2]
    else:
        target_date = datetime.now().strftime("%Y-%m-%d")

    if output_folder:
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        output_file = str(Path(output_folder) / f"RA_{target_date}_events.csv")
    else:
        output_file = f"RA_{target_date}_events.csv"

    python_exe = get_python_executable()

    command = [
        python_exe,
        "RA_event_fetcher.py",
        "34",  # Berlin area code
        target_date,
        target_date,
        "-o",
        output_file,
    ]

    print(f"Fetching RA events for {target_date}...")
    print(f"Command: {' '.join(command)}\n")

    result = subprocess.run(command, check=False)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())

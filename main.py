"""
main.py - Berlin Events Map Generator
Runs all scraping scripts and combines the results into one map.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
import glob
import subprocess
import sys
import time


@dataclass
class ScraperConfig:
    """Configuration for a single scraper."""
    name: str
    script: str
    output_csv: Optional[str]
    enabled: bool = True
    is_ra: bool = False


# Configuration
SCRAPERS = [
    ScraperConfig(
        name="Resident Advisor",
        script="RA_event_fetcher.py",
        output_csv=None,  # RA has dynamic naming (RA_YYYY-MM-DD_events.csv)
        enabled=True,
        is_ra=True
    ),
    ScraperConfig(
        name="tip Berlin",
        script="scrapeTipBerlinBot.py",
        output_csv="tip_berlin_events.csv",
        enabled=True
    ),
    ScraperConfig(
        name="Visit Berlin",
        script="scrapeVisitBerlin.py",
        output_csv="visitberlin_events.csv",
        enabled=True
    ),
    ScraperConfig(
        name="Gratis in Berlin",
        script="scrapeGratisInBerlinParallel.py",
        output_csv="gratis_berlin_events.csv",
        enabled=True
    ),
]

COMBINE_SCRIPT = "combineMapsLegend.py"


def print_header(text: str) -> None:
    """Print a formatted header."""
    print(f"\n{'=' * 70}")
    print(text)
    print(f"{'=' * 70}")


def print_step(text: str, script: str) -> None:
    """Print step information."""
    print(f"\n{'=' * 70}")
    print(f"🔄 Running: {text}")
    print(f"   Script: {script}")
    print(f"{'=' * 70}\n")


def run_script(
    script_name: str,
    description: str,
    output_folder: Optional[str] = None,
    is_ra: bool = False
) -> bool:
    """
    Run a Python script and handle errors.
    
    Args:
        script_name: Name of the script to run.
        description: Human-readable description for logging.
        output_folder: Optional folder path for script output.
        is_ra: Whether this is the Resident Advisor scraper.
        
    Returns:
        True if script ran successfully, False otherwise.
    """
    print_step(description, script_name)
    start_time = time.time()
    
    try:
        if is_ra:
            cmd = [sys.executable, "RA_run_today.py"]
        else:
            cmd = [sys.executable, script_name]
            if output_folder:
                cmd.append(output_folder)
        
        subprocess.run(cmd, check=True)
        
        elapsed = time.time() - start_time
        print(f"\n✓ {description} completed in {elapsed:.1f} seconds")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description} failed with error code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"\n✗ Script not found: {script_name}")
        return False
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        return False


def verify_output(scraper: ScraperConfig, run_folder: str) -> bool:
    """
    Verify that a scraper produced its expected output file.
    
    Args:
        scraper: Scraper configuration.
        run_folder: Folder where output files are stored.
        
    Returns:
        True if output file exists, False otherwise.
    """
    if scraper.is_ra:
        ra_files = glob.glob("RA_*_events.csv")
        if ra_files:
            output_path = ra_files[-1]
            print(f"   ✓ Output file created: {output_path}")
            return True
        print("   ⚠️  Warning: RA output file not found")
        return False
    
    if scraper.output_csv:
        output_path = Path(run_folder) / scraper.output_csv
        if output_path.exists():
            print(f"   ✓ Output file created: {output_path}")
            return True
        print(f"   ⚠️  Warning: Expected output file not found: {output_path}")
    return False


def run_scrapers(run_folder: str) -> tuple[list[str], list[str]]:
    """
    Run all enabled scrapers.
    
    Args:
        run_folder: Folder for output files.
        
    Returns:
        Tuple of (successful_scrapers, failed_scrapers) names.
    """
    successful: list[str] = []
    failed: list[str] = []
    
    for scraper in SCRAPERS:
        if not scraper.enabled:
            print(f"⏭️  Skipping {scraper.name} (disabled)")
            continue
        
        script_path = Path(scraper.script)
        if not script_path.exists():
            print(f"⚠️  Warning: {scraper.script} not found - skipping")
            failed.append(scraper.name)
            continue
        
        success = run_script(
            scraper.script,
            scraper.name,
            run_folder,
            scraper.is_ra
        )
        
        if success and verify_output(scraper, run_folder):
            successful.append(scraper.name)
        else:
            failed.append(scraper.name)
    
    return successful, failed


def print_scraping_summary(successful: list[str], failed: list[str]) -> None:
    """Print summary of scraping results."""
    print_header("SCRAPING SUMMARY")
    print(f"✓ Successful: {len(successful)}")
    for name in successful:
        print(f"   - {name}")
    
    if failed:
        print(f"\n✗ Failed: {len(failed)}")
        for name in failed:
            print(f"   - {name}")


def main() -> bool:
    """
    Main pipeline execution.
    
    Returns:
        True if pipeline completed successfully, False otherwise.
    """
    print_header("BERLIN EVENTS MAP - AUTOMATED PIPELINE")
    
    # Create timestamped output folder
    run_folder = Path("output") / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_folder.mkdir(parents=True, exist_ok=True)
    print(f"\n📁 Output folder: {run_folder}\n")
    
    total_start = time.time()
    
    # Step 1: Run scrapers
    print("\n[STEP 1/2] Running scrapers...\n")
    successful, failed = run_scrapers(str(run_folder))
    print_scraping_summary(successful, failed)
    
    if not successful:
        print("\n✗ No scrapers succeeded. Cannot create combined map.")
        print("   Please check the error messages above.")
        return False
    
    # Step 2: Combine maps
    print("\n[STEP 2/2] Combining maps...\n")
    
    if not Path(COMBINE_SCRIPT).exists():
        print(f"✗ Combine script not found: {COMBINE_SCRIPT}")
        return False
    
    if not run_script(COMBINE_SCRIPT, "Combine Maps", str(run_folder)):
        print("\n✗ Failed to create combined map")
        return False
    
    # Final summary
    total_elapsed = time.time() - total_start
    print_header("✓✓✓ PIPELINE COMPLETED SUCCESSFULLY! ✓✓✓")
    print(f"\n Total time: {total_elapsed:.1f} seconds")
    print(f" Sources processed: {len(successful)}")
    print(f"\n{'=' * 70}")
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n✗ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {e}")
        sys.exit(1)

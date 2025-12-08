"""
main.py - Berlin Events Map Generator
Runs all scraping scripts and combines the results into one map
"""

from datetime import datetime
import subprocess
import sys
import time
from pathlib import Path
import os

print("=" * 70)
print("BERLIN EVENTS MAP - AUTOMATED PIPELINE")
print("=" * 70)

# Create timestamped output folder for this run
RUN_FOLDER = os.path.join('output', datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
os.makedirs(RUN_FOLDER, exist_ok=True)

print(f"\n📁 Output folder: {RUN_FOLDER}\n")

# Define scraping scripts and their output files
SCRAPERS = [
    {
        'name': 'Gratis in Berlin',
        'script': 'scrapeGratisInBerlinParallel.py',
        'output_csv': 'gratis_berlin_events.csv',
        'enabled': True
    },
    {
        'name': 'tip Berlin',
        'script': 'scrapeTipBerlinBot.py',
        'output_csv': 'tip_berlin_events.csv',
        'enabled': True
    },
    # Add more scrapers here
    # {
    #     'name': 'Another Source',
    #     'script': 'scrape_another.py',
    #     'output_csv': 'another_events.csv',
    #     'enabled': True
    # }
]

COMBINE_SCRIPT = 'combineMapsLegend.py'

def check_file_exists(filepath):
    """Check if a file exists"""
    return Path(filepath).exists()

def run_script(script_name, description, output_folder=None):
    """Run a Python script and handle errors"""
    print(f"\n{'='*70}")
    print(f"🔄 Running: {description}")
    print(f"   Script: {script_name}")
    print(f"{'='*70}\n")
    
    start_time = time.time()
    
    try:
        # Run the script with output folder argument
        cmd = [sys.executable, script_name]
        if output_folder:
            cmd.append(output_folder)
        
        result = subprocess.run(
            cmd,
            capture_output=False,
            text=True,
            check=True
        )
        
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

def main():
    """Main pipeline execution"""
    
    total_start = time.time()
    successful_scrapers = []
    failed_scrapers = []
    
    print("\n[STEP 1/2] Running scrapers...\n")
    
    # Run each scraper
    for scraper in SCRAPERS:
        if not scraper['enabled']:
            print(f"⏭️  Skipping {scraper['name']} (disabled)")
            continue
        
        if not check_file_exists(scraper['script']):
            print(f"⚠️  Warning: {scraper['script']} not found - skipping")
            failed_scrapers.append(scraper['name'])
            continue
        
        success = run_script(scraper['script'], scraper['name'], RUN_FOLDER)
        
        if success:
            # Verify output file was created
            output_path = os.path.join(RUN_FOLDER, scraper['output_csv'])
            if check_file_exists(output_path):
                print(f"   ✓ Output file created: {output_path}")
                successful_scrapers.append(scraper['name'])
            else:
                print(f"   ⚠️  Warning: Expected output file not found: {output_path}")
                failed_scrapers.append(scraper['name'])
        else:
            failed_scrapers.append(scraper['name'])
    
    # Summary of scraping
    print(f"\n{'='*70}")
    print("SCRAPING SUMMARY")
    print(f"{'='*70}")
    print(f"✓ Successful: {len(successful_scrapers)}")
    for name in successful_scrapers:
        print(f"   - {name}")
    
    if failed_scrapers:
        print(f"\n✗ Failed: {len(failed_scrapers)}")
        for name in failed_scrapers:
            print(f"   - {name}")
    
    # Check if we have any successful scrapers
    if not successful_scrapers:
        print("\n✗ No scrapers succeeded. Cannot create combined map.")
        print("   Please check the error messages above.")
        return False
    
    # Run combine script
    print(f"\n[STEP 2/2] Combining maps...\n")
    
    if not check_file_exists(COMBINE_SCRIPT):
        print(f"✗ Combine script not found: {COMBINE_SCRIPT}")
        return False
    
    success = run_script(COMBINE_SCRIPT, "Combine Maps", RUN_FOLDER)
    
    if not success:
        print("\n✗ Failed to create combined map")
        return False
    
    # Final summary
    total_elapsed = time.time() - total_start
    
    print(f"\n{'='*70}")
    print("✓✓✓ PIPELINE COMPLETED SUCCESSFULLY! ✓✓✓")
    print(f"{'='*70}")
    print(f"\n Total time: {total_elapsed:.1f} seconds")
    print(f" Sources processed: {len(successful_scrapers)}")
    print(f"\n{'='*70}")
    
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

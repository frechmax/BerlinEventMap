import os
import sys
import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

OUTPUT_FOLDER = sys.argv[1] if len(sys.argv) > 1 else '.'
INPUT_FILE = sys.argv[2] if len(sys.argv) > 2 else 'ra_clubs.csv'
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, 'ra_clubs_geocoded.csv')

print('='*60)
print('RA CLUBS GEOCODER')
print('='*60)
print(f"Input: {INPUT_FILE}")
print(f"Output folder: {OUTPUT_FOLDER}")

if not os.path.exists(INPUT_FILE):
    print(f"✗ Input file not found: {INPUT_FILE}")
    sys.exit(1)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Load CSV with flexible parsing (auto, then semicolon fallback)
try:
    df = pd.read_csv(INPUT_FILE)
except Exception as e1:
    try:
        df = pd.read_csv(INPUT_FILE, sep=';', engine='python')
    except Exception as e2:
        print(f"✗ Failed to read CSV: {e1}\nFallback failed: {e2}")
        sys.exit(1)

# Expect columns: Clubname, Ort, Adresse (if available). We'll try 'Ort' first.
address_col = None
for candidate in ['Adresse', 'address', 'Ort', 'Location']:
    if candidate in df.columns:
        address_col = candidate
        break

if address_col is None:
    print('✗ No address/location column found. Expected one of: Adresse, address, Ort, Location')
    sys.exit(1)

geolocator = Nominatim(user_agent='berlin-ra-clubs-map')
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.5)

lats = []
lons = []

total = len(df)
for idx, row in df.iterrows():
    addr = str(row[address_col]) if pd.notna(row[address_col]) else ''
    name = str(row['Clubname']) if 'Clubname' in df.columns and pd.notna(row['Clubname']) else ''

    query = addr if any(ch.isdigit() for ch in addr) else f"{name}, {addr}, Berlin, Germany".strip(', ')

    print(f"[{idx+1}/{total}] {query[:60]}...", end=' ')
    try:
        location = geocode(query)
        if location:
            lats.append(location.latitude)
            lons.append(location.longitude)
            print('✓')
        else:
            lats.append(None)
            lons.append(None)
            print('✗')
    except Exception:
        lats.append(None)
        lons.append(None)
        print('✗')

# Append results
df['lat'] = lats
df['lon'] = lons

mapped = df.dropna(subset=['lat', 'lon'])
print('\n' + '='*60)
print(f"✓ {len(mapped)}/{len(df)} clubs geocoded")
print('='*60)

# Save
try:
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✓ Saved: {OUTPUT_FILE}")
except Exception as e:
    print(f"✗ Failed to save CSV: {e}")
    sys.exit(1)

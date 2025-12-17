import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import sys
from geopy.geocoders import Nominatim
from time import sleep

# Get output folder from command line argument
OUTPUT_FOLDER = sys.argv[1] if len(sys.argv) > 1 else '.'

def scrape_visitberlin_events():
    # Header simulieren, um nicht geblockt zu werden
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # URL mit heutigem Datum generieren
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    url = f"https://www.visitberlin.de/de/tagestipps-veranstaltungen-berlin?keys=&date_between[min]={today}&date_between[max]={today}&district=All&items_per_page=max"

    print(f"Rufe URL ab: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        html_content = response.text
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen der Seite: {e}")
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Alle Event-Container finden
    articles = soup.find_all('article', class_='teaser-search--event')
    print(f"{len(articles)} Events gefunden.")

    events_data = []

    for article in articles:
        item = {}

        # 1. Titel
        title_tag = article.find('h2', class_='teaser-search__heading')
        item['title'] = title_tag.get_text(strip=True) if title_tag else "N/A"

        # 2. Datum
        time_tag = article.find('time')
        item['date'] = time_tag.get_text(strip=True) if time_tag else "N/A"

        # 3. Uhrzeit
        time_range_tag = article.find('p', class_='teaser-search__time')
        if time_range_tag:
            content = time_range_tag.find('span', class_='me__content')
            item['time'] = content.get_text(strip=True) if content else "N/A"
        else:
            item['time'] = "N/A"

        # 4. Ort/Adresse
        loc_tag = article.find('p', class_='teaser-search__location')
        if loc_tag:
            nopr = loc_tag.find('span', class_='nopr')
            if nopr:
                item['address'] = nopr.get_text(strip=True)
            else:
                content = loc_tag.find('span', class_='me__content')
                item['address'] = content.get_text(strip=True) if content else "N/A"
        else:
            item['address'] = "N/A"

        # 5. Beschreibung
        desc_tag = article.find('div', class_='teaser-search__text')
        if desc_tag and desc_tag.div:
            item['description'] = desc_tag.div.get_text(strip=True)[:500]  # Begrenzen auf 500 Zeichen
        else:
            item['description'] = ""

        # 6. Link
        link_tag = article.find('a', class_='teaser-search__mainlink')
        if link_tag and link_tag.get('href'):
            link = link_tag['href']
            if link.startswith('/'):
                link = "https://www.visitberlin.de" + link
            item['url'] = link
        else:
            item['url'] = "N/A"

        events_data.append(item)

    return events_data

print("=" * 60)
print("VISITBERLIN.DE EVENT SCRAPER")
print("=" * 60)

# Scrape Events
events = scrape_visitberlin_events()

print(f"\n{len(events)} Events gefunden")

if len(events) == 0:
    print("\n⚠ Keine Events gefunden!")
    exit()

# DataFrame erstellen
df = pd.DataFrame(events)

# Geocoding
print(f"\n[2/3] Geocoding der Adressen...")
print("(Hier können wir NICHT parallel arbeiten wegen Rate Limits)\n")

geolocator = Nominatim(user_agent="berlin-visit-events-map")

def geocode_address(address):
    try:
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        return None, None
    except:
        return None, None

for idx, row in df.iterrows():
    print(f"  [{idx+1}/{len(df)}] {row['address']}", end=" ")
    lat, lon = geocode_address(row['address'])
    df.at[idx, 'lat'] = lat
    df.at[idx, 'lon'] = lon
    
    if lat:
        print(f"✓")
    else:
        print(f"✗")
    
    sleep(1.5)  # Nominatim Rate Limit

df_mapped = df.dropna(subset=['lat', 'lon'])

print(f"\n{'='*60}")
print(f"✓ {len(df_mapped)}/{len(df)} Events mit Koordinaten")
print(f"{'='*60}")

# CSV speichern
output_file = os.path.join(OUTPUT_FOLDER, 'visitberlin_events.csv')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)  # Stelle sicher, dass der Ordner existiert
df_mapped.to_csv(output_file, index=False)
print(f"\n✓ CSV gespeichert: {output_file}")

print(f"\n{'='*60}")
print(f"✓✓✓ FERTIG! ✓✓✓")
print(f"{'='*60}")

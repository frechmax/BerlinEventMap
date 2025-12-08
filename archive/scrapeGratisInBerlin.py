import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
import folium
import os

print("=" * 60)
print("GRATIS-IN-BERLIN.DE EVENT SCRAPER")
print("=" * 60)

# Scrape der Hauptseite
url = "https://www.gratis-in-berlin.de/heute"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

print(f"\n[1/5] Lade Hauptseite: {url}")
response = requests.get(url, headers=headers)
print(f"✓ Status Code: {response.status_code}")

soup = BeautifulSoup(response.content, 'html.parser')

# Events extrahieren
events = []
event_items = soup.find_all('h2', class_='overviewcontentheading')
print(f"✓ {len(event_items)} Event-Überschriften gefunden")

print(f"\n[2/5] Scrape Event-Details...")
for idx, item in enumerate(event_items, 1):
    # Event-Titel und Link
    link_tag = item.find('a', class_='singletip')
    if link_tag:
        title = link_tag.get_text(strip=True)
        event_url = 'https://www.gratis-in-berlin.de' + link_tag['href']
        
        print(f"\n  [{idx}/{len(event_items)}] {title[:50]}...")
        
        # Detail-Seite aufrufen für Adresse
        try:
            print(f"      → Lade Detail-Seite...")
            detail_response = requests.get(event_url, headers=headers)
            detail_soup = BeautifulSoup(detail_response.content, 'html.parser')
            
            # Adresse extrahieren
            map_div = detail_soup.find('div', class_='mapTipp')
            if map_div:
                # Adresse aus dem Text extrahieren (vor dem ersten Link)
                address_text = map_div.get_text(strip=True).split('-')[0].strip()
                
                events.append({
                    'title': title,
                    'url': event_url,
                    'address': address_text
                })
                print(f"      ✓ Adresse: {address_text[:40]}...")
            else:
                print(f"      ✗ Keine Adresse gefunden")
            
            sleep(1)  # Rate limiting für höfliches Scraping
            
        except Exception as e:
            print(f"      ✗ Fehler: {e}")
            continue

# DataFrame erstellen
df = pd.DataFrame(events)
print(f"\n{'='*60}")
print(f"✓ {len(df)} Events erfolgreich gescraped")
print(f"{'='*60}")

# Geocoding der Adressen
geolocator = Nominatim(user_agent="berlin-gratis-events-map")

def geocode_address(address):
    try:
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        return None, None
    except:
        return None, None

print(f"\n[3/5] Geocoding der Adressen...")
print("(Dies kann einige Minuten dauern...)\n")

for idx, row in df.iterrows():
    print(f"  [{idx+1}/{len(df)}] Geocode: {row['address'][:40]}...")
    lat, lon = geocode_address(row['address'])
    df.at[idx, 'lat'] = lat
    df.at[idx, 'lon'] = lon
    
    if lat and lon:
        print(f"         ✓ Koordinaten: {lat:.4f}, {lon:.4f}")
    else:
        print(f"         ✗ Keine Koordinaten gefunden")
    
    sleep(1.5)  # Nominatim Rate Limit

# Entferne Events ohne Koordinaten
df_mapped = df.dropna(subset=['lat', 'lon'])
print(f"\n{'='*60}")
print(f"✓ {len(df_mapped)}/{len(df)} Events mit gültigen Koordinaten")
print(f"{'='*60}")

# CSV speichern
print(f"\n[4/5] Speichere Daten...")
df_mapped.to_csv('berlin_events.csv', index=False)
print(f"✓ CSV gespeichert: berlin_events.csv ({len(df_mapped)} Events)")

# Interaktive Karte erstellen
print(f"\n[5/5] Erstelle interaktive Karte...")
berlin_map = folium.Map(
    location=[52.5200, 13.4050],  # Berlin Zentrum
    zoom_start=12,
    tiles='OpenStreetMap'
)

from folium.plugins import MarkerCluster
marker_cluster = MarkerCluster().add_to(berlin_map)

# Marker für jedes Event hinzufügen
for idx, row in df_mapped.iterrows():
    popup_html = f"""
    <div style="width: 250px;">
        <h4>{row['title']}</h4>
        <p><b>Adresse:</b> {row['address']}</p>
        <a href="{row['url']}" target="_blank">Mehr Infos</a>
    </div>
    """
    
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=folium.Popup(popup_html, max_width=300),
        tooltip=row['title'],
        icon=folium.Icon(color='green', icon='info-sign')
    ).add_to(marker_cluster)

print(f"✓ {len(df_mapped)} Marker zur Karte hinzugefügt")

# Ordner erstellen und Karte speichern
os.makedirs('berlin-events', exist_ok=True)
berlin_map.save('berlin-events/index.html')

print(f"\n{'='*60}")
print(f"✓✓✓ FERTIG! ✓✓✓")
print(f"{'='*60}")
print(f"\n📂 Karte: berlin-events/index.html")
print(f"📊 CSV: berlin_events.csv")
print(f"\n💡 Öffne 'berlin-events/index.html' im Browser")
print(f"   oder lade den Ordner 'berlin-events' auf Netlify hoch!")
print(f"\n{'='*60}")

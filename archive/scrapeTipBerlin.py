import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
import os

# Hauptseite scrapen
url = "https://www.tip-berlin.de/events/"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

print("Scraping tip-berlin.de...")
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, 'html.parser')

events = []

# Finde alle Event-Boxen
event_boxes = soup.find_all('div', class_='collections__box--event')
print(f"Gefundene Event-Boxen: {len(event_boxes)}")

for box in event_boxes:
    try:
        # Titel extrahieren
        title_tag = box.find('h2', class_='collections__box__title')
        title = title_tag.get_text(strip=True) if title_tag else None
        print(f"Verarbeites Event: {title}")
        # URL extrahieren
        link_tag = box.find('a', class_='collections__box__link')
        event_url = link_tag['href'] if link_tag else None
        
        # Kategorie extrahieren
        category_tag = box.find('p', class_='collections__box__event-category')
        category = category_tag.get_text(strip=True) if category_tag else None
        
        # Adresse extrahieren (Desktop-Version)
        address_span = box.find('span', class_='-desktop-v')
        address = address_span.get_text(strip=True) if address_span else None
        
        # Venue extrahieren (Mobile-Version als Backup)
        venue_span = box.find('span', class_='-mobile-v')
        venue = venue_span.get_text(strip=True) if venue_span else None
        
        # Datum extrahieren
        date_tags = box.find_all('h3', class_='collections__box__title')
        date = date_tags[-1].get_text(strip=True) if date_tags else None
        
        if title and address:
            events.append({
                'title': title,
                'category': category,
                'venue': venue,
                'address': address,
                'date': date,
                'url': event_url
            })
            print(f"✓ {title}")
        
    except Exception as e:
        print(f"✗ Fehler beim Parsen: {e}")
        continue

print(f"\n{len(events)} Events gefunden")

# DataFrame erstellen
df = pd.DataFrame(events)

# Geocoding
print("\nGeocoding der Adressen...")
geolocator = Nominatim(user_agent="tip-berlin-events-map")

def geocode_address(address):
    try:
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        return None, None
    except:
        return None, None

# Koordinaten hinzufügen (mit Rate Limiting)
for idx, row in df.iterrows():
    lat, lon = geocode_address(row['address'])
    df.at[idx, 'lat'] = lat
    df.at[idx, 'lon'] = lon
    sleep(1.5)  # Nominatim Rate Limit respektieren

df_mapped = df.dropna(subset=['lat', 'lon'])
print(f"{len(df_mapped)} Events mit Koordinaten")

# CSV speichern
df_mapped.to_csv('tip_berlin_events.csv', index=False, encoding='utf-8')
print("✓ Daten gespeichert als 'tip_berlin_events.csv'")

# Karte erstellen
import folium
from folium.plugins import MarkerCluster

berlin_map = folium.Map(
    location=[52.5200, 13.4050],
    zoom_start=12,
    tiles='OpenStreetMap'
)

marker_cluster = MarkerCluster().add_to(berlin_map)

# Marker hinzufügen
for idx, row in df_mapped.iterrows():
    popup_html = f"""
    <div style="width: 280px;">
        <h4>{row['title']}</h4>
        <p><b>Kategorie:</b> {row['category']}</p>
        <p><b>Venue:</b> {row['venue']}</p>
        <p><b>Adresse:</b> {row['address']}</p>
        <p><b>Datum:</b> {row['date']}</p>
        <a href="{row['url']}" target="_blank">Mehr Infos</a>
    </div>
    """
    
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=folium.Popup(popup_html, max_width=320),
        tooltip=row['title'],
        icon=folium.Icon(color='blue', icon='info-sign')
    ).add_to(marker_cluster)

# Karte speichern
os.makedirs('tip-berlin-map', exist_ok=True)
berlin_map.save('tip-berlin-map/index.html')
print("✓ Karte gespeichert als 'tip-berlin-map/index.html'")

from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
import folium
from folium.plugins import MarkerCluster
import os

# Lade den HTML-Text aus deiner Datei
print("Lade HTML aus Datei...")
with open('input_tip_berlin.txt', 'r', encoding='utf-8') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')

events = []

# Event-Boxen finden
event_boxes = soup.find_all('div', class_='collections__box--event')
print(f"Gefundene Event-Boxen: {len(event_boxes)}")

for box in event_boxes:
    try:
        # Titel
        title_tag = box.find('h2', class_='collections__box__title')
        title = title_tag.get_text(strip=True) if title_tag else None
        
        # URL
        link_tag = box.find('a', class_='collections__box__link')
        event_url = link_tag['href'] if link_tag else None
        
        # Kategorie
        category_tag = box.find('p', class_='collections__box__event-category')
        category = category_tag.get_text(strip=True) if category_tag else None
        
        # Adresse (Desktop-Version)
        address_span = box.find('span', class_='-desktop-v')
        address = address_span.get_text(strip=True) if address_span else None
        
        # Venue (Mobile-Version)
        venue_span = box.find('span', class_='-mobile-v')
        venue = venue_span.get_text(strip=True) if venue_span else None
        
        # Datum
        date_tags = box.find_all('h3', class_='collections__box__title')
        date = date_tags[-1].get_text(strip=True) if date_tags else None
        
        if title and address:
            events.append({
                'title': title,
                'category': category,
                'venue': venue,
                'address': address,
                'date': date,
                'url': event_url if event_url else ''
            })
            print(f"✓ {title}")
    
    except Exception as e:
        print(f"✗ Fehler: {e}")
        continue

print(f"\n{len(events)} Events gefunden")

if len(events) == 0:
    print("Keine Events gefunden! Prüfe die HTML-Struktur in paste.txt")
    exit()

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

for idx, row in df.iterrows():
    lat, lon = geocode_address(row['address'])
    df.at[idx, 'lat'] = lat
    df.at[idx, 'lon'] = lon
    sleep(1.5)
    print(f"  {idx+1}/{len(df)}: {row['venue']}")

df_mapped = df.dropna(subset=['lat', 'lon'])
print(f"\n{len(df_mapped)} Events mit Koordinaten")

# CSV speichern
df_mapped.to_csv('tip_berlin_events.csv', index=False, encoding='utf-8')
print("✓ CSV gespeichert: tip_berlin_events.csv")

# Karte erstellen
print("\nErstelle Karte...")
berlin_map = folium.Map(
    location=[52.5200, 13.4050],
    zoom_start=12,
    tiles='OpenStreetMap'
)

marker_cluster = MarkerCluster().add_to(berlin_map)

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
print("✓ Karte erstellt: tip-berlin-map/index.html")
print("\nFertig! Öffne 'tip-berlin-map/index.html' im Browser oder lade den Ordner auf Netlify hoch.")

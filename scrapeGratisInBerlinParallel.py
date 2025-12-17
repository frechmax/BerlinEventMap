from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

def scrape_event_details(event_data):
    """Scraped Details für ein einzelnes Event"""
    try:
        response = requests.get(event_data['url'], headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Adresse extrahieren
        map_div = soup.find('div', class_='mapTipp')
        address_text = None
        if map_div:
            address_text = map_div.get_text(strip=True).split('-')[0].strip()
        
        # Beschreibung extrahieren
        description = None
        desc_div = soup.find('div', class_='overview-text')
        if desc_div:
            description = desc_div.get_text(separator=' ', strip=True)
            # Begrenzen auf 500 Zeichen
            description = description[:500] if description else None
        
        # Detaillierte Zeitangabe extrahieren
        detailed_date = None
        date_div = soup.find('div', class_='dateTipp')
        if date_div:
            detailed_date = date_div.get_text(separator=' ', strip=True)
        
        if address_text:
            return {
                'title': event_data['title'],
                'url': event_data['url'],
                'address': address_text,
                'description': description,
                'detailed_date': detailed_date,
                'success': True
            }
        return {**event_data, 'success': False}
    except Exception as e:
        return {**event_data, 'success': False, 'error': str(e)}

def scrape_gratis_berlin_events():
    """Scrape events from gratis-in-berlin.de using parallel processing."""
    url = "https://www.gratis-in-berlin.de/heute"
    
    print("\n[1/4] Lade Hauptseite...")
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    event_items = soup.find_all('h2', class_='overviewcontentheading')
    print(f"✓ {len(event_items)} Events gefunden\n")
    
    # URLs sammeln
    event_urls = []
    for item in event_items:
        link_tag = item.find('a', class_='singletip')
        if link_tag:
            title = link_tag.get_text(strip=True)
            event_url = 'https://www.gratis-in-berlin.de' + link_tag['href']
            event_urls.append({
                'title': title,
                'url': event_url
            })
    
    # PARALLEL PROCESSING - deutlich schneller!
    print("[2/4] Scrape Event-Details parallel...")
    print("(Dies ist viel schneller als sequentiell!)\n")
    
    events = []
    completed = 0
    
    # ThreadPoolExecutor für paralleles Scraping
    # max_workers=10 bedeutet 10 gleichzeitige Requests
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Starte alle Scraping-Tasks
        future_to_event = {
            executor.submit(scrape_event_details, event): event 
            for event in event_urls
        }
        
        # Verarbeite abgeschlossene Tasks
        for future in as_completed(future_to_event):
            result = future.result()
            completed += 1
            
            if result.get('success'):
                events.append({
                    'title': result['title'],
                    'url': result['url'],
                    'address': result['address'],
                    'description': result.get('description'),
                    'detailed_date': result.get('detailed_date')
                })
                print(f"  [{completed}/{len(event_urls)}] ✓ {result['title']}")
            else:
                print(f"  [{completed}/{len(event_urls)}] ✗ {result['title']}")
    
    print(f"\n{'='*60}")
    print(f"✓ {len(events)}/{len(event_urls)} Events erfolgreich gescraped")
    print(f"{'='*60}")
    
    return events

def geocode_address(address):
    """Geocode an address to coordinates."""
    try:
        geolocator = Nominatim(user_agent="berlin-gratis-events-map")
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        return None, None
    except:
        return None, None

def run_gratis_berlin_scraper(output_folder='.'):
    """Main function to scrape gratis-in-berlin.de and save results."""
    print("=" * 60)
    print("SCHNELLER GRATIS-IN-BERLIN.DE SCRAPER")
    print("=" * 60)
    
    events = scrape_gratis_berlin_events()
    
    # DataFrame erstellen
    df = pd.DataFrame(events)
    
    if len(df) == 0:
        print("\n✗ Keine Events gefunden!")
        return pd.DataFrame()
    
    # Geocoding
    print(f"\n[3/4] Geocoding der Adressen...")
    print("(Hier können wir NICHT parallel arbeiten wegen Rate Limits)\n")
    
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
    output_file = os.path.join(output_folder, 'gratis_berlin_events.csv')
    df_mapped.to_csv(output_file, index=False)
    print(f"\n✓ CSV gespeichert: {output_file}")
    
    return df_mapped

if __name__ == "__main__":
    # Get output folder from command line argument
    OUTPUT_FOLDER = sys.argv[1] if len(sys.argv) > 1 else '.'
    run_gratis_berlin_scraper(OUTPUT_FOLDER)

# # Karte erstellen
# print(f"\n[4/4] Erstelle Karte...")

# import folium
# from folium.plugins import MarkerCluster

# berlin_map = folium.Map(
#     location=[52.5200, 13.4050],
#     zoom_start=12,
#     tiles='OpenStreetMap'
# )

# marker_cluster = MarkerCluster(
#     max_cluster_radius=35,
#     disable_clustering_at_zoom=13,
#     spiderfyOnMaxZoom=True
# ).add_to(berlin_map)

# for idx, row in df_mapped.iterrows():
#     popup_html = f"""
#     <div style="width: 250px;">
#         <h4>{row['title']}</h4>
#         <p><b>Adresse:</b> {row['address']}</p>
#         <a href="{row['url']}" target="_blank">Mehr Infos</a>
#     </div>
#     """
    
#     folium.Marker(
#         location=[row['lat'], row['lon']],
#         popup=folium.Popup(popup_html, max_width=300),
#         tooltip=row['title'],
#         icon=folium.Icon(color='green', icon='info-sign')
#     ).add_to(marker_cluster)

# os.makedirs('berlin-events', exist_ok=True)
# berlin_map.save('berlin-events/index.html')

print(f"\n{'='*60}")
print(f"✓✓✓ FERTIG! ✓✓✓")
print(f"{'='*60}")
# print(f"\n📂 Karte: berlin-events/index.html")
print(f"📊 CSV: berlin_events.csv")
print(f"\n⚡ Durch Parallel Processing wurde das Scraping")
print(f"   ~10x schneller als die sequentielle Version!")
print(f"\n{'='*60}")

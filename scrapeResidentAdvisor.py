"""
scrapeResidentAdvisor.py
Scraped NUR Events von HEUTE von Resident Advisor
"""

import requests
import json
import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
import os
from datetime import datetime

print("=" * 60)
print("RESIDENT ADVISOR EVENT SCRAPER (NUR HEUTE)")
print("=" * 60)

URL = "https://de.ra.co/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Referer": "https://de.ra.co/events/de/berlin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}

EVENTS_QUERY = """
query GET_EVENT_LISTINGS($filters: FilterInputDtoInput, $pageSize: Int, $page: Int) {
    eventListings(filters: $filters, pageSize: $pageSize, page: $page) {
        data {
            id
            event {
                id
                title
                date
                startTime
                endTime
                contentUrl
                attending
                venue {
                    id
                    name
                    contentUrl
                    address
                    area {
                        name
                    }
                }
            }
        }
        totalResults
    }
}
"""

# NUR HEUTE
today = datetime.now().strftime("%Y-%m-%d")

print(f"\n[1/4] Lade Events von HEUTE: {today}\n")

all_events = []
page = 1
max_pages = 10

while page <= max_pages:
    variables = {
        "filters": {
            "areas": {
                "eq": 34  # Berlin
            },
            "dateRange": {
                "gte": today,
                "lte": today  # GLEICHER TAG = nur heute
            }
        },
        "pageSize": 50,
        "page": page
    }
    
    payload = {
        "query": EVENTS_QUERY,
        "variables": variables
    }
    
    print(f"  Seite {page}...", end=" ")
    
    try:
        response = requests.post(URL, headers=HEADERS, json=payload, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        if "errors" in data:
            print(f"\n    ✗ GraphQL Error: {data['errors']}")
            break
        
        if "data" not in data or "eventListings" not in data["data"]:
            print("✗ Keine Daten")
            break
        
        listings = data["data"]["eventListings"]["data"]
        
        if not listings:
            print("✗ Keine Events mehr")
            break
        
        print(f"✓ {len(listings)} Events")
        
        for listing in listings:
            try:
                event = listing.get("event", {})
                venue = event.get("venue", {})
                area = venue.get("area", {})
                
                event_date_str = event.get('date', '')
                event_date = event_date_str.split('T')[0] if event_date_str else None
                
                # Prüfe nochmal, dass es wirklich heute ist
                if event_date != today:
                    continue
                
                all_events.append({
                    'event_id': event.get('id'),
                    'title': event.get('title'),
                    'date': event_date,
                    'start_time': event.get('startTime', '').split('T')[-1][:5] if event.get('startTime') else None,
                    'venue': venue.get('name'),
                    'venue_address': venue.get('address'),
                    'area': area.get('name'),
                    'attending': event.get('attending', 0),
                    'url': f"https://de.ra.co{event.get('contentUrl', '')}" if event.get('contentUrl') else None,
                })
            except Exception as e:
                continue
        
        page += 1
        sleep(1.5)
        
    except Exception as e:
        print(f"\n    ✗ Fehler: {e}")
        break

print(f"\n{'='*60}")
print(f"✓ {len(all_events)} Events von HEUTE ({today})")
print(f"{'='*60}")

if len(all_events) == 0:
    print("\n⚠ Keine Events heute gefunden!")
    print("Tipp: Versuche 'morgen' oder ein anderes Datum")
    exit()

df = pd.DataFrame(all_events)
df = df.drop_duplicates(subset=['event_id'])

# Sortiere nach Startzeit
df = df.sort_values('start_time')

print(f"\nEvents heute:")
for idx, row in df.head(10).iterrows():
    time = row.get('start_time', 'TBA')
    print(f"  {time} - {row['title'][:50]} @ {row['venue']}")

# Geocoding
print(f"\n[2/4] Geocoding der Venues...")
geolocator = Nominatim(user_agent="ra-berlin-events-map")

BERLIN_VENUES = {
    'Berghain': (52.5108, 13.4429),
    'Panorama Bar': (52.5108, 13.4429),
    'Watergate': (52.5053, 13.4415),
    'Tresor': (52.5126, 13.4154),
    'About Blank': (52.5245, 13.4693),
    '://about blank': (52.5245, 13.4693),
    'Sisyphos': (52.5198, 13.4872),
    'RSO': (52.5074, 13.4536),
    'RSO.Berlin': (52.5074, 13.4536),
    'Renate': (52.5001, 13.4652),
    'Salon zur Wilden Renate': (52.5001, 13.4652),
    'Wilde Renate': (52.5001, 13.4652),
    'Kater Blau': (52.5123, 13.4250),
    'KitKatClub': (52.5025, 13.4100),
    'Griessmühle': (52.4756, 13.4394),
    'Club der Visionaere': (52.4967, 13.4427),
    'Else': (52.5251, 13.4124),
    'Golden Gate': (52.4992, 13.4393),
    'Ritter Butzke': (52.5030, 13.4190),
    'Birgit & Bier': (52.5145, 13.4210),
    'Fitzroy': (52.5287, 13.4149),
    'OST': (52.4969, 13.4643),
}

def geocode_venue(venue_name, venue_address=None):
    if not venue_name:
        return None, None
    
    for known_venue, coords in BERLIN_VENUES.items():
        if known_venue.lower() in venue_name.lower():
            return coords
    
    if venue_address:
        try:
            location = geolocator.geocode(venue_address + ", Berlin, Germany")
            if location:
                return location.latitude, location.longitude
        except:
            pass
    
    try:
        search_query = f"{venue_name}, Berlin, Germany"
        location = geolocator.geocode(search_query)
        if location:
            return location.latitude, location.longitude
    except:
        pass
    
    return None, None

for idx, row in df.iterrows():
    venue_name = row.get('venue')
    venue_address = row.get('venue_address')
    
    if venue_name:
        print(f"  [{idx+1}/{len(df)}] {venue_name[:35]}...", end=" ")
        lat, lon = geocode_venue(venue_name, venue_address)
        df.at[idx, 'lat'] = lat
        df.at[idx, 'lon'] = lon
        
        if lat:
            print(f"✓")
        else:
            print(f"✗")
        
        sleep(1.5)

df_mapped = df.dropna(subset=['lat', 'lon'])

print(f"\n{'='*60}")
print(f"✓ {len(df_mapped)}/{len(df)} Events mit Koordinaten")
print(f"{'='*60}")

df_mapped.to_csv('ra_berlin_events.csv', index=False, encoding='utf-8')
print(f"\n[3/4] ✓ CSV: ra_berlin_events.csv")

# Karte
print(f"\n[4/4] Erstelle Karte...")

import folium
from folium.plugins import MarkerCluster

berlin_map = folium.Map(location=[52.5200, 13.4050], zoom_start=12, prefer_canvas=True)

marker_cluster = MarkerCluster(
    max_cluster_radius=40,
    disable_clustering_at_zoom=14
).add_to(berlin_map)

for idx, row in df_mapped.iterrows():
    popup_html = f"""
    <div style="width: 280px;">
        <h4>{row['title']}</h4>
        <p><b>Venue:</b> {row['venue']}</p>
        <p><b>Heute:</b> {row.get('start_time', 'TBA')} Uhr</p>
        <p><b>Attending:</b> {row.get('attending', 0)} Personen</p>
        <a href="{row['url']}" target="_blank">→ Resident Advisor</a>
    </div>
    """
    
    folium.CircleMarker(
        location=[row['lat'], row['lon']],
        radius=7,
        popup=folium.Popup(popup_html, max_width=300),
        tooltip=f"{row.get('start_time', 'TBA')} - {row['title'][:30]}",
        color='white',
        fillColor='#FF5E00',
        fillOpacity=0.8,
        weight=2
    ).add_to(marker_cluster)

os.makedirs('ra-berlin-map', exist_ok=True)
berlin_map.save('ra-berlin-map/index.html')

print(f"\n{'='*60}")
print(f"✓✓✓ FERTIG! ✓✓✓")
print(f"{'='*60}")
print(f"\n📂 ra-berlin-map/index.html")
print(f"📊 ra_berlin_events.csv")
print(f"🎵 {len(df_mapped)} Events HEUTE ({today})")
print(f"{'='*60}")

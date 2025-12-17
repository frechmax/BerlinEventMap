from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
import os
import sys

# Get output folder from command line argument
OUTPUT_FOLDER = sys.argv[1] if len(sys.argv) > 1 else '.'

def scrape_tip_berlin():
    events = []
    
    with sync_playwright() as p:
        print("Starte Browser...")
        browser = p.chromium.launch(headless=True)
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        page = context.new_page()
        
        # 1. Tageshighlights-Seite öffnen
        print("Öffne Tageshighlights-Seite...")
        page.goto("https://www.tip-berlin.de/event-tageshighlights/", wait_until="networkidle")
        page.wait_for_timeout(3000)
        
        # 2. Cookie-Banner schließen
        print("Schließe Cookie-Banner...")
        try:
            # Versuche "Akzeptieren" Button zu finden und zu klicken
            # Verschiedene mögliche Selektoren probieren
            cookie_buttons = [
                'button:has-text("Akzeptieren")',
                'button:has-text("Alle akzeptieren")',
                'button:has-text("Accept")',
                '[title="Akzeptieren"]',
                '.sp_choice_type_11',  # Sourcepoint Cookie-Banner
                'button[title="Zustimmen"]'
            ]
            
            for selector in cookie_buttons:
                try:
                    cookie_btn = page.locator(selector).first
                    if cookie_btn.is_visible(timeout=2000):
                        cookie_btn.click(timeout=5000)
                        print(f"  ✓ Cookie-Banner geschlossen mit: {selector}")
                        page.wait_for_timeout(1000)
                        break
                except:
                    continue
                    
        except Exception as e:
            print(f"  ⚠ Cookie-Banner nicht gefunden oder bereits geschlossen: {e}")
        
        # 3. Werbe-Popup schließen (falls vorhanden)
        print("Prüfe auf Werbe-Popups...")
        try:
            close_buttons = [
                'button:has-text("Schließen")',
                'button:has-text("×")',
                '[aria-label="Close"]',
                '.close',
                '.modal-close'
            ]
            
            for selector in close_buttons:
                try:
                    close_btn = page.locator(selector).first
                    if close_btn.is_visible(timeout=1000):
                        close_btn.click(timeout=3000)
                        print(f"  ✓ Popup geschlossen")
                        page.wait_for_timeout(1000)
                        break
                except:
                    continue
                    
        except Exception as e:
            print(f"  ⚠ Kein Werbe-Popup gefunden")
        
        page.wait_for_timeout(2000)
        
        # 4. Jetzt auf "Mehr" Button klicken
        print("Klicke auf ersten 'Mehr' Button...")
        try:
            mehr_button = page.locator('a.tip-recommended-posts__more-link').first
            mehr_button.click(timeout=10000)
            
            # Warte auf Navigation
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(3000)
            
            print(f"✓ Navigiert zu: {page.url}")
            
        except Exception as e:
            print(f"✗ Fehler beim Klicken: {e}")
            print("Versuche direkte Navigation...")
            page.goto("https://www.tip-berlin.de/event/", wait_until="networkidle")
            page.wait_for_timeout(3000)
        
        # 5. HTML nach dem Klick holen
        html = page.content()
        
        with open('tip_berlin_scraped.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print("✓ HTML gespeichert")
        
        browser.close()
    
    # HTML parsen
    soup = BeautifulSoup(html, 'html.parser')
    event_boxes = soup.find_all('div', class_='collections__box--event')
    print(f"\nGefundene Event-Boxen: {len(event_boxes)}")
    
    if len(event_boxes) == 0:
        event_boxes = soup.find_all('div', class_='collections__box')
        print(f"Alternative Event-Boxen: {len(event_boxes)}")
    
    for box in event_boxes:
        try:
            title_tag = box.find('h2', class_='collections__box__title')
            title = title_tag.get_text(strip=True) if title_tag else None
            
            link_tag = box.find('a', class_='collections__box__link')
            event_url = link_tag['href'] if link_tag else None
            
            category_tag = box.find('p', class_='collections__box__event-category')
            category = category_tag.get_text(strip=True) if category_tag else None
            
            address_span = box.find('span', class_='-desktop-v')
            address = address_span.get_text(strip=True) if address_span else None
            
            venue_span = box.find('span', class_='-mobile-v')
            venue = venue_span.get_text(strip=True) if venue_span else None
            
            date_tags = box.find_all('h3', class_='collections__box__title')
            date = date_tags[-1].get_text(strip=True) if date_tags else None
            
            if title and (address or venue):
                events.append({
                    'title': title,
                    'category': category,
                    'venue': venue,
                    'address': address,
                    'date': date,
                    'url': event_url
                })
                print(f"✓ {title}")
        except:
            continue
    
    return events

print("=" * 60)
print("TIP-BERLIN.DE EVENT SCRAPER")
print("=" * 60)

events = scrape_tip_berlin()
print(f"\n{len(events)} Events gefunden")

if len(events) == 0:
    print("\n⚠ Keine Events gefunden!")
    exit()

df = pd.DataFrame(events)

# Geocoding
print("\n" + "=" * 60)
print("GEOCODING")
print("=" * 60)

geolocator = Nominatim(user_agent="tip-berlin-events-map")

def geocode_address_or_venue(text, city="Berlin"):
    try:
        if any(char.isdigit() for char in text):
            search_query = text
        else:
            search_query = f"{text}, {city}, Germany"
        
        location = geolocator.geocode(search_query)
        if location:
            return location.latitude, location.longitude
        return None, None
    except:
        return None, None

for idx, row in df.iterrows():
    location_text = row.get('address') or row.get('venue')
    
    if location_text:
        print(f"[{idx+1}/{len(df)}] {location_text[:40]}...", end=" ")
        lat, lon = geocode_address_or_venue(location_text)
        df.at[idx, 'lat'] = lat
        df.at[idx, 'lon'] = lon
        
        if lat:
            print(f"✓")
        else:
            print(f"✗")
        
        sleep(1.5)

df_mapped = df.dropna(subset=['lat', 'lon'])
print(f"\n✓ {len(df_mapped)}/{len(df)} Events mit Koordinaten")
from datetime import datetime
output_file = os.path.join(OUTPUT_FOLDER, 'tip_berlin_events.csv')
df_mapped.to_csv(output_file, index=False, encoding='utf-8')
print(f"✓ CSV gespeichert: {output_file}")

# # Karte
# import folium
# from folium.plugins import MarkerCluster

# berlin_map = folium.Map(location=[52.5200, 13.4050], zoom_start=12)
# # MarkerCluster mit angepassten Einstellungen
# marker_cluster = MarkerCluster(
#     max_cluster_radius=40,  # Standard: 80 - kleinerer Wert = weniger Clustering
#     disable_clustering_at_zoom=13,  # Ab Zoom-Level 13 keine Cluster mehr
#     spiderfyOnMaxZoom=True,  # Marker verteilen sich beim maximalen Zoom
#     showCoverageOnHover=False,  # Keine Cluster-Grenze beim Hover
#     zoomToBoundsOnClick=True  # Zoom beim Klick auf Cluster
# ).add_to(berlin_map)

# for idx, row in df_mapped.iterrows():
#     popup_html = f"""
#     <div style="width: 280px;">
#         <h4>{row['title']}</h4>
#         <p><b>Kategorie:</b> {row['category']}</p>
#         <p><b>Venue:</b> {row['venue']}</p>
#         <p><b>Adresse:</b> {row['address']}</p>
#         <p><b>Datum:</b> {row['date']}</p>
#         <a href="{row['url']}" target="_blank">Mehr Infos</a>
#     </div>
#     """
    
#     folium.Marker(
#         location=[row['lat'], row['lon']],
#         popup=folium.Popup(popup_html, max_width=320),
#         tooltip=row['title'],
#         icon=folium.Icon(color='blue', icon='info-sign')
#     ).add_to(marker_cluster)

# os.makedirs('tip-berlin-map', exist_ok=True)
# berlin_map.save('tip-berlin-map/index.html')

# print(f"\n{'='*60}")
# print("✓✓✓ FERTIG! ✓✓✓")
# print(f"\n📂 tip-berlin-map/index.html")
# print(f"📊 tip_berlin_events.csv")
# print(f"{'='*60}")

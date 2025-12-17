import requests
from bs4 import BeautifulSoup
import json
import pandas as pd

def scrape_ra_clubs(url, output_file="ra_clubs.csv"):
    # Header setzen, um wie ein echter Browser zu wirken
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    print(f"Rufe URL ab: {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen der Seite: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # Suche nach dem __NEXT_DATA__ Script-Tag, wo RA die Daten speichert
    next_data_tag = soup.find('script', id='__NEXT_DATA__')
    
    if not next_data_tag:
        print("Konnte die Datenstruktur (__NEXT_DATA__) nicht finden. Seite hat sich evtl. geändert.")
        return

    try:
        data_json = json.loads(next_data_tag.string)
        # Die Daten liegen oft im Apollo-State-Cache
        apollo_state = data_json.get('props', {}).get('apolloState', {})
        
        clubs_list = []
        
        # Wir suchen nach Einträgen, die wie Clubs/Venues aussehen
        # Schlüssel in Apollo sehen oft aus wie "Venue:12345" oder "Club:12345"
        for key, value in apollo_state.items():
            if key.startswith('Venue:') or key.startswith('Club:'):
                club_name = value.get('name')
                
                # Nur Einträge mit Namen verarbeiten
                if not club_name:
                    continue

                # Adresse extrahieren
                address = value.get('address', 'N/A')
                
                # URL zusammenbauen
                content_url = value.get('contentUrl')
                full_link = f"https://ra.co{content_url}" if content_url else "N/A"

                # Koordinaten sind in der Listenansicht oft nicht direkt enthalten.
                # Wir prüfen auf typische Felder, falls vorhanden.
                lat = value.get('latitude') or value.get('lat')
                lng = value.get('longitude') or value.get('lng')
                
                # Falls Koordinaten in einem nested object 'location' oder 'maps' liegen:
                if not lat and 'location' in value and isinstance(value['location'], dict):
                    lat = value['location'].get('latitude')
                    lng = value['location'].get('longitude')

                coords = f"{lat}, {lng}" if lat and lng else "Nicht in Liste verfügbar"

                clubs_list.append({
                    "Clubname": club_name,
                    "Adresse": address,
                    "Koordinaten": coords,
                    "Link": full_link
                })

        # In CSV speichern
        if clubs_list:
            df = pd.DataFrame(clubs_list)
            # Duplikate entfernen (manchmal sind Venues mehrfach verlinkt)
            df = df.drop_duplicates(subset=['Clubname'])
            
            df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Erfolgreich {len(df)} Clubs extrahiert und in '{output_file}' gespeichert.")
            print(df.head())
        else:
            print("Keine Club-Einträge im Datensatz gefunden.")

    except json.JSONDecodeError:
        print("Fehler beim Parsen der JSON-Daten.")

if __name__ == "__main__":
    target_url = "https://de.ra.co/clubs"
    scrape_ra_clubs(target_url)

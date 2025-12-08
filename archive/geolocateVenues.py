from geopy.geocoders import Nominatim
from time import sleep
import pandas as pd

geolocator = Nominatim(user_agent="berlin-events-venue-locator")

def geocode_venue(venue_name, city="Berlin"):
    """
    Geocodiert einen Veranstaltungsort-Namen
    """
    try:
        # Kombiniere Venue-Name mit Stadt für bessere Ergebnisse
        search_query = f"{venue_name}, {city}, Germany"
        location = geolocator.geocode(search_query)
        
        if location:
            print(f"  ✓ {venue_name}: {location.latitude:.4f}, {location.longitude:.4f}")
            return location.latitude, location.longitude
        else:
            print(f"  ✗ {venue_name}: Nicht gefunden")
            return None, None
    except Exception as e:
        print(f"  ✗ {venue_name}: Fehler - {e}")
        return None, None

# Beispiel-Verwendung
venues = [
    "Humboldt Forum",
    "Berghain",
    "Philharmonie Berlin",
    "Olympiastadion Berlin",
    "Tempodrom"
]

print("Geocoding Veranstaltungsorte...\n")

results = []
for venue in venues:
    lat, lon = geocode_venue(venue)
    results.append({
        'venue': venue,
        'lat': lat,
        'lon': lon
    })
    sleep(1.5)  # Rate limiting

df = pd.DataFrame(results)
df_valid = df.dropna(subset=['lat', 'lon'])

print(f"\n{len(df_valid)}/{len(venues)} Venues gefunden")

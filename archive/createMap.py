import pandas as pd
import folium
from folium.plugins import MarkerCluster
import os

# CSV-Datei einlesen
df = pd.read_csv('berlin_events_mittwoch.csv')

# Nur Events mit gültigen Koordinaten
df_mapped = df.dropna(subset=['lat', 'lon'])

print(f"Lade {len(df_mapped)} Events aus CSV...")

# Karte erstellen
berlin_map = folium.Map(
    location=[52.5200, 13.4050],
    zoom_start=12,
    tiles='OpenStreetMap'
)

# MarkerCluster erstellen
marker_cluster = MarkerCluster().add_to(berlin_map)

# Marker aus CSV-Daten hinzufügen
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

# Ordner erstellen und Karte speichern
os.makedirs('berlin-events', exist_ok=True)
berlin_map.save('berlin-events/index.html')

print("✓ Karte mit Clustern erstellt: berlin-events/index.html")

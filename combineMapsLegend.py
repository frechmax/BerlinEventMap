import pandas as pd
import folium
import os
import sys
from datetime import datetime
from folium.plugins import MarkerCluster

# Get run folder from command line argument
RUN_FOLDER = sys.argv[1] if len(sys.argv) > 1 else '.'

print("=" * 60)
print("MULTI-SOURCE EVENT MAP GENERATOR")
print("=" * 60)
print(f"Reading CSVs from: {RUN_FOLDER}\n")

# Define your CSV files and their colors
csv_sources = [
    {
        'file': os.path.join(RUN_FOLDER, 'gratis_berlin_events.csv'),
        'name': 'Gratis in Berlin',
        'color': 'green',
        'icon': 'gift'
    },
    {
        'file': os.path.join(RUN_FOLDER, 'tip_berlin_events.csv'),
        'name': 'tip Berlin',
        'color': 'blue',
        'icon': 'star'
    },
    # Add more CSV sources here
    # {
    #     'file': 'another_events.csv',
    #     'name': 'Another Source',
    #     'color': 'red',
    #     'icon': 'info-sign'
    # }
]

# Available colors: red, blue, green, purple, orange, darkred, 
#                   lightred, beige, darkblue, darkgreen, cadetblue, 
#                   darkpurple, white, pink, lightblue, lightgreen, gray, black, lightgray


print("\n[1/3] Lade CSV-Dateien...\n")

all_events = []
source_stats = {}

for source in csv_sources:
    try:
        df = pd.read_csv(source['file'])
        df['source'] = source['name']
        df['color'] = source['color']
        df['icon'] = source['icon']
        
        df_valid = df.dropna(subset=['lat', 'lon'])
        all_events.append(df_valid)
        source_stats[source['name']] = len(df_valid)
        
        print(f"✓ {source['name']}: {len(df_valid)} events")
        
    except FileNotFoundError:
        print(f"✗ {source['name']}: Datei nicht gefunden")
    except Exception as e:
        print(f"✗ {source['name']}: Fehler - {e}")

if not all_events:
    print("\n✗ Keine Daten gefunden!")
    exit()

combined_df = pd.concat(all_events, ignore_index=True)

print(f"\n{'='*60}")
print(f"✓ Gesamt: {len(combined_df)} Events")
print(f"{'='*60}")

# Karte erstellen
print("\n[2/3] Erstelle Karte mit Legende...\n")

berlin_map = folium.Map(
    location=[52.5200, 13.4050],
    zoom_start=12,
    prefer_canvas=True,
    tiles='OpenStreetMap'
)

# Bereite Daten für FastMarkerCluster vor
def prepare_data_for_cluster(df):
    data = []
    for idx, row in df.iterrows():
        # Popup HTML
        popup = f"""
        <b>{row.get('title', 'Event')}</b><br>
        {row.get('address', '')}<br>
        <a href="{row.get('url', '#')}" target="_blank">Info</a>
        """
        
        # [lat, lon, popup, icon_color]
        data.append([
            row['lat'], 
            row['lon'],
            popup
        ])
    return data

# Separate Cluster für jede Quelle
clusters = {}
for source in csv_sources:
    if source['name'] in source_stats:
        clusters[source['name']] = MarkerCluster(
            name=source['name'],
            max_cluster_radius=40,
            # disable_clustering_at_zoom=15,
            chunked_loading=True, 
            spiderfyOnMaxZoom=True,
            showCoverageOnHover=False,
            animate=False
        ).add_to(berlin_map)

# Marker hinzufügen
for idx, row in combined_df.iterrows():
    popup_fields = []
    
    if 'title' in row and pd.notna(row['title']):
        popup_fields.append(f"<h4>{row['title']}</h4>")
    
    if 'category' in row and pd.notna(row['category']):
        popup_fields.append(f"<p><b>Kategorie:</b> {row['category']}</p>")
    
    if 'venue' in row and pd.notna(row['venue']):
        popup_fields.append(f"<p><b>Venue:</b> {row['venue']}</p>")
    
    if 'address' in row and pd.notna(row['address']):
        popup_fields.append(f"<p><b>Adresse:</b> {row['address']}</p>")
    
    if 'date' in row and pd.notna(row['date']):
        popup_fields.append(f"<p><b>Datum:</b> {row['date']}</p>")
    
    if 'detailed_date' in row and pd.notna(row['detailed_date']):
        popup_fields.append(f"<p><b>Zeitangabe:</b> {row['detailed_date']}</p>")
    
    if 'description' in row and pd.notna(row['description']):
        popup_fields.append(f"<p><b>Beschreibung:</b> {row['description']}</p>")
    
    popup_fields.append(f"<p><b>Quelle:</b> {row['source']}</p>")
    
    if 'url' in row and pd.notna(row['url']):
        popup_fields.append(f"<a href='{row['url']}' target='_blank'>Mehr Infos</a>")
    
    popup_html = f"<div style='width: 280px;'>{''.join(popup_fields)}</div>"
    
    marker = folium.Marker(
        location=[row['lat'], row['lon']],
        popup=folium.Popup(popup_html, max_width=320),
        tooltip=row.get('title', 'Event'),
        icon=folium.Icon(color=row['color'], icon=row['icon'])
    )
    
    marker.add_to(clusters[row['source']])

# Layer Control hinzufügen
folium.LayerControl(position='topright').add_to(berlin_map)

# AKTUELLES DATUM UND WOCHENTAG
now = datetime.now()
wochentag = now.strftime('%A')  # z.B. "Samstag"
datum = now.strftime('%d.%m.%Y')  # z.B. "06.12.2025"

# Deutsche Wochentage (falls locale nicht funktioniert)
wochentage_de = {
    'Monday': 'Montag',
    'Tuesday': 'Dienstag', 
    'Wednesday': 'Mittwoch',
    'Thursday': 'Donnerstag',
    'Friday': 'Freitag',
    'Saturday': 'Samstag',
    'Sunday': 'Sonntag'
}
wochentag = wochentage_de.get(now.strftime('%A'), wochentag)

# LEGENDE MIT DATUM
legend_html = f'''
<div style="position: fixed; 
            bottom: 10px; 
            right: 10px; 
            width: 120px; 
            background-color: rgba(255,255,255,0.95); 
            border: 1px solid #ddd;
            z-index: 9999; 
            font-size: 10px;
            padding: 5px 6px;
            border-radius: 3px;
            box-shadow: 0 1px 4px rgba(0,0,0,0.2);
            ">
    <div style="font-weight: bold; font-size: 11px; margin-bottom: 3px;">
        📍 {len(combined_df)} Events
    </div>
    <div style="font-size: 9px; color: #666; margin-bottom: 4px;">
        {wochentag[:2]}, {datum}
    </div>
'''

color_emoji = {
    'red': '🔴',
    'blue': '🔵',
    'green': '🟢',
    'purple': '🟣',
    'orange': '🟠',
}

for source in csv_sources:
    if source['name'] in source_stats:
        emoji = color_emoji.get(source['color'], '🔵')
        count = source_stats[source['name']]
        
        legend_html += f'''
        <div style="display: flex; justify-content: space-between; margin: 2px 0; font-size: 9px;">
            <span>{emoji} {source['name'][:20]}</span>
            <span style="font-weight: bold; color: #666;">{count}</span>
        </div>
        '''

legend_html += '</div>'

berlin_map.get_root().html.add_child(folium.Element(legend_html))

# Save map
print("\n[3/3] Saving map...\n")

map_file = os.path.join(RUN_FOLDER, 'index.html')
berlin_map.save(map_file)

print(f"✓ Map saved to: {map_file}")
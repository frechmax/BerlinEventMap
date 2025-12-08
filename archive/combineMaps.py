import pandas as pd
import folium
import os
from datetime import datetime

print("=" * 60)
print("MULTI-SOURCE EVENT MAP GENERATOR")
print("=" * 60)

# Define your CSV files and their colors
csv_sources = [
    {
        'file': datetime.now().strftime('%Y-%m-%d') + 'gratis_berlin_events.csv',  # gratis-in-berlin.de
        'name': 'Gratis in Berlin',
        'color': 'green',
        'icon': 'gift'
    },
    {
        'file':  datetime.now().strftime('%Y-%m-%d') + 'tip_berlin_events.csv',  # tip-berlin.de
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

print("\n[1/3] Loading CSV files...\n")

all_events = []
source_stats = {}

for source in csv_sources:
    try:
        df = pd.read_csv(source['file'])
        df['source'] = source['name']
        df['color'] = source['color']
        df['icon'] = source['icon']
        
        # Filter for valid coordinates
        df_valid = df.dropna(subset=['lat', 'lon'])
        
        all_events.append(df_valid)
        source_stats[source['name']] = len(df_valid)
        
        print(f"✓ {source['name']}: {len(df_valid)} events loaded from {source['file']}")
        
    except FileNotFoundError:
        print(f"✗ {source['name']}: File '{source['file']}' not found - skipping")
    except Exception as e:
        print(f"✗ {source['name']}: Error - {e}")

if not all_events:
    print("\n✗ No valid CSV files found! Exiting.")
    exit()

# Combine all dataframes
combined_df = pd.concat(all_events, ignore_index=True)

print(f"\n{'='*60}")
print(f"✓ Total events loaded: {len(combined_df)}")
for source_name, count in source_stats.items():
    print(f"  - {source_name}: {count} events")
print(f"{'='*60}")

# Create map
print(f"\n[2/3] Creating interactive map...\n")

berlin_map = folium.Map(
    location=[52.5200, 13.4050],
    zoom_start=12,
    tiles='OpenStreetMap'
)

# from folium.plugins import FastMarkerCluster
from folium.plugins import MarkerCluster


# # Create separate marker clusters for each source
# clusters = {}
# for source in csv_sources:
#     if source['name'] in source_stats:
#         clusters[source['name']] = MarkerCluster(name=source['name']).add_to(berlin_map)

# MarkerCluster mit angepassten Einstellungen
marker_cluster = MarkerCluster(
    max_cluster_radius=2,  # Standard: 80 - kleinerer Wert = weniger Clustering
    # disable_clustering_at_zoom=13,  # Ab Zoom-Level 13 keine Cluster mehr
    spiderfyOnMaxZoom=True,  # Marker verteilen sich beim maximalen Zoom
    showCoverageOnHover=False,  # Keine Cluster-Grenze beim Hover
    zoomToBoundsOnClick=True  # Zoom beim Klick auf Cluster
).add_to(berlin_map)

# # Sammle alle Koordinaten
# locations = []
# for idx, row in combined_df.iterrows():
#     locations.append([row['lat'], row['lon']])

# # FastMarkerCluster
# marker_cluster = FastMarkerCluster(
#     data=locations,
#     disableClusteringAtZoom=12  # Ab Zoom 12 keine Cluster
# ).add_to(berlin_map)

# Add markers
marker_count = 0
for idx, row in combined_df.iterrows():
    # Build popup HTML with all available fields
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
    
    popup_fields.append(f"<p><b>Quelle:</b> {row['source']}</p>")
    
    if 'url' in row and pd.notna(row['url']):
        popup_fields.append(f"<a href='{row['url']}' target='_blank'>Mehr Infos</a>")
    
    popup_html = f"<div style='width: 280px;'>{''.join(popup_fields)}</div>"
    
    # Create marker
    marker = folium.Marker(
        location=[row['lat'], row['lon']],
        popup=folium.Popup(popup_html, max_width=320),
        tooltip=row.get('title', 'Event'),
        icon=folium.Icon(color=row['color'], icon=row['icon'])
    )
    
    # Add to appropriate cluster
    marker.add_to(marker_cluster)
    marker_count += 1

print(f"✓ Added {marker_count} markers to map")

# Add layer control to toggle sources
folium.LayerControl().add_to(berlin_map)

# Save map
print(f"\n[3/3] Saving map...\n")

os.makedirs('combined-events-map/' + datetime.now().strftime('%Y-%m-%d %H-%M-%S'), exist_ok=True)
berlin_map.save('combined-events-map/' + datetime.now().strftime('%Y-%m-%d %H-%M-%S') + '/index.html')

print(f"{'='*60}")
print(f"✓✓✓ SUCCESS! ✓✓✓")
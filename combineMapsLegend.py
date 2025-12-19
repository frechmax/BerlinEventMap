import pandas as pd
import folium
import os
import sys
import glob
from datetime import datetime
from folium.plugins import MarkerCluster

def combine_maps_with_legend(run_folder='.'):
    """Combine event data from multiple CSV sources into a single map."""
    print("=" * 60)
    print("MULTI-SOURCE EVENT MAP GENERATOR")
    print("=" * 60)
    print(f"Reading CSVs from: {run_folder}\n")

    # Define your CSV files and their colors
    csv_sources = [
        {
            'file': os.path.join(run_folder, 'tip_berlin_events.csv'),
            'name': 'tip Berlin',
            'color': 'blue',
            'icon': 'star'
        },
        {
            'file': os.path.join(run_folder, 'gratis_berlin_events.csv'),
            'name': 'Gratis in Berlin',
            'color': 'green',
            'icon': 'gift'
        },
        {
            'file': os.path.join(run_folder, 'visitberlin_events.csv'),
            'name': 'visit Berlin',
            'color': 'purple',
            'icon': 'info-sign'
        },
        {
            'file': os.path.join(run_folder, 'RA_*_events.csv'),
            'name': 'Resident Advisor',
            'color': 'red',
            'icon': 'music'
        }
    ]

    # Available colors: red, blue, green, purple, orange, darkred, 
    #                   lightred, beige, darkblue, darkgreen, cadetblue, 
    #                   darkpurple, white, pink, lightblue, lightgreen, gray, black, lightgray


    print("\n[1/3] Lade CSV-Dateien...\n")

    all_events = []
    source_stats = {}

    for source in csv_sources:
        try:
            # Handle wildcard patterns for RA events
            file_path = source['file']
            if '*' in file_path:
                # Find matching file
                matches = glob.glob(file_path)
                if matches:
                    file_path = matches[0]  # Use first match
                else:
                    raise FileNotFoundError(f"No files matching pattern: {file_path}")
            
            df = pd.read_csv(file_path)
            df['source'] = source['name']
            df['color'] = source['color']
            df['icon'] = source['icon']
            
            # Normalize column names for latitude/longitude
            if 'Venue Latitude' in df.columns and 'Venue Longitude' in df.columns:
                df['lat'] = df['Venue Latitude']
                df['lon'] = df['Venue Longitude']
            
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
        return None

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
        
        # Event Title
        if 'title' in row and pd.notna(row['title']):
            popup_fields.append(f"<h4>{row['title']}</h4>")
        elif 'Event name' in row and pd.notna(row['Event name']):
            popup_fields.append(f"<h4>{row['Event name']}</h4>")
        
        # Category
        if 'category' in row and pd.notna(row['category']):
            popup_fields.append(f"<p><b>Kategorie:</b> {row['category']}</p>")
        
        # Venue
        if 'Venue' in row and pd.notna(row['Venue']):
            popup_fields.append(f"<p><b>Venue:</b> {row['Venue']}</p>")
        elif 'venue' in row and pd.notna(row['venue']):
            popup_fields.append(f"<p><b>Venue:</b> {row['venue']}</p>")
        
        # Address
        if 'Venue Address' in row and pd.notna(row['Venue Address']):
            popup_fields.append(f"<p><b>Adresse:</b> {row['Venue Address']}</p>")
        elif 'address' in row and pd.notna(row['address']):
            popup_fields.append(f"<p><b>Adresse:</b> {row['address']}</p>")
        
        # Date
        if 'Date' in row and pd.notna(row['Date']):
            popup_fields.append(f"<p><b>Datum:</b> {row['Date']}</p>")
        elif 'date' in row and pd.notna(row['date']):
            popup_fields.append(f"<p><b>Datum:</b> {row['date']}</p>")
        
        # Start Time (RA specific)
        if 'Start Time' in row and pd.notna(row['Start Time']):
            popup_fields.append(f"<p><b>Start:</b> {row['Start Time']}</p>")
        
        # End Time (RA specific)
        if 'End Time' in row and pd.notna(row['End Time']):
            popup_fields.append(f"<p><b>Ende:</b> {row['End Time']}</p>")
        
        # Artists (RA specific)
        if 'Artists' in row and pd.notna(row['Artists']):
            popup_fields.append(f"<p><b>Artists:</b> {row['Artists']}</p>")
        
        # Guests attending (RA specific)
        if 'Number of guests attending' in row and pd.notna(row['Number of guests attending']):
            popup_fields.append(f"<p><b>Interested:</b> {int(row['Number of guests attending'])}</p>")
        
        # Detailed date (other sources)
        if 'detailed_date' in row and pd.notna(row['detailed_date']):
            popup_fields.append(f"<p><b>Zeitangabe:</b> {row['detailed_date']}</p>")
        
        # Description
        if 'description' in row and pd.notna(row['description']):
            popup_fields.append(f"<p><b>Beschreibung:</b> {row['description']}</p>")
        
        # Source
        popup_fields.append(f"<p><b>Quelle:</b> {row['source']}</p>")
        
        # URL/Link
        if 'Event URL' in row and pd.notna(row['Event URL']):
            url = row['Event URL']
            if not url.startswith('http'):
                url = f"https://ra.co{url}"
            popup_fields.append(f"<a href='{url}' target='_blank'>Mehr Infos</a>")
        elif 'url' in row and pd.notna(row['url']):
            popup_fields.append(f"<a href='{row['url']}' target='_blank'>Mehr Infos</a>")
        
        popup_html = f"<div style='width: 320px;'>{''.join(popup_fields)}</div>"
        
        # Get tooltip text - find first non-NaN title/name field
        tooltip_text = 'Event'
        if 'title' in row and pd.notna(row['title']):
            tooltip_text = str(row['title'])
        elif 'Event name' in row and pd.notna(row['Event name']):
            tooltip_text = str(row['Event name'])
        elif 'name' in row and pd.notna(row['name']):
            tooltip_text = str(row['name'])
        
        marker = folium.Marker(
            location=[row['lat'], row['lon']],
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=tooltip_text,
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

    map_file = os.path.join(run_folder, 'index.html')
    berlin_map.save(map_file)

    # copy to index.html for GitHub Pages
    index_file = os.path.join('.', 'index.html')
    berlin_map.save(index_file)
    print(f"✓ Map saved to: {map_file}")
    
    return berlin_map

if __name__ == "__main__":
    # Get run folder from command line argument
    RUN_FOLDER = sys.argv[1] if len(sys.argv) > 1 else '.'
    combine_maps_with_legend(RUN_FOLDER)
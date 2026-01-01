"""
lean_main.py - Direkter Event Map Generator (ohne CSV-Zwischenspeicherung)

Führt alle Scraper aus, holt die DataFrames direkt und erstellt eine HTML-Karte,
ohne CSV-Dateien als Zwischenspeicher zu nutzen.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import folium
import pandas as pd
from folium.plugins import MarkerCluster

# Import der Scraper-Funktionen
from scrapeGratisInBerlinParallel import run_gratis_berlin_scraper
from scrapeTipBerlinBot import run_tip_berlin_scraper
from scrapeVisitBerlin import run_visitberlin_scraper
from RA_event_fetcher import EventFetcher

# Farb-Emoji-Mapping für Legende
COLOR_EMOJI = {
    "red": "🔴",
    "blue": "🔵",
    "green": "🟢",
    "purple": "🟣",
    "orange": "🟠",
}

# Deutsche Wochentagsnamen
WEEKDAYS_DE = {
    "Monday": "Montag",
    "Tuesday": "Dienstag",
    "Wednesday": "Mittwoch",
    "Thursday": "Donnerstag",
    "Friday": "Freitag",
    "Saturday": "Samstag",
    "Sunday": "Sonntag",
}


def format_datetime(value: str) -> str:
    """Formatiert ISO-Datetime zu lesbarem Format."""
    if not value or not isinstance(value, str):
        return str(value) if value else ""
    
    try:
        dt_str = value.split(".")[0]
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
        
        if dt.hour == 0 and dt.minute == 0:
            return dt.strftime("%d.%m.%Y")
        return dt.strftime("%d.%m.%Y %H:%M")
    except (ValueError, AttributeError):
        return str(value)


def get_event_field(row: pd.Series, *field_names: str) -> Optional[any]:
    """Holt den ersten nicht-null Wert aus einer Liste von Feldnamen."""
    for field in field_names:
        if field in row and pd.notna(row[field]):
            return row[field]
    return None


def build_popup_html(row: pd.Series) -> str:
    """Erstellt HTML-Popup für einen Marker."""
    parts = []
    
    # Titel
    title = get_event_field(row, "title", "Event name")
    if title:
        parts.append(f"<h4>{title}</h4>")
    
    # Kategorie
    category = get_event_field(row, "category")
    if category:
        parts.append(f"<p><b>Kategorie:</b> {category}</p>")
    
    # Venue
    venue = get_event_field(row, "Venue", "venue")
    if venue:
        parts.append(f"<p><b>Venue:</b> {venue}</p>")
    
    # Adresse
    address = get_event_field(row, "Venue Address", "address")
    if address:
        parts.append(f"<p><b>Adresse:</b> {address}</p>")
    
    # Datum
    date = get_event_field(row, "Date", "date", "detailed_date")
    if date:
        parts.append(f"<p><b>Datum:</b> {format_datetime(date) if 'T' in str(date) else date}</p>")
    
    # Beschreibung
    description = get_event_field(row, "description")
    if description:
        desc_short = str(description)[:200] + "..." if len(str(description)) > 200 else description
        parts.append(f"<p><b>Beschreibung:</b> {desc_short}</p>")
    
    # Quelle
    parts.append(f"<p><b>Quelle:</b> {row['source']}</p>")
    
    # URL
    url = get_event_field(row, "Event URL", "url")
    if url:
        parts.append(f"<a href='{url}' target='_blank'>Mehr Infos</a>")
    
    return f"<div style='width: 320px;'>{''.join(parts)}</div>"


def build_legend_html(event_count: int, source_stats: dict[str, int]) -> str:
    """Erstellt HTML für die Kartenlegende."""
    now = datetime.now()
    weekday = WEEKDAYS_DE.get(now.strftime("%A"), now.strftime("%A"))
    date_str = now.strftime("%d.%m.%Y")
    
    legend_parts = [
        f'''<div style="position: fixed; 
                bottom: 10px; 
                right: 10px; 
                width: 120px; 
                background-color: rgba(255,255,255,0.95); 
                border: 1px solid #ddd;
                z-index: 9999; 
                font-size: 10px;
                padding: 5px 6px;
                border-radius: 3px;
                box-shadow: 0 1px 4px rgba(0,0,0,0.2);">
        <div style="font-weight: bold; font-size: 11px; margin-bottom: 3px;">
            📍 {event_count} Events
        </div>
        <div style="font-size: 9px; color: #666; margin-bottom: 4px;">
            {weekday[:2]}, {date_str}
        </div>'''
    ]
    
    for source_name, count in source_stats.items():
        color = {
            "Gratis in Berlin": "green",
            "tip Berlin": "blue",
            "Visit Berlin": "purple",
            "Resident Advisor": "red",
        }.get(source_name, "blue")
        
        emoji = COLOR_EMOJI.get(color, "🔵")
        legend_parts.append(f'''
        <div style="display: flex; justify-content: space-between; margin: 2px 0; font-size: 9px;">
            <span>{emoji} {source_name[:20]}</span>
            <span style="font-weight: bold; color: #666;">{count}</span>
        </div>''')
    
    legend_parts.append("</div>")
    return "".join(legend_parts)


def create_map_from_dataframes(
    dataframes: dict[str, pd.DataFrame],
    output_path: str = "index.html"
) -> Optional[folium.Map]:
    """
    Erstellt eine Folium-Karte direkt aus DataFrames.
    
    Args:
        dataframes: Dict mit source_name -> DataFrame
        output_path: Pfad für die HTML-Ausgabe
        
    Returns:
        Folium Map Objekt oder None
    """
    print("\n[3/3] Erstelle kombinierte Karte...")
    
    # Source-Konfiguration (Name, Farbe, Icon)
    source_config = {
        "Gratis in Berlin": {"color": "green", "icon": "gift"},
        "tip Berlin": {"color": "blue", "icon": "star"},
        "Visit Berlin": {"color": "purple", "icon": "info-sign"},
        "Resident Advisor": {"color": "red", "icon": "music"},
    }
    
    # DataFrames mit Quelle markieren
    all_events = []
    source_stats = {}
    
    for source_name, df in dataframes.items():
        if df.empty:
            continue
        
        df = df.copy()
        df["source"] = source_name
        
        # Farbe und Icon aus Konfiguration
        config = source_config.get(source_name, {"color": "blue", "icon": "circle"})
        df["color"] = config["color"]
        df["icon"] = config["icon"]
        
        # Nur Events mit Koordinaten
        df_valid = df.dropna(subset=["lat", "lon"])
        
        if not df_valid.empty:
            all_events.append(df_valid)
            source_stats[source_name] = len(df_valid)
            print(f"  ✓ {source_name}: {len(df_valid)} events")
    
    if not all_events:
        print("\n✗ Keine Events mit Koordinaten gefunden!")
        return None
    
    # Kombiniere alle DataFrames
    combined_df = pd.concat(all_events, ignore_index=True)
    print(f"\n  ✓ Gesamt: {len(combined_df)} Events mit Koordinaten")
    
    # Erstelle Karte
    berlin_map = folium.Map(
        location=[52.5200, 13.4050],
        zoom_start=12,
        prefer_canvas=True,
        tiles="OpenStreetMap",
    )
    
    # Cluster-Icon (grau)
    grey_cluster_icon = """
    function(cluster) {
        var childCount = cluster.getChildCount();
        var size = 30;
        if (childCount > 10) size = 40;
        if (childCount > 50) size = 50;
        
        return L.divIcon({
            html: '<div style="background-color: rgba(110, 110, 110, 0.8); color: white; border-radius: 50%; width: ' + size + 'px; height: ' + size + 'px; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 12px; border: 2px solid rgba(160, 160, 160, 0.5);">' + childCount + '</div>',
            className: 'marker-cluster-grey',
            iconSize: L.point(size, size)
        });
    }
    """
    
    # Erstelle separate Cluster für jede Quelle
    clusters = {}
    for source_name in source_stats.keys():
        clusters[source_name] = MarkerCluster(
            name=source_name,
            max_cluster_radius=40,
            chunked_loading=True,
            spiderfyOnMaxZoom=True,
            showCoverageOnHover=False,
            animate=False,
            icon_create_function=grey_cluster_icon,
        ).add_to(berlin_map)
    
    # Füge Marker hinzu
    for _, row in combined_df.iterrows():
        popup_html = build_popup_html(row)
        tooltip_text = str(get_event_field(row, "title", "Event name") or "Event")
        
        marker = folium.Marker(
            location=[row["lat"], row["lon"]],
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=tooltip_text,
            icon=folium.Icon(color=row["color"], icon=row["icon"]),
        )
        
        marker.add_to(clusters[row["source"]])
    
    # Füge Layer-Control hinzu
    folium.LayerControl(position="topright").add_to(berlin_map)
    
    # Füge Legende hinzu
    legend_html = build_legend_html(len(combined_df), source_stats)
    berlin_map.get_root().html.add_child(folium.Element(legend_html))
    
    # Speichere Karte
    berlin_map.save(output_path)
    print(f"\n  ✓ Karte gespeichert: {output_path}")
    
    return berlin_map


def fetch_ra_events() -> pd.DataFrame:
    """
    Holt RA Events direkt über die API und gibt DataFrame zurück.
    
    Returns:
        DataFrame mit RA Events
    """
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        listing_date_gte = f"{today}T00:00:00.000Z"
        listing_date_lte = f"{today}T23:59:59.999Z"
        
        print("  Fetching RA events via GraphQL API...")
        event_fetcher = EventFetcher(34, listing_date_gte, listing_date_lte)  # 34 = Berlin
        events = event_fetcher.fetch_all_events()
        
        if not events:
            print("  ⚠ No RA events found")
            return pd.DataFrame()
        
        # Konvertiere zu DataFrame
        rows = []
        for event in events:
            event_data = event["event"]
            venue = event_data["venue"]
            
            # Hole Koordinaten aus dem Cache
            venue_key = f"{venue['name']}|{venue.get('address', '')}"
            lat, lon = event_fetcher._venues_cache.get(venue_key, (None, None))
            
            rows.append({
                "title": event_data["title"],
                "Date": event_data["date"],
                "Start Time": event_data["startTime"],
                "End Time": event_data["endTime"],
                "Artists": ", ".join(artist["name"] for artist in event_data["artists"]),
                "Venue": venue["name"],
                "Venue Address": venue.get("address", ""),
                "lat": lat,
                "lon": lon,
                "Event URL": event_data["contentUrl"],
                "Number of guests attending": event_data["attending"],
            })
        
        df = pd.DataFrame(rows)
        return df.dropna(subset=["lat", "lon"])
    
    except Exception as e:
        print(f"  ✗ RA API Error: {e}")
        return pd.DataFrame()


def run_scrapers_direct() -> dict[str, pd.DataFrame]:
    """
    Führt alle Scraper aus und gibt DataFrames zurück.
    
    Returns:
        Dict mit source_name -> DataFrame
    """
    dataframes = {}
    
    print("=" * 70)
    print("LEAN BERLIN EVENTS MAP - DIREKTE VERARBEITUNG (OHNE CSV)")
    print("=" * 70)
    
    # Temporäres Verzeichnis für Scraper (die es erwarten)
    temp_dir = Path("temp_scraper_output")
    temp_dir.mkdir(exist_ok=True)
    
    try:
        # 1. Resident Advisor
        print("\n[1/4] Scrape Resident Advisor...")
        print("-" * 70)
        try:
            df_ra = fetch_ra_events()
            if not df_ra.empty:
                dataframes["Resident Advisor"] = df_ra
                print(f"✓ Resident Advisor: {len(df_ra)} events erfolgreich geladen")
        except Exception as e:
            print(f"✗ Resident Advisor Fehler: {e}")
        
        # 2. Gratis in Berlin
        print("\n[2/4] Scrape Gratis in Berlin...")
        print("-" * 70)
        try:
            df_gratis = run_gratis_berlin_scraper(str(temp_dir))
            if not df_gratis.empty:
                dataframes["Gratis in Berlin"] = df_gratis
                print(f"✓ Gratis in Berlin: {len(df_gratis)} events erfolgreich geladen")
        except Exception as e:
            print(f"✗ Gratis in Berlin Fehler: {e}")
        
        # 3. tip Berlin
        print("\n[3/4] Scrape tip Berlin...")
        print("-" * 70)
        try:
            df_tip = run_tip_berlin_scraper(str(temp_dir))
            if not df_tip.empty:
                dataframes["tip Berlin"] = df_tip
                print(f"✓ tip Berlin: {len(df_tip)} events erfolgreich geladen")
        except Exception as e:
            print(f"✗ tip Berlin Fehler: {e}")
        
        # 4. Visit Berlin
        print("\n[4/4] Scrape Visit Berlin...")
        print("-" * 70)
        try:
            df_visit = run_visitberlin_scraper(str(temp_dir))
            if not df_visit.empty:
                dataframes["Visit Berlin"] = df_visit
                print(f"✓ Visit Berlin: {len(df_visit)} events erfolgreich geladen")
        except Exception as e:
            print(f"✗ Visit Berlin Fehler: {e}")
    
    finally:
        # Aufräumen: Lösche temporäre CSVs
        import shutil
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            print(f"\n  ℹ Temporäre Dateien bereinigt")
    
    return dataframes


def main():
    """Hauptfunktion."""
    print("\n🚀 Starte direkten Event-Scraping-Prozess...")
    
    # Erstelle Output-Ordner
    output_folder = Path("output") / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_folder.mkdir(parents=True, exist_ok=True)
    output_html = output_folder / "index.html"
    
    # Führe Scraper aus und hole DataFrames
    dataframes = run_scrapers_direct()
    
    if not dataframes:
        print("\n✗ Keine Events gefunden!")
        return False
    
    # Erstelle Karte direkt aus DataFrames
    map_obj = create_map_from_dataframes(dataframes, str(output_html))
    
    if map_obj:
        print("\n" + "=" * 70)
        print("✓✓✓ ERFOLGREICH ABGESCHLOSSEN! ✓✓✓")
        print("=" * 70)
        print(f"\n📂 Output: {output_html}")
        print("💡 Keine CSV-Dateien wurden erstellt - direkte Verarbeitung!")
        print(f"\n📊 Quellen: {', '.join(dataframes.keys())}")
        return True
    else:
        print("\n✗ Fehler beim Erstellen der Karte")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n✗ Abgebrochen durch Benutzer")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Unerwarteter Fehler: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

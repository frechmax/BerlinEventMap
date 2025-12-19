"""
combineMapsLegend.py - Multi-Source Event Map Generator

Combines event data from multiple CSV sources (tip Berlin, Gratis in Berlin,
Visit Berlin, Resident Advisor) into a single interactive Folium map with
a legend and layer controls.
"""

import glob
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import folium
import pandas as pd
from folium.plugins import MarkerCluster

# German weekday names
WEEKDAYS_DE = {
    "Monday": "Montag",
    "Tuesday": "Dienstag",
    "Wednesday": "Mittwoch",
    "Thursday": "Donnerstag",
    "Friday": "Freitag",
    "Saturday": "Samstag",
    "Sunday": "Sonntag",
}

# Color emoji mapping for legend
COLOR_EMOJI = {
    "red": "🔴",
    "blue": "🔵",
    "green": "🟢",
    "purple": "🟣",
    "orange": "🟠",
}


@dataclass
class CSVSource:
    """Configuration for a CSV data source."""
    file_pattern: str
    name: str
    color: str
    icon: str


# CSV source configurations
CSV_SOURCES = [
    CSVSource("tip_berlin_events.csv", "tip Berlin", "blue", "star"),
    CSVSource("gratis_berlin_events.csv", "Gratis in Berlin", "green", "gift"),
    CSVSource("visitberlin_events.csv", "visit Berlin", "purple", "info-sign"),
    CSVSource("RA_*_events.csv", "Resident Advisor", "red", "music"),
]


def load_csv_sources(run_folder: str) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Load and combine all CSV sources.
    
    Args:
        run_folder: Path to the folder containing CSV files.
        
    Returns:
        Tuple of (combined DataFrame, source statistics dict).
    """
    all_events: list[pd.DataFrame] = []
    source_stats: dict[str, int] = {}

    for source in CSV_SOURCES:
        try:
            file_path = Path(run_folder) / source.file_pattern
            
            # Handle wildcard patterns (for RA events)
            if "*" in source.file_pattern:
                matches = glob.glob(str(file_path))
                # Also check root directory for RA files (they're saved there by RA_run_today.py)
                if not matches:
                    root_matches = glob.glob(source.file_pattern)
                    matches = root_matches
                if not matches:
                    raise FileNotFoundError(f"No files matching: {file_path}")
                file_path = Path(matches[-1])  # Use most recent file
            
            df = pd.read_csv(file_path)
            df["source"] = source.name
            df["color"] = source.color
            df["icon"] = source.icon
            
            # Normalize latitude/longitude columns
            if "Venue Latitude" in df.columns and "Venue Longitude" in df.columns:
                df["lat"] = df["Venue Latitude"]
                df["lon"] = df["Venue Longitude"]
            
            df_valid = df.dropna(subset=["lat", "lon"])
            all_events.append(df_valid)
            source_stats[source.name] = len(df_valid)
            
            print(f"✓ {source.name}: {len(df_valid)} events")
            
        except FileNotFoundError:
            print(f"✗ {source.name}: File not found")
        except Exception as e:
            print(f"✗ {source.name}: Error - {e}")

    if not all_events:
        return pd.DataFrame(), source_stats

    return pd.concat(all_events, ignore_index=True), source_stats


def get_event_field(row: pd.Series, *field_names: str) -> Optional[Any]:
    """
    Get the first non-null value from a list of possible field names.
    
    Args:
        row: DataFrame row.
        field_names: Field names to check in order.
        
    Returns:
        First non-null value found, or None.
    """
    for field in field_names:
        if field in row and pd.notna(row[field]):
            return row[field]
    return None


def format_datetime(value: str) -> str:
    """
    Format an ISO datetime string to a readable format.
    
    Args:
        value: ISO datetime string like "2025-12-19T20:00:00.000"
        
    Returns:
        Formatted string like "19.12.2025 20:00" or original if parsing fails.
    """
    if not value or not isinstance(value, str):
        return str(value) if value else ""
    
    try:
        # Handle ISO format with optional milliseconds
        dt_str = value.split(".")[0]  # Remove milliseconds
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
        
        # If time is midnight, only show date
        if dt.hour == 0 and dt.minute == 0:
            return dt.strftime("%d.%m.%Y")
        return dt.strftime("%d.%m.%Y %H:%M")
    except (ValueError, AttributeError):
        return str(value)


def format_time_only(value: str) -> str:
    """
    Format an ISO datetime string to show only time.
    
    Args:
        value: ISO datetime string like "2025-12-19T20:00:00.000"
        
    Returns:
        Formatted string like "20:00" or original if parsing fails.
    """
    if not value or not isinstance(value, str):
        return str(value) if value else ""
    
    try:
        dt_str = value.split(".")[0]
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%H:%M")
    except (ValueError, AttributeError):
        return str(value)


def build_popup_html(row: pd.Series) -> str:
    """
    Build HTML popup content for a map marker.
    
    Args:
        row: DataFrame row with event data.
        
    Returns:
        HTML string for the popup.
    """
    parts: list[str] = []
    
    # Title
    title = get_event_field(row, "title", "Event name")
    if title:
        parts.append(f"<h4>{title}</h4>")
    
    # Category
    category = get_event_field(row, "category")
    if category:
        parts.append(f"<p><b>Kategorie:</b> {category}</p>")
    
    # Venue
    venue = get_event_field(row, "Venue", "venue")
    if venue:
        parts.append(f"<p><b>Venue:</b> {venue}</p>")
    
    # Address
    address = get_event_field(row, "Venue Address", "address")
    if address:
        parts.append(f"<p><b>Adresse:</b> {address}</p>")
    
    # Date
    date = get_event_field(row, "Date", "date")
    if date:
        parts.append(f"<p><b>Datum:</b> {format_datetime(date)}</p>")
    
    # Time fields (RA specific)
    start_time = get_event_field(row, "Start Time")
    if start_time:
        parts.append(f"<p><b>Start:</b> {format_time_only(start_time)}</p>")
    
    end_time = get_event_field(row, "End Time")
    if end_time:
        parts.append(f"<p><b>Ende:</b> {format_time_only(end_time)}</p>")
    
    # Artists (RA specific)
    artists = get_event_field(row, "Artists")
    if artists:
        parts.append(f"<p><b>Artists:</b> {artists}</p>")
    
    # Guests attending (RA specific)
    attending = get_event_field(row, "Number of guests attending")
    if attending is not None:
        parts.append(f"<p><b>Interested:</b> {int(attending)}</p>")
    
    # Detailed date
    detailed_date = get_event_field(row, "detailed_date")
    if detailed_date:
        parts.append(f"<p><b>Zeitangabe:</b> {detailed_date}</p>")
    
    # Description
    description = get_event_field(row, "description")
    if description:
        parts.append(f"<p><b>Beschreibung:</b> {description}</p>")
    
    # Source
    parts.append(f"<p><b>Quelle:</b> {row['source']}</p>")
    
    # URL
    url = get_event_field(row, "Event URL", "url")
    if url:
        if not str(url).startswith("http"):
            url = f"https://ra.co{url}"
        parts.append(f"<a href='{url}' target='_blank'>Mehr Infos</a>")
    
    return f"<div style='width: 320px;'>{''.join(parts)}</div>"


def build_legend_html(event_count: int, source_stats: dict[str, int]) -> str:
    """
    Build HTML for the map legend.
    
    Args:
        event_count: Total number of events.
        source_stats: Dictionary of source names to event counts.
        
    Returns:
        HTML string for the legend.
    """
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
    
    for source in CSV_SOURCES:
        if source.name in source_stats:
            emoji = COLOR_EMOJI.get(source.color, "🔵")
            count = source_stats[source.name]
            legend_parts.append(f'''
        <div style="display: flex; justify-content: space-between; margin: 2px 0; font-size: 9px;">
            <span>{emoji} {source.name[:20]}</span>
            <span style="font-weight: bold; color: #666;">{count}</span>
        </div>''')
    
    legend_parts.append("</div>")
    return "".join(legend_parts)


def combine_maps_with_legend(run_folder: str = ".") -> Optional[folium.Map]:
    """
    Combine event data from multiple CSV sources into a single map.
    
    Args:
        run_folder: Path to the folder containing CSV files.
        
    Returns:
        Folium map object, or None if no data found.
    """
    print("=" * 60)
    print("MULTI-SOURCE EVENT MAP GENERATOR")
    print("=" * 60)
    print(f"Reading CSVs from: {run_folder}\n")

    print("\n[1/3] Loading CSV files...\n")
    combined_df, source_stats = load_csv_sources(run_folder)

    if combined_df.empty:
        print("\n✗ No data found!")
        return None

    print(f"\n{'=' * 60}")
    print(f"✓ Total: {len(combined_df)} Events")
    print(f"{'=' * 60}")

    # Create map
    print("\n[2/3] Creating map with legend...\n")

    berlin_map = folium.Map(
        location=[52.5200, 13.4050],
        zoom_start=12,
        prefer_canvas=True,
        tiles="OpenStreetMap",
    )

    # Custom grey cluster icon
    grey_cluster_icon = """
    function(cluster) {
        var childCount = cluster.getChildCount();
        var size = 30;
        if (childCount > 10) size = 40;
        if (childCount > 50) size = 50;
        
        return L.divIcon({
            html: '<div style="background-color: rgba(110, 110, 110, 0.8); color: white; border-radius: 50%; width: ' + size + 'px; height: ' + size + 'px; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 12px; border: 2px solid rgba(80, 80, 80, 0.9);">' + childCount + '</div>',
            className: 'marker-cluster-grey',
            iconSize: L.point(size, size)
        });
    }
    """

    # Create separate clusters for each source
    clusters: dict[str, MarkerCluster] = {}
    for source in CSV_SOURCES:
        if source.name in source_stats:
            clusters[source.name] = MarkerCluster(
                name=source.name,
                max_cluster_radius=40,
                chunked_loading=True,
                spiderfyOnMaxZoom=True,
                showCoverageOnHover=False,
                animate=False,
                icon_create_function=grey_cluster_icon,
                
            ).add_to(berlin_map)

    # Add markers
    for _, row in combined_df.iterrows():
        popup_html = build_popup_html(row)
        
        # Get tooltip text
        tooltip_text = str(
            get_event_field(row, "title", "Event name", "name") or "Event"
        )
        
        marker = folium.Marker(
            location=[row["lat"], row["lon"]],
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=tooltip_text,
            icon=folium.Icon(color=row["color"], icon=row["icon"]),
        )
        
        marker.add_to(clusters[row["source"]])

    # Add layer control
    folium.LayerControl(position="topright").add_to(berlin_map)

    # Add legend
    legend_html = build_legend_html(len(combined_df), source_stats)
    berlin_map.get_root().html.add_child(folium.Element(legend_html))

    # Save map
    print("\n[3/3] Saving map...\n")

    output_path = Path(run_folder)
    map_file = output_path / "index.html"
    berlin_map.save(str(map_file))

    # Also copy to root for GitHub Pages
    root_index = Path(".") / "index.html"
    berlin_map.save(str(root_index))
    
    print(f"✓ Map saved to: {map_file}")

    return berlin_map


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    combine_maps_with_legend(folder)
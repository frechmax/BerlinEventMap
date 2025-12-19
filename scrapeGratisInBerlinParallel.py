"""
scrapeGratisInBerlinParallel.py - gratis-in-berlin.de Event Scraper

Scrapes free events from gratis-in-berlin.de using parallel processing
for detail pages, geocodes venues, and saves results to CSV.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# Constants
MAX_WORKERS = 10
GEOCODE_DELAY_SECONDS = 1.5
REQUEST_TIMEOUT = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

HEADERS = {"User-Agent": USER_AGENT}


@dataclass
class EventDetail:
    """Represents a detailed event from gratis-in-berlin.de."""
    title: str
    url: str
    address: Optional[str] = None
    description: Optional[str] = None
    detailed_date: Optional[str] = None
    success: bool = False
    error: Optional[str] = None


class GratisBerlinScraper:
    """Scraper for gratis-in-berlin.de events."""
    
    def __init__(self):
        self._geolocator: Optional[Nominatim] = None
    
    @property
    def geolocator(self) -> Nominatim:
        """Lazy-loaded geolocator instance."""
        if self._geolocator is None:
            self._geolocator = Nominatim(user_agent="berlin-gratis-events-map")
        return self._geolocator
    
    @staticmethod
    def _scrape_event_detail(event_data: dict[str, str]) -> EventDetail:
        """
        Scrape details for a single event page.
        
        Args:
            event_data: Dictionary with 'title' and 'url' keys.
            
        Returns:
            EventDetail with scraped data.
        """
        try:
            response = requests.get(
                event_data["url"], headers=HEADERS, timeout=REQUEST_TIMEOUT
            )
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract address
            address_text = None
            map_div = soup.find("div", class_="mapTipp")
            if map_div:
                address_text = map_div.get_text(strip=True).split("-")[0].strip()
            
            # Extract description
            description = None
            desc_div = soup.find("div", class_="overview-text")
            if desc_div:
                description = desc_div.get_text(separator=" ", strip=True)[:500]
            
            # Extract detailed date
            detailed_date = None
            date_div = soup.find("div", class_="dateTipp")
            if date_div:
                detailed_date = date_div.get_text(separator=" ", strip=True)
            
            if address_text:
                return EventDetail(
                    title=event_data["title"],
                    url=event_data["url"],
                    address=address_text,
                    description=description,
                    detailed_date=detailed_date,
                    success=True,
                )
            
            return EventDetail(
                title=event_data["title"],
                url=event_data["url"],
                success=False,
            )
            
        except requests.exceptions.RequestException as e:
            return EventDetail(
                title=event_data["title"],
                url=event_data["url"],
                success=False,
                error=str(e),
            )
        except Exception as e:
            return EventDetail(
                title=event_data["title"],
                url=event_data["url"],
                success=False,
                error=str(e),
            )
    
    def scrape_events(self) -> list[EventDetail]:
        """
        Scrape all events from gratis-in-berlin.de using parallel processing.
        
        Returns:
            List of successfully scraped event details.
        """
        url = "https://www.gratis-in-berlin.de/heute"
        
        print("\n[1/4] Loading main page...")
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(response.content, "html.parser")
        
        event_items = soup.find_all("h2", class_="overviewcontentheading")
        print(f"✓ {len(event_items)} events found\n")
        
        # Collect URLs
        event_urls: list[dict[str, str]] = []
        for item in event_items:
            link_tag = item.find("a", class_="singletip")
            if link_tag:
                title = link_tag.get_text(strip=True)
                event_url = f"https://www.gratis-in-berlin.de{link_tag['href']}"
                event_urls.append({"title": title, "url": event_url})
        
        # Parallel processing
        print("[2/4] Scraping event details in parallel...")
        print(f"(Using {MAX_WORKERS} parallel workers)\n")
        
        events: list[EventDetail] = []
        completed = 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_event = {
                executor.submit(self._scrape_event_detail, event): event
                for event in event_urls
            }
            
            for future in as_completed(future_to_event):
                result = future.result()
                completed += 1
                
                if result.success:
                    events.append(result)
                    print(f"  [{completed}/{len(event_urls)}] ✓ {result.title}")
                else:
                    print(f"  [{completed}/{len(event_urls)}] ✗ {result.title}")
        
        print(f"\n{'=' * 60}")
        print(f"✓ {len(events)}/{len(event_urls)} events successfully scraped")
        print(f"{'=' * 60}")
        
        return events
    
    def geocode_address(self, address: str) -> tuple[Optional[float], Optional[float]]:
        """
        Geocode an address to coordinates.
        
        Args:
            address: Address string to geocode.
            
        Returns:
            Tuple of (latitude, longitude) or (None, None) if geocoding fails.
        """
        try:
            location = self.geolocator.geocode(address)
            if location:
                return location.latitude, location.longitude
            return None, None
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"  Geocoding error: {e}")
            return None, None
        except Exception as e:
            print(f"  Unexpected error: {e}")
            return None, None


def run_gratis_berlin_scraper(output_folder: str = ".") -> pd.DataFrame:
    """
    Main function to scrape gratis-in-berlin.de and save results.
    
    Args:
        output_folder: Directory to save output CSV.
        
    Returns:
        DataFrame with geocoded events.
    """
    print("=" * 60)
    print("GRATIS-IN-BERLIN.DE PARALLEL SCRAPER")
    print("=" * 60)
    
    scraper = GratisBerlinScraper()
    events = scraper.scrape_events()
    
    if not events:
        print("\n✗ No events found!")
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame([
        {
            "title": e.title,
            "url": e.url,
            "address": e.address,
            "description": e.description,
            "detailed_date": e.detailed_date,
        }
        for e in events
    ])
    
    # Geocoding
    print("\n[3/4] Geocoding addresses...")
    print("(Sequential processing due to rate limits)\n")
    
    for idx, row in df.iterrows():
        print(f"  [{idx + 1}/{len(df)}] {row['address']}", end=" ")
        lat, lon = scraper.geocode_address(row["address"])
        df.at[idx, "lat"] = lat
        df.at[idx, "lon"] = lon
        
        if lat:
            print("✓")
        else:
            print("✗")
        
        sleep(GEOCODE_DELAY_SECONDS)
    
    df_mapped = df.dropna(subset=["lat", "lon"])
    
    print(f"\n{'=' * 60}")
    print(f"✓ {len(df_mapped)}/{len(df)} events with coordinates")
    print(f"{'=' * 60}")
    
    # Save CSV
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / "gratis_berlin_events.csv"
    df_mapped.to_csv(output_file, index=False)
    print(f"\n✓ CSV saved: {output_file}")
    
    print(f"\n{'=' * 60}")
    print("✓✓✓ DONE! ✓✓✓")
    print(f"{'=' * 60}")
    print(f"\n⚡ Parallel processing made scraping ~{MAX_WORKERS}x faster!")
    
    return df_mapped


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    run_gratis_berlin_scraper(folder)

"""
scrapeVisitBerlin.py - visitBerlin.de Event Scraper

Scrapes daily event listings from visitberlin.de, geocodes the venues,
and saves results to CSV.
"""

import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# Constants
GEOCODE_DELAY_SECONDS = 1.5
REQUEST_TIMEOUT = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"


@dataclass
class VisitBerlinEvent:
    """Represents an event from visitberlin.de."""
    title: str
    date: str = "N/A"
    time: str = "N/A"
    address: str = "N/A"
    description: str = ""
    url: str = "N/A"


class VisitBerlinScraper:
    """Scraper for visitberlin.de events."""
    
    def __init__(self):
        self._geolocator: Optional[Nominatim] = None
        self.headers = {"User-Agent": USER_AGENT}
    
    @property
    def geolocator(self) -> Nominatim:
        """Lazy-loaded geolocator instance."""
        if self._geolocator is None:
            self._geolocator = Nominatim(user_agent="berlin-visit-events-map")
        return self._geolocator
    
    def scrape_events(self, date: Optional[str] = None) -> list[VisitBerlinEvent]:
        """
        Scrape events from visitberlin.de for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format. Defaults to today.
            
        Returns:
            List of scraped events.
        """
        target_date = date or datetime.now().strftime("%Y-%m-%d")
        url = (
            f"https://www.visitberlin.de/de/tagestipps-veranstaltungen-berlin"
            f"?keys=&date_between[min]={target_date}&date_between[max]={target_date}"
            f"&district=All&items_per_page=max"
        )

        print(f"Fetching URL: {url}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        articles = soup.find_all("article", class_="teaser-search--event")
        print(f"{len(articles)} events found.")

        events: list[VisitBerlinEvent] = []

        for article in articles:
            event = self._parse_article(article)
            events.append(event)

        return events
    
    def _parse_article(self, article: BeautifulSoup) -> VisitBerlinEvent:
        """Parse a single article element into an event."""
        # Title
        title_tag = article.find("h2", class_="teaser-search__heading")
        title = title_tag.get_text(strip=True) if title_tag else "N/A"

        # Date
        time_tag = article.find("time")
        date = time_tag.get_text(strip=True) if time_tag else "N/A"

        # Time
        time_range_tag = article.find("p", class_="teaser-search__time")
        time_str = "N/A"
        if time_range_tag:
            content = time_range_tag.find("span", class_="me__content")
            time_str = content.get_text(strip=True) if content else "N/A"

        # Address
        loc_tag = article.find("p", class_="teaser-search__location")
        address = "N/A"
        if loc_tag:
            nopr = loc_tag.find("span", class_="nopr")
            if nopr:
                address = nopr.get_text(strip=True)
            else:
                content = loc_tag.find("span", class_="me__content")
                address = content.get_text(strip=True) if content else "N/A"

        # Description
        desc_tag = article.find("div", class_="teaser-search__text")
        description = ""
        if desc_tag and desc_tag.div:
            description = desc_tag.div.get_text(strip=True)[:500]

        # URL
        link_tag = article.find("a", class_="teaser-search__mainlink")
        url = "N/A"
        if link_tag and link_tag.get("href"):
            link = link_tag["href"]
            if link.startswith("/"):
                link = f"https://www.visitberlin.de{link}"
            url = link

        return VisitBerlinEvent(
            title=title,
            date=date,
            time=time_str,
            address=address,
            description=description,
            url=url,
        )
    
    def geocode_address(self, address: str) -> tuple[Optional[float], Optional[float]]:
        """
        Geocode an address with fallback logic.
        
        Args:
            address: Address string to geocode.
            
        Returns:
            Tuple of (latitude, longitude) or (None, None) if geocoding fails.
        """
        try:
            # Try full address first
            location = self.geolocator.geocode(f"{address}, Berlin, Germany")
            if location:
                return location.latitude, location.longitude
            
            # Try extracting main venue name (before -, –, or :)
            for sep in ["-", "–", ":"]:
                if sep in address:
                    main_part = address.split(sep)[0].strip()
                    if main_part != address:
                        location = self.geolocator.geocode(f"{main_part}, Berlin, Germany")
                        if location:
                            return location.latitude, location.longitude
            
            # Try extracting venue name before /
            if "/" in address:
                main_part = address.split("/")[0].strip()
                location = self.geolocator.geocode(f"{main_part}, Berlin, Germany")
                if location:
                    return location.latitude, location.longitude
            
            return None, None
            
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"  Geocoding error: {e}")
            return None, None
        except Exception as e:
            print(f"  Unexpected error: {e}")
            return None, None


def run_visitberlin_scraper(output_folder: str = ".") -> pd.DataFrame:
    """
    Main function to scrape visitberlin.de and save results.
    
    Args:
        output_folder: Directory to save output CSV.
        
    Returns:
        DataFrame with geocoded events.
    """
    print("=" * 60)
    print("VISITBERLIN.DE EVENT SCRAPER")
    print("=" * 60)

    scraper = VisitBerlinScraper()
    events = scraper.scrape_events()

    print(f"\n{len(events)} events found")

    if not events:
        print("\n⚠ No events found!")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame([
        {
            "title": e.title,
            "date": e.date,
            "time": e.time,
            "address": e.address,
            "description": e.description,
            "url": e.url,
        }
        for e in events
    ])

    # Geocoding
    print("\n[2/3] Geocoding addresses...")
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
    output_file = output_path / "visitberlin_events.csv"
    df_mapped.to_csv(output_file, index=False)
    print(f"\n✓ CSV saved: {output_file}")

    print(f"\n{'=' * 60}")
    print("✓✓✓ DONE! ✓✓✓")
    print(f"{'=' * 60}")
    
    return df_mapped


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    run_visitberlin_scraper(folder)

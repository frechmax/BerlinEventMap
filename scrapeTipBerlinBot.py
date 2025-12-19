"""
scrapeTipBerlinBot.py - tip-berlin.de Event Scraper

Scrapes daily event highlights from tip-berlin.de using Playwright for
JavaScript-rendered content, then geocodes venues and saves to CSV.
"""

import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Optional

import pandas as pd
import pytz
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from playwright.sync_api import sync_playwright, Page, Browser

# Constants
GEOCODE_DELAY_SECONDS = 1.5
BERLIN_TIMEZONE = pytz.timezone("Europe/Berlin")


@dataclass
class Event:
    """Represents a scraped event."""
    title: str
    category: Optional[str]
    venue: Optional[str]
    address: Optional[str]
    date: Optional[str]
    url: Optional[str]


class TipBerlinScraper:
    """Scraper for tip-berlin.de events."""
    
    def __init__(self):
        self._geolocator: Optional[Nominatim] = None
    
    @property
    def geolocator(self) -> Nominatim:
        """Lazy-loaded geolocator instance."""
        if self._geolocator is None:
            self._geolocator = Nominatim(user_agent="tip-berlin-events-map")
        return self._geolocator
    
    def _handle_cookie_banner(self, page: Page) -> None:
        """Attempt to close cookie consent banner."""
        cookie_selectors = [
            'button:has-text("Akzeptieren")',
            'button:has-text("Alle akzeptieren")',
            'button:has-text("Accept")',
            '[title="Akzeptieren"]',
            '.sp_choice_type_11',
            'button[title="Zustimmen"]',
        ]
        
        for selector in cookie_selectors:
            try:
                cookie_btn = page.locator(selector).first
                if cookie_btn.is_visible(timeout=2000):
                    cookie_btn.click(timeout=5000)
                    print(f"  ✓ Cookie banner closed with: {selector}")
                    page.wait_for_timeout(1000)
                    return
            except Exception:
                continue
        
        print("  ⚠ Cookie banner not found or already closed")
    
    def _handle_popup(self, page: Page) -> None:
        """Attempt to close any advertising popups."""
        close_selectors = [
            'button:has-text("Schließen")',
            'button:has-text("×")',
            '[aria-label="Close"]',
            '.close',
            '.modal-close',
        ]
        
        for selector in close_selectors:
            try:
                close_btn = page.locator(selector).first
                if close_btn.is_visible(timeout=1000):
                    close_btn.click(timeout=3000)
                    print("  ✓ Popup closed")
                    page.wait_for_timeout(1000)
                    return
            except Exception:
                continue
    
    def _navigate_to_events(self, page: Page) -> None:
        """Navigate to the events listing page."""
        try:
            mehr_button = page.locator("a.tip-recommended-posts__more-link").first
            mehr_button.click(timeout=10000)
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(3000)
            print(f"✓ Navigated to: {page.url}")
        except Exception as e:
            print(f"✗ Click failed: {e}")
            print("Trying direct navigation...")
            page.goto("https://www.tip-berlin.de/event/", wait_until="networkidle")
            page.wait_for_timeout(3000)
    
    def _parse_events_from_html(self, html: str) -> list[Event]:
        """Parse event data from HTML content."""
        soup = BeautifulSoup(html, "html.parser")
        event_boxes = soup.find_all("div", class_="collections__box--event")
        
        print(f"\nFound event boxes: {len(event_boxes)}")
        
        if not event_boxes:
            event_boxes = soup.find_all("div", class_="collections__box")
            print(f"Alternative event boxes: {len(event_boxes)}")
        
        events: list[Event] = []
        
        for box in event_boxes:
            try:
                title_tag = box.find("h2", class_="collections__box__title")
                title = title_tag.get_text(strip=True) if title_tag else None
                
                if not title:
                    continue
                
                link_tag = box.find("a", class_="collections__box__link")
                event_url = link_tag["href"] if link_tag else None
                
                category_tag = box.find("p", class_="collections__box__event-category")
                category = category_tag.get_text(strip=True) if category_tag else None
                
                address_span = box.find("span", class_="-desktop-v")
                address = address_span.get_text(strip=True) if address_span else None
                
                venue_span = box.find("span", class_="-mobile-v")
                venue = venue_span.get_text(strip=True) if venue_span else None
                
                date_tags = box.find_all("h3", class_="collections__box__title")
                date = date_tags[-1].get_text(strip=True) if date_tags else None
                
                if address or venue:
                    events.append(Event(
                        title=title,
                        category=category,
                        venue=venue,
                        address=address,
                        date=date,
                        url=event_url,
                    ))
                    print(f"✓ {title}")
                    
            except (KeyError, AttributeError) as e:
                print(f"✗ Failed to parse event: {e}")
                continue
        
        return events
    
    def scrape_events(self) -> list[Event]:
        """Scrape events from tip-berlin.de."""
        events: list[Event] = []
        
        with sync_playwright() as p:
            print("Starting browser...")
            browser: Browser = p.chromium.launch(headless=True)
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            
            page = context.new_page()
            page.route("**/*", lambda route: route.continue_())
            
            # Navigate to daily highlights
            today = datetime.now(BERLIN_TIMEZONE).strftime("%Y-%m-%d")
            print(f"  Today (Berlin time): {today}")
            
            url = f"https://www.tip-berlin.de/event-tageshighlights/?t={int(datetime.now().timestamp())}"
            print("Opening daily highlights page...")
            page.goto(url, wait_until="networkidle")
            page.wait_for_timeout(3000)
            
            # Handle overlays
            print("Closing cookie banner...")
            self._handle_cookie_banner(page)
            
            print("Checking for ad popups...")
            self._handle_popup(page)
            
            page.wait_for_timeout(2000)
            
            # Navigate to full events list
            print("Clicking 'More' button...")
            self._navigate_to_events(page)
            
            # Parse the page
            html = page.content()
            browser.close()
        
        events = self._parse_events_from_html(html)
        return events
    
    def geocode_location(self, text: str, city: str = "Berlin") -> tuple[Optional[float], Optional[float]]:
        """
        Geocode an address or venue name to coordinates.
        
        Args:
            text: Address or venue name to geocode.
            city: City name for context.
            
        Returns:
            Tuple of (latitude, longitude) or (None, None) if geocoding fails.
        """
        try:
            # If text contains digits, assume it's an address
            if any(char.isdigit() for char in text):
                search_query = text
            else:
                search_query = f"{text}, {city}, Germany"
            
            location = self.geolocator.geocode(search_query)
            if location:
                return location.latitude, location.longitude
            return None, None
            
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"  Geocoding error: {e}")
            return None, None
        except Exception as e:
            print(f"  Unexpected geocoding error: {e}")
            return None, None

def run_tip_berlin_scraper(output_folder: str = ".") -> pd.DataFrame:
    """
    Main function to scrape tip-berlin.de and save results.
    
    Args:
        output_folder: Directory to save the output CSV.
        
    Returns:
        DataFrame with geocoded events.
    """
    print("=" * 60)
    print("TIP-BERLIN.DE EVENT SCRAPER")
    print("=" * 60)

    scraper = TipBerlinScraper()
    events = scraper.scrape_events()
    
    print(f"\n{len(events)} events found")

    if not events:
        print("\n⚠ No events found!")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame([
        {
            "title": e.title,
            "category": e.category,
            "venue": e.venue,
            "address": e.address,
            "date": e.date,
            "url": e.url,
        }
        for e in events
    ])

    # Geocoding
    print("\n" + "=" * 60)
    print("GEOCODING")
    print("=" * 60)

    for idx, row in df.iterrows():
        location_text = row.get("address") or row.get("venue")
        
        if location_text:
            print(f"[{idx + 1}/{len(df)}] {location_text[:40]}", end=" ")
            lat, lon = scraper.geocode_location(location_text)
            df.at[idx, "lat"] = lat
            df.at[idx, "lon"] = lon
            
            if lat:
                print("✓")
            else:
                print("✗")
            
            sleep(GEOCODE_DELAY_SECONDS)

    df_mapped = df.dropna(subset=["lat", "lon"])
    print(f"\n✓ {len(df_mapped)}/{len(df)} events with coordinates")
    
    # Save CSV
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / "tip_berlin_events.csv"
    df_mapped.to_csv(output_file, index=False, encoding="utf-8")
    print(f"✓ CSV saved: {output_file}")
    
    return df_mapped


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    run_tip_berlin_scraper(folder)

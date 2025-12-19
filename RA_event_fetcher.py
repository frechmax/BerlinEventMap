"""
RA_event_fetcher.py - Resident Advisor Event Fetcher

Fetches event listings from ra.co using their GraphQL API and saves them to CSV
with geocoded venue coordinates.
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import requests
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim

# Constants
URL = "https://ra.co/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Referer": "https://ra.co/events/uk/london",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}
QUERY_TEMPLATE_PATH = Path("graphql_query_template.json")
API_DELAY_SECONDS = 1
GEOCODE_DELAY_SECONDS = 1.5


class EventFetcher:
    """Fetches and processes event details from RA.co."""

    def __init__(self, area_code: int, listing_date_gte: str, listing_date_lte: str):
        """
        Initialize the EventFetcher.

        Args:
            area_code: The area code to filter events (e.g., 34 for Berlin).
            listing_date_gte: Start date for event listings (ISO format).
            listing_date_lte: End date for event listings (ISO format).
        """
        self.payload = self._generate_payload(area_code, listing_date_gte, listing_date_lte)
        self.geolocator = Nominatim(user_agent="ra-events-geocoder")
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=GEOCODE_DELAY_SECONDS)
        self._venues_cache: dict[str, tuple[Optional[float], Optional[float]]] = {}

    @staticmethod
    def _generate_payload(area_code: int, listing_date_gte: str, listing_date_lte: str) -> dict[str, Any]:
        """
        Generate the payload for the GraphQL request.

        Args:
            area_code: The area code to filter events.
            listing_date_gte: The start date for event listings (inclusive).
            listing_date_lte: The end date for event listings (inclusive).

        Returns:
            The generated payload dictionary.

        Raises:
            FileNotFoundError: If the query template file doesn't exist.
        """
        with QUERY_TEMPLATE_PATH.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        payload["variables"]["filters"]["areas"]["eq"] = area_code
        payload["variables"]["filters"]["listingDate"]["gte"] = listing_date_gte
        payload["variables"]["filters"]["listingDate"]["lte"] = listing_date_lte

        return payload

    def get_events(self, page_number: int) -> list[dict[str, Any]]:
        """
        Fetch events for the given page number.

        Args:
            page_number: The page number for event listings.

        Returns:
            A list of event dictionaries.
        """
        self.payload["variables"]["page"] = page_number

        try:
            response = requests.post(URL, headers=HEADERS, json=self.payload, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.Timeout:
            print(f"Error: Request timed out for page {page_number}")
            return []
        except requests.exceptions.RequestException as e:
            print(f"Error: Request failed - {e}")
            return []
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON response for page {page_number}")
            return []

        if "data" not in data:
            print(f"Error: Unexpected response structure - {data}")
            return []

        return data["data"]["eventListings"]["data"]

    def fetch_all_events(self) -> list[dict[str, Any]]:
        """
        Fetch all events across all pages.

        Returns:
            A list of all event dictionaries.
        """
        all_events: list[dict[str, Any]] = []
        page_number = 1

        while True:
            events = self.get_events(page_number)
            if not events:
                break

            all_events.extend(events)
            page_number += 1
            time.sleep(API_DELAY_SECONDS)

        return all_events

    def _geocode_venue(
        self, venue_name: str, venue_address: Optional[str] = None
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Geocode a venue and return latitude and longitude.

        Args:
            venue_name: The name of the venue.
            venue_address: The address of the venue (optional).

        Returns:
            Tuple of (latitude, longitude) or (None, None) if geocoding fails.
        """
        # Check cache first
        cache_key = f"{venue_name}|{venue_address or ''}"
        if cache_key in self._venues_cache:
            return self._venues_cache[cache_key]

        try:
            query = f"{venue_name}, {venue_address}, Berlin, Germany" if venue_address else f"{venue_name}, Berlin, Germany"
            location = self.geocode(query)

            if location:
                result = (location.latitude, location.longitude)
            else:
                result = (None, None)
        except Exception as e:
            print(f"Warning: Geocoding failed for {venue_name}: {str(e)[:50]}")
            result = (None, None)

        self._venues_cache[cache_key] = result
        return result

    def save_events_to_csv(self, events: list[dict[str, Any]], output_file: str = "events.csv") -> None:
        """
        Save events to a CSV file with venue coordinates.

        Args:
            events: A list of event dictionaries.
            output_file: The output file path.
        """
        if not events:
            print("No events to save.")
            return

        print(f"\nGeocoding {len(events)} venues...")

        # Pre-geocode all unique venues
        unique_venues = {
            (event["event"]["venue"]["name"], event["event"]["venue"].get("address", ""))
            for event in events
        }

        for venue_name, venue_address in unique_venues:
            if f"{venue_name}|{venue_address}" not in self._venues_cache:
                print(f"  Geocoding: {venue_name[:50]:<50}", end=" ", flush=True)
                lat, lon = self._geocode_venue(venue_name, venue_address)
                if lat is not None:
                    print(f"✓ ({lat:.4f}, {lon:.4f})")
                else:
                    print("✗")

        # Write CSV
        output_path = Path(output_file)
        with output_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([
                "Event name", "Date", "Start Time", "End Time", "Artists",
                "Venue", "Venue Address", "Venue Latitude", "Venue Longitude",
                "Event URL", "Number of guests attending"
            ])

            for event in events:
                event_data = event["event"]
                venue_name = event_data["venue"]["name"]
                venue_address = event_data["venue"].get("address", "")
                lat, lon = self._venues_cache.get(f"{venue_name}|{venue_address}", (None, None))

                writer.writerow([
                    event_data["title"],
                    event_data["date"],
                    event_data["startTime"],
                    event_data["endTime"],
                    ", ".join(artist["name"] for artist in event_data["artists"]),
                    venue_name,
                    venue_address,
                    lat if lat is not None else "",
                    lon if lon is not None else "",
                    event_data["contentUrl"],
                    event_data["attending"],
                ])

        print(f"\n✓ Saved {len(events)} events to {output_file}")

    @staticmethod
    def print_event_details(events: list[dict[str, Any]]) -> None:
        """
        Print the details of events to console.

        Args:
            events: A list of event dictionaries.
        """
        for event in events:
            event_data = event["event"]
            print(f"Event name: {event_data['title']}")
            print(f"Date: {event_data['date']}")
            print(f"Start Time: {event_data['startTime']}")
            print(f"End Time: {event_data['endTime']}")
            print(f"Artists: {[artist['name'] for artist in event_data['artists']]}")
            print(f"Venue: {event_data['venue']['name']}")
            print(f"Event URL: {event_data['contentUrl']}")
            print(f"Number of guests attending: {event_data['attending']}")
            print("-" * 80)


def main() -> int:
    """
    Main entry point for the RA event fetcher.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    parser = argparse.ArgumentParser(
        description="Fetch events from ra.co and save them to a CSV file."
    )
    parser.add_argument(
        "areas", type=int, help="The area code to filter events (e.g., 34 for Berlin)."
    )
    parser.add_argument(
        "start_date",
        type=str,
        help="The start date for event listings (format: YYYY-MM-DD).",
    )
    parser.add_argument(
        "end_date",
        type=str,
        help="The end date for event listings (format: YYYY-MM-DD).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="events.csv",
        help="The output file path (default: events.csv).",
    )
    args = parser.parse_args()

    # Validate dates
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as e:
        print(f"Error: Invalid date format - {e}")
        return 1

    if start_date > end_date:
        print("Error: Start date must be before or equal to end date.")
        return 1

    listing_date_gte = f"{args.start_date}T00:00:00.000Z"
    listing_date_lte = f"{args.end_date}T23:59:59.999Z"

    try:
        event_fetcher = EventFetcher(args.areas, listing_date_gte, listing_date_lte)
        events = event_fetcher.fetch_all_events()

        if not events:
            print("No events found for the specified criteria.")
            return 0

        print(f"Found {len(events)} events.")
        event_fetcher.save_events_to_csv(events, args.output)
        return 0

    except FileNotFoundError:
        print(f"Error: Query template file not found: {QUERY_TEMPLATE_PATH}")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
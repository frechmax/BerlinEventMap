from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # headless=False zum Sehen
    page = browser.new_page()
    
    print("Lade Seite...")
    page.goto("https://www.tip-berlin.de/event-tageshighlights/", wait_until="networkidle")
    page.wait_for_timeout(5000)
    
    html = page.content()
    
    # HTML in Datei speichern zum Analysieren
    with open('debug_page.html', 'w', encoding='utf-8') as f:
        f.write(html)
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Verschiedene Selektoren testen
    print("\n--- DEBUG INFO ---")
    print(f"1. collections__box--event: {len(soup.find_all('div', class_='collections__box--event'))}")
    print(f"2. collections__box: {len(soup.find_all('div', class_='collections__box'))}")
    print(f"3. Alle divs mit 'collections': {len(soup.find_all('div', class_=lambda x: x and 'collections' in x))}")
    
    # Ersten Event-Container ausgeben
    first_event = soup.find('div', class_='collections__box')
    if first_event:
        print("\n--- ERSTER EVENT (HTML) ---")
        print(first_event.prettify()[:1000])
    else:
        print("\nKeine Events gefunden!")
    
    # Prüfe, ob Container überhaupt existiert
    container = soup.find('div', id='collections-container-today')
    if container:
        print("\n✓ Container gefunden")
        print(f"Container hat {len(container.find_all('div'))} divs")
    else:
        print("\n✗ Container NICHT gefunden")
    
    input("Drücke Enter zum Schließen...")
    browser.close()

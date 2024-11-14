# scraper.py

import aiohttp
import asyncio
import sqlite3
import re
import json
from bs4 import BeautifulSoup
import csv
import os
import time

# Sample 20 property URLs for testing
sample_urls = [
    "https://www.immoweb.be/en/classified/villa/for-sale/londerzeel/1840/20311104",
    "https://www.immoweb.be/en/classified/apartment/for-sale/-/1150/20066882",
    "https://www.immoweb.be/en/classified/house/for-sale/st-marcel-de-careiret/30330/20283692",
    "https://www.immoweb.be/en/classified/house/for-sale/longlaville/54810/10725071",
    "https://www.immoweb.be/en/classified/house/for-sale/longueville/1325/20246093",
    "https://www.immoweb.be/en/classified/house/for-sale/schaerbeek/1030/20273974",
    "https://www.immoweb.be/en/classified/apartment/for-sale/gent/9000/20266471"
    # Add 15 more URLs here for testing
]
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.immoweb.be/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }
semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent fetches

async def fetch_sitemap(url="https://www.immoweb.be/sitemap.xml"):
    """Fetches the main sitemap and returns URLs containing 'classifieds'."""
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.text()
                soup = BeautifulSoup(content, 'xml')
                # Filter URLs to keep only 'classifieds' files
                urls = [loc.text for loc in soup.find_all('loc') if "classifieds" in loc.text]
                return urls
            else:
                print("Failed to fetch the sitemap.")
                return []

async def fetch_property_urls(session, classified_url, unique_urls):
    """Fetches and filters property URLs from a given classified XML file asynchronously."""
    async with semaphore:  # Only 10 fetch_property_urls tasks will run at a time
        async with session.get(classified_url) as response:
            if response.status == 200:
                content = await response.text()
                soup = BeautifulSoup(content, 'xml')
                for loc in soup.find_all('loc'):
                    url = loc.text
                    # Check for valid English "for-sale" classified URLs
                    if "en/classified" in url and "for-sale" in url:
                        # Ensure we're only adding complete URLs to avoid isolated property IDs
                        if url.startswith("https://www.immoweb.be/en/classified"):
                            unique_urls.add(url)
            else:
                print(f"Failed to fetch classified URL: {classified_url}")

def setup_database():
    conn = sqlite3.connect("properties.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS properties (
            property_id TEXT PRIMARY KEY,
            url TEXT,
            locality TEXT,
            postal TEXT,
            address TEXT,
            region TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            price INTEGER,
            sale_type TEXT,
            property_type TEXT,
            number_of_bedrooms INTEGER,
            living_area REAL,
            basement BOOLEAN,
            open_fire BOOLEAN,
            terrace BOOLEAN,
            terrace_area REAL,
            terrace_orientation TEXT,
            garden BOOLEAN,
            garden_area REAL,
            garden_orientation TEXT,
            number_of_facades INTEGER,
            construction_year INTEGER,
            state_of_building TEXT,
            swimming_pool BOOLEAN,
            epc TEXT,
            kwh REAL,
            last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

async def fetch_property_data(session, url):
    """Fetch property data from JSON embedded in the HTML page."""
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            content = await response.text()
            soup = BeautifulSoup(content, 'html.parser')
            script_tag = soup.find("script", string=re.compile("window.classified"))
            if script_tag:
                json_text = re.search(r"window.classified = ({.*?});", script_tag.string).group(1)
                data = json.loads(json_text)
                return extract_data(data, url)
        return None

def extract_data(data, url):
    """Extract key details from the property JSON data."""

    # Ensure the property is for sale
    if (
        data.get("transaction", {}).get("type") != "FOR_SALE" or
        data.get("transaction", {}).get("subtype") == "LIFE_ANNUITY" or
        data.get("property", {}).get("location", {}).get("country") not in ["Belgium", "BELGIUM"]
    ):
        print(f"Skipping property at {url} due to sale type, subtype, or country restrictions.")
        return None

    # Begin extracting required fields
    property_id = data.get("id")
    locality = data.get("property", {}).get("location", {}).get("locality")
    postal = data.get("property", {}).get("location", {}).get("postalCode")
    address = data.get("property", {}).get("location", {}).get("street")
    region = data.get("property", {}).get("location", {}).get("region")
    country = data.get("property", {}).get("location", {}).get("country")
    latitude = data.get("property", {}).get("location", {}).get("latitude")
    longitude = data.get("property", {}).get("location", {}).get("longitude")
    price_value = data.get("price", {}).get("mainValue", None)
    price = int(price_value) if price_value is not None else None
    sale_type = data.get("price", {}).get("type")
    property_type = data.get("property", {}).get("type")
    number_of_bedrooms = data.get("property", {}).get("bedroomCount")
    living_area = data.get("property", {}).get("netHabitableSurface")
    basement = data.get("property", {}).get("hasBasement")
    open_fire = data.get("property", {}).get("fireplaceExists")
    terrace = data.get("property", {}).get("hasTerrace")
    terrace_area = data.get("property", {}).get("terraceSurface")
    terrace_orientation = data.get("property", {}).get("terraceOrientation")
    garden = data.get("property", {}).get("hasGarden")
    garden_area = data.get("property", {}).get("gardenSurface")
    garden_orientation = data.get("property", {}).get("gardenOrientation")

    # Building details with safe access
    building = data.get("property", {}).get("building")
    if building is not None:
        number_of_facades = building.get("facadeCount")
        construction_year = building.get("constructionYear")
        state_of_building = building.get("condition")
    else:
        number_of_facades = None
        construction_year = None
        state_of_building = None

    # Additional details
    swimming_pool = data.get("property", {}).get("hasSwimmingPool")

    # Certificate details with safe access
    certificates = data.get("transaction", {}).get("certificates")
    if certificates is not None:
        epc = certificates.get("epcScore")
        kwh = certificates.get("primaryEnergyConsumptionPerSqm")
    else:
        epc = None
        kwh = None

    # Return structured data for database storage
    return {
        "property_id": property_id,
        "url": url,
        "locality": locality,
        "postal": postal,
        "address": address,
        "region": region,
        "country": country,
        "latitude": latitude,
        "longitude": longitude,
        "price": price,
        "sale_type": sale_type,
        "property_type": property_type,
        "number_of_bedrooms": number_of_bedrooms,
        "living_area": living_area,
        "basement": basement,
        "open_fire": open_fire,
        "terrace": terrace,
        "terrace_area": terrace_area,
        "terrace_orientation": terrace_orientation,
        "garden": garden,
        "garden_area": garden_area,
        "garden_orientation": garden_orientation,
        "number_of_facades": number_of_facades,
        "construction_year": construction_year,
        "state_of_building": state_of_building,
        "swimming_pool": swimming_pool,
        "epc": epc,
        "kwh": kwh,
    }

def save_to_database(data):
    """Save property data to SQLite database."""
    conn = sqlite3.connect("properties.db")
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO properties (
            property_id, url, locality, postal, address, region, country,
            latitude, longitude, price, sale_type, property_type, 
            number_of_bedrooms, living_area, basement, open_fire, terrace, 
            terrace_area, terrace_orientation, garden, garden_area, 
            garden_orientation, number_of_facades, construction_year, 
            state_of_building, swimming_pool, epc, kwh
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data["property_id"],
        data["url"],
        data["locality"],
        data["postal"],
        data["address"],
        data["region"],
        data["country"],
        data["latitude"],
        data["longitude"],
        data["price"],
        data["sale_type"],
        data["property_type"],
        data["number_of_bedrooms"],
        data["living_area"],
        data["basement"],
        data["open_fire"],
        data["terrace"],
        data["terrace_area"],
        data["terrace_orientation"],
        data["garden"],
        data["garden_area"],
        data["garden_orientation"],
        data["number_of_facades"],
        data["construction_year"],
        data["state_of_building"],
        data["swimming_pool"],
        data["epc"],
        data["kwh"]
    ))
    conn.commit()
    conn.close()

async def main():
    setup_database()

    urls_file = 'data/raw/unique_property_urls.csv'

    if os.path.exists(urls_file):
        file_mod_time = os.path.getmtime(urls_file)
        current_time = time.time()
        age_in_seconds = current_time - file_mod_time
        age_in_hours = age_in_seconds / 3600

        if age_in_hours < 24:
            # File is less than 24 hours old, use it
            print("Loading URLs from existing file.")
            unique_urls = set()
            with open(urls_file, mode='r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                for row in reader:
                    unique_urls.add(row[0])
        else:
            # File is older than 24 hours, re-fetch sitemap and update file
            print("URLs file is older than 24 hours. Updating URLs file.")
            classified_urls = await fetch_sitemap()

            unique_urls = set()

            async with aiohttp.ClientSession(headers=headers) as session:
                tasks = [fetch_property_urls(session, url, unique_urls) for url in classified_urls]
                await asyncio.gather(*tasks)

            print(f"Total unique 'for-sale' property URLs (English): {len(unique_urls)}")

            # Save URLs to a CSV file
            with open(urls_file, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["Property URL"])
                for url in unique_urls:
                    writer.writerow([url])

            print("Updated unique URLs file.")
    else:
        # File does not exist, fetch sitemap and create file
        print("URLs file does not exist. Fetching URLs.")
        classified_urls = await fetch_sitemap()

        unique_urls = set()

        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [fetch_property_urls(session, url, unique_urls) for url in classified_urls]
            await asyncio.gather(*tasks)

        print(f"Total unique 'for-sale' property URLs (English): {len(unique_urls)}")

        # Save URLs to a CSV file
        with open(urls_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Property URL"])
            for url in unique_urls:
                writer.writerow([url])

        print("Saved unique URLs to file.")

    # Proceed with data scraping using `unique_urls`
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch_property_data(session, url) for url in sample_urls]  # Replace `sample_urls` with `unique_urls` when ready
        results = await asyncio.gather(*tasks)

    # Filter out None results (failed scrapes) and save to database
    for data in results:
        if data:
            save_to_database(data)

    print("Data scraping complete, data saved to database.")

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())

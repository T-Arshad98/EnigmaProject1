import pandas as pd
from sodapy import Socrata
import requests
import time
import logging
from config import BASE_URL, APP_TOKEN, CLIENT_ENDPOINT, RETRY_COUNT, RETRY_DELAY

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


client = Socrata(BASE_URL,
                APP_TOKEN,
                "tarshad@ualberta.ca",
                "3dC=&R]9:V4pJx_")


def validate_data(data):
    if not isinstance(data, list):
        return False
    if len(data) > 0 and not isinstance(data[0], dict):
        return False
    return True

def GetDataChunk(offset, limit):
    try:
        response = client.get(CLIENT_ENDPOINT, limit=limit, offset=offset, timeout=10)
        if not validate_data(response):
            logging.error("Invalid data structure received")
            return None
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data chunk: {e}")
        return None

def GetAllData():
    offset = 0
    limit = 1000
    
    while True:
        for attempt in range(RETRY_COUNT):
            data = GetDataChunk(limit=limit, offset=offset)
            
            if data is None:
                if attempt < RETRY_COUNT - 1:
                    logging.info(f"Retrying in {RETRY_DELAY} seconds... (Attempt {attempt + 1}/{RETRY_COUNT})")
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    logging.error("Max retries reached. Exiting.")
                    return
            
            if not data:  # Empty response
                return
            
            logging.info(f"Fetched {len(data)} rows, Offset: {offset}")
            offset += limit
            yield data
            break


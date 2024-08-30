import requests
import json
import sys
import logging
import time
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from dateutil.parser import parse
from requests.exceptions import ConnectionError, Timeout, RequestException
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError, AzureError, ServiceUnavailableError
from tenacity import retry, stop_after_attempt, wait_exponential

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure that the correct number of arguments are provided
if len(sys.argv) != 5:
    print("Usage: python script_name.py <nasa_api_key> <azure_storage_connection_string> <start_date> <container_name>")
    sys.exit(1)

# Fetch credentials and parameters from command-line arguments
nasa_api_key = sys.argv[1]
connect_str = sys.argv[2]

# Validate and parse start date
try:
    start_date = parse(sys.argv[3]).strftime("%Y-%m-%d")
except ValueError:
    logging.critical("The start date provided is not valid. Please use YYYY-MM-DD format.")
    sys.exit(1)

container_name = sys.argv[4]

# Get the current date
current_date = datetime.now().strftime("%Y-%m-%d")

# Retry logic for fetching data from NASA API
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
def fetch_neo_data(url, params):
    response = requests.get(url, params=params, timeout=10)  # Added timeout
    response.raise_for_status()
    return response.json()

# Retry logic for blob uploads
def upload_blob_with_retry(blob_client, content, retries=3):
    for attempt in range(retries):
        try:
            blob_client.upload_blob(content)
            return
        except (ServiceUnavailableError, AzureError) as e:
            logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                logging.critical("Max retries reached, failed to upload blob.")
                raise

def handle_paging(base_url, params):
    next_url = base_url
    while next_url:
        logging.info(f"Fetching data from: {next_url}")
        neo_data = fetch_neo_data(next_url, params)
        yield neo_data
        next_url = neo_data.get("links", {}).get("next")

try:
    # NASA NEO API endpoint and parameters
    nasa_api_url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        "start_date": start_date,
        "end_date": current_date,
        "api_key": nasa_api_key
    }

    # Initialize the BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a container if it doesn't exist
    try:
        container_client = blob_service_client.create_container(container_name)
    except ResourceExistsError:
        logging.warning(f"Container {container_name} already exists.")
        container_client = blob_service_client.get_container_client(container_name)
    except AzureError as e:
        logging.error(f"An Azure error occurred: {str(e)}")
        sys.exit(1)

    # Fetch and process all pages of NEO data
    for neo_data in handle_paging(nasa_api_url, params):
        neos = neo_data.get("near_earth_objects", {})

        for date, neo_list in neos.items():
            for neo in neo_list:
                try:
                    neo_id = neo["id"]
                    neo_filename = f"{neo_id}.json"
                    neo_content = json.dumps(neo, indent=2)
                    
                    # Upload the NEO JSON to Blob Storage with retry logic
                    blob_client = container_client.get_blob_client(neo_filename)
                    upload_blob_with_retry(blob_client, neo_content)
                    logging.info(f"Uploaded {neo_filename} to {container_name}")
                except ResourceNotFoundError:
                    logging.error(f"Failed to upload {neo_filename} because the container or blob was not found.")
                except AzureError as e:
                    logging.error(f"Failed to upload NEO data for {neo_id} to Blob Storage. Azure error: {str(e)}")
    
    logging.info("All NEO data has been uploaded successfully.")

except Exception as e:
    logging.critical(f"An unexpected error occurred during execution: {str(e)}")
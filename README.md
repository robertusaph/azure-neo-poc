# NASA NEO Azure Blob Uploader

This Python script fetches Near-Earth Object (NEO) data from the NASA API and uploads it to Azure Blob Storage. The script handles API paging, retries on network issues, and uploads data securely.

## Features

- Fetches NEO data from NASA API.
- Handles paging automatically to retrieve all data.
- Implements retry logic for robust API requests and Azure Blob uploads.
- Validates input data (e.g., dates) to ensure correctness.

## Requirements

- Python 3.7+
- Azure Storage Blob SDK
- Requests library
- Tenacity library
- Dateutil library

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/nasa-neo-azure-uploader.git
   cd nasa-neo-azure-uploader
    ```

2. Create a virtual environment and activate it:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3. Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

To run the script, use the following command:

    python azure-neo-poc.py <nasa_api_key> <azure_storage_connection_string> <start_date> <container_name>

Example:

    python script_name.py YOUR_NASA_API_KEY YOUR_AZURE_STORAGE_CONNECTION_STRING 2024-08-01 neo-container


## Environment Variables

The script requires the following inputs as command-line arguments:

    `nasa_api_key`: Your NASA API key.
    `azure_storage_connection_string`: Your Azure Storage connection string.
    `start_date`: The start date for fetching NEO data (format: YYYY-MM-DD).
    `container_name`: The name of the Azure Blob Storage container.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

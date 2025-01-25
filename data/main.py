import pandas as pd
import os
import time
import json
import logging
from typing import Optional, Dict, Any
from pathlib import Path

from GenPhish.data.dns_util import get_dns_records_with_geolocation, get_ip_geolocation
from GenPhish.data.whois import whois_lookup
from GenPhish.data.url import get_url_metadata


class DataProcessor:
    def __init__(
        self,
        source_path: str,
        target_path: str,
        limit: int = 100,
        is_random: bool = False,
    ):
        self.source_path = Path(source_path)
        self.target_path = Path(target_path)
        self.limit = limit
        self.is_random = is_random
        self.results = []

        # Configure logging
        self.setup_logging()

    def setup_logging(self) -> None:
        """Configure logging settings"""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("logs/processing.log"),
                logging.StreamHandler(),
            ],
        )

    def read(self) -> Optional[pd.DataFrame]:
        """Read the CSV file and return a DataFrame"""
        try:
            df = pd.read_csv(self.source_path)
            logging.info(f"Successfully read file: {len(df)} rows, {len(df.columns)} columns")
            return df
        except FileNotFoundError:
            logging.error(f"File not found: {self.source_path}")
            return None
        except Exception as e:
            logging.error(f"Error reading file '{self.source_path}': {e}")
            return None

    def process_url(self, url: str, whois_api_key: str, ip_api_key: str) -> Optional[Dict[str, Any]]:
        """Process a single URL and return flattened data"""
        try:
            dns_record = get_dns_records_with_geolocation(url, ip_api_key)
            who_is_record = whois_lookup(whois_api_key, url)
            url_features = get_url_metadata(url)

            # Flatten nested dictionaries
            flattened_record = {
                "url": url,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            # Add URL-related features
            for k, v in url_features.items():
                flattened_record[k] = v

            # Add DNS record details
            for record_type, prefix in [("A", "ipa"), ("MX", "ipmx")]:
                if dns_record.get(record_type):
                    ips = dns_record[record_type]["IPs"][:2]
                    locs = dns_record[record_type]["Geolocations"][:2]
                    for idx, (ip, loc) in enumerate(zip(ips, locs), start=1):
                        flattened_record[f"{prefix}_{idx}"] = ip
                        flattened_record[f"{prefix}_loc_{idx}"] = loc
                        
            # Add WHOIS record details
            for k, v in who_is_record.items():
                flattened_record[f"whois_{k}"] = v

            return flattened_record
        except Exception as e:
            logging.error(f"Error processing URL {url}: {e}")
            return None

    def process_data(self, whois_api_key: str, ip_api_key: str) -> None:
        """Main processing function"""
        try:
            # Read source data
            df = self.read()
            if df is None or df.empty:
                logging.error("No data to process.")
                return

            # Apply limit and random selection if needed
            if len(df) > self.limit:
                if self.is_random:
                    df = df.sample(n=self.limit, random_state=42)
                    logging.info(f"Randomly selected {self.limit} rows")
                else:
                    df = df.head(self.limit)
                    logging.info(f"Selected first {self.limit} rows")

            # Process URLs
            processed_data = []
            for _, row in df.iterrows():
                url = row.get("URL")
                if not url:
                    logging.warning("Missing URL in row. Skipping.")
                    continue

                record = self.process_url(url, whois_api_key, ip_api_key)
                if record:
                    processed_data.append(record)

            # Save as single JSON file
            with self.target_path.open("w", encoding="utf-8") as f:
                json.dump(processed_data, f, ensure_ascii=False, indent=4)

            logging.info(f"Processing complete. Processed {len(processed_data)} records.")

        except Exception as e:
            logging.error(f"Error during data processing: {e}")
            raise


def main():
    # Configuration
    start_time = time.time()
    SOURCE_PATH = r"C:\Users\sridh\OneDrive\Desktop\fyp\Grambeddings\random_malignant_5000.csv"
    TARGET_PATH = Path("processed_data.json")
    LIMIT = 10
    IS_RANDOM = False
    WHOIS_API_KEY = "c9fb65bde43648eea1ff68fd29994a26"
    IP_API_KEY = "f799932a57c40c"

    # Create and run processor
    processor = DataProcessor(SOURCE_PATH, TARGET_PATH, LIMIT, IS_RANDOM)
    processor.process_data(WHOIS_API_KEY, IP_API_KEY)
    end_time = time.time()
    print(f"{end_time - start_time} seconds")

if __name__ == "__main__":
    main()

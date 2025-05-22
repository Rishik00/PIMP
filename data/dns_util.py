from concurrent.futures import ThreadPoolExecutor
import json
import ipinfo
import dns.asyncresolver
import tldextract
import time, os
from typing import List, Dict, Optional
import asyncio
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
IP_INFO_API_KEY = os.getenv('IPINFO_API_KEY_TWO')

async def get_ip_geolocation(ip: str, handler: ipinfo.Handler) -> str:
    try:
        details = handler.getDetails(ip)
        return details.country if details and 'country' in details.all else 'Unknown'
    except Exception:
        return "Unknown"

async def get_dns_records_with_geolocation(url: str, handler: ipinfo.Handler) -> Dict:
    domain = tldextract.extract(url).registered_domain
    records = {}

    try:
        # A records (IP Address)
        a_records = await dns.asyncresolver.resolve(domain, 'A')
        ip_list = [r.to_text() for r in a_records]

        # Get geolocation for all IPs concurrently
        geolocations = await asyncio.gather(
            *[get_ip_geolocation(ip, handler) for ip in ip_list]
        )

        records['A'] = {'IPs': ip_list, 'Geolocations': geolocations}

    except Exception:
        records['A'] = {'IPs': [], 'Geolocations': []}

    try:
        # MX records (Mail servers)
        mx_records = await dns.asyncresolver.resolve(domain, 'MX')
        mx_ips = []
        mx_geolocations = []

        # Process MX records concurrently
        mx_tasks = []
        for mx_record in mx_records:
            mx_host = mx_record.exchange.to_text()
            mx_tasks.append(dns.asyncresolver.resolve(mx_host, 'A'))

        mx_results = await asyncio.gather(
            *mx_tasks, return_exceptions=True
        )

        for result in mx_results:
            if not isinstance(result, Exception):
                for r in result:
                    ip = r.to_text()
                    mx_ips.append(ip)

        # Get geolocation for MX IPs concurrently
        if mx_ips:
            mx_geolocations = await asyncio.gather(
                *[get_ip_geolocation(ip, handler) for ip in mx_ips]
            )

        records['MX'] = {'MailServers': [r.exchange.to_text() for r in mx_records],'IPs': mx_ips, 'Geolocations': mx_geolocations}

    except Exception:
        records['MX'] = {'MailServers': [], 'IPs': [], 'Geolocations': []}

    return records

async def get_dns_records_with_geolocation(url: str, handler: ipinfo.Handler) -> Dict:
    domain = tldextract.extract(url).registered_domain
    records = {}

    try:
        # A records (IP Address)
        a_records = await dns.asyncresolver.resolve(domain, 'A')
        ip_list = [r.to_text() for r in a_records]

        # Get geolocation for all IPs concurrently
        geolocations = await asyncio.gather(
            *[get_ip_geolocation(ip, handler) for ip in ip_list]
        )

        records['A'] = {'IPs': ip_list, 'Geolocations': geolocations}

    except Exception:
        records['A'] = {'IPs': [], 'Geolocations': []}

    try:
        # MX records (Mail servers)
        mx_records = await dns.asyncresolver.resolve(domain, 'MX')
        mx_ips = []
        mx_geolocations = []

        # Process MX records concurrently
        mx_tasks = []
        for mx_record in mx_records:
            mx_host = mx_record.exchange.to_text()
            mx_tasks.append(dns.asyncresolver.resolve(mx_host, 'A'))

        mx_results = await asyncio.gather(
            *mx_tasks, return_exceptions=True
        )

        for result in mx_results:
            if not isinstance(result, Exception):
                for r in result:
                    ip = r.to_text()
                    mx_ips.append(ip)

        # Get geolocation for MX IPs concurrently
        if mx_ips:
            mx_geolocations = await asyncio.gather(
                *[get_ip_geolocation(ip, handler) for ip in mx_ips]
            )

        records['MX'] = {'MailServers': [r.exchange.to_text() for r in mx_records],'IPs': mx_ips, 'Geolocations': mx_geolocations}

    except Exception:
        records['MX'] = {'MailServers': [], 'IPs': [], 'Geolocations': []}

    return records

async def process_url_async(url: str, handler: ipinfo.Handler) -> Dict:
    try:
        dns_records = await get_dns_records_with_geolocation(url, handler)
        return {
            'url': url,
            'dns_records': dns_records
        }
    except Exception as e:
        return {
            'url': url,
            'dns_records': {},
            'error': str(e)
        }                                                                                                                            

async def process_batch_async(urls: List[str], handler: ipinfo.Handler) -> List[Dict]:
    """Process a batch of URLs concurrently and return their DNS records"""
    tasks = [process_url_async(url, handler) for url in urls]
    return await asyncio.gather(*tasks)

def process_chunk(chunk: pd.DataFrame, url_column: str, handler: ipinfo.Handler) -> List[Dict]:
    """Process a chunk of URLs in a separate thread"""
    urls = chunk[url_column].tolist()
    return asyncio.run(process_batch_async(urls, handler))

def batch_process_csv(
    input_csv_path: str,
    output_json_path: str,
    url_column: str,
    batch_size: int = 50,
    num_threads: int = 4,
    api_key: str = IP_INFO_API_KEY[0],
    limit: Optional[int] = None
) -> None:

    # Initialize handler
    handler = ipinfo.getHandler(api_key)

    # Calculate total rows to process
    all_results = []
    total_processed = 0
    total_rows = sum(1 for _ in pd.read_csv(input_csv_path, chunksize=batch_size))
    total_rows = total_rows * batch_size  # Approximate total

    if limit:
        print(f"Processing up to {limit} URLs out of {total_rows} total URLs")
        if limit < batch_size:
            batch_size = limit
    else:
        print(f"Processing all {total_rows} URLs")

    # Read CSV in chunks
    chunk_iterator = pd.read_csv(input_csv_path, chunksize=batch_size)
    chunk_iterator.columns = ['Class', 'URL']
    print(chunk_iterator.columns)
    print(f"Starting batch processing of {input_csv_path}")

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_chunk = {}

        # Submit chunks to thread pool with limit consideration
        for chunk in chunk_iterator:
            if limit and total_processed >= limit:
                break

            # If this chunk would exceed the limit, trim it
            if limit:
                remaining = limit - total_processed
                if len(chunk) > remaining:
                    chunk = chunk.head(remaining)

            # fn: process_chunk(chunk: pd.DataFrame, url_column: str, handler: ipinfo.Handler) -> List[Dict]:

            future = executor.submit(process_chunk, chunk, url_column, handler)
            future_to_chunk[future] = chunk
            total_processed += len(chunk)

        # Reset counter for progress tracking
        total_processed = 0

        # Process completed chunks
        for future in asyncio.as_completed(future_to_chunk):
            try:
                batch_results = future.result()
                all_results.extend(batch_results)

                # Update progress
                total_processed += len(batch_results)
                elapsed_time = time.time() - start_time
                print(f"Processed {total_processed} URLs in {elapsed_time:.2f} seconds")

                # Periodically save results
                with open(output_json_path, 'w') as f:
                    json.dump(all_results, f, indent=2)

            except Exception as e:
                print(f"Error processing batch: {str(e)}")

    print(f"\nProcessing complete! Results saved to {output_json_path}")
    print(f"Total URLs processed: {total_processed}")
    print(f"Total time taken: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    batch_process_csv(
        input_csv_path="/content/random_10000_set3.csv",
        output_json_path="url_dns_results_test_set3.json",
        url_column="URL",
        batch_size=150,
        num_threads=6,
    )
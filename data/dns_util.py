import ipinfo
import dns.asyncresolver
import tldextract
from typing import List, Dict
import asyncio
import pandas as pd

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
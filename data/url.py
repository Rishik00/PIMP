from pathlib import Path
import re
import json
import time
from urllib.parse import urlparse, unquote
from math import log2
from typing import Dict
from collections import Counter

def entropy(url):
    char_frequency = {}
    entropy = 0
    total_chars = len(url)

    for char in url: 
        char_frequency[char] = char_frequency.get(char, 0) + 1
    
    for count in char_frequency.values():
        prob = count / total_chars
        entropy -= prob * log2

    return entropy

def process_url(url):
    metadata = {}
    
    # Basic URL cleaning
    url = url.strip().lower()
    parsed = urlparse(url)
    try:
        # Basic Length Features
        metadata['url_length'] = len(url)
        metadata['hostname_length'] = len(parsed.netloc)

        # Domain Components
        hostname_parts = parsed.netloc.split('.')
        metadata['subdomain_length'] = len('.'.join(hostname_parts[:-2])) if len(hostname_parts) > 2 else 0
        metadata['tld_length'] = len(hostname_parts[-1]) if hostname_parts else 0
        metadata['domain_length'] = len(hostname_parts[-2]) if len(hostname_parts) > 1 else 0

        # Count Features
        metadata['num_digits'] = sum(c.isdigit() for c in url)
        metadata['num_letters'] = sum(c.isalpha() for c in url)  # Added missing feature
        metadata['num_special_chars'] = sum(not c.isalnum() for c in url)  # Added missing feature
        metadata['num_paths'] = len([p for p in parsed.path.split('/') if p])
        metadata['num_query_params'] = len(parsed.query.split('&')) if parsed.query else 0
        metadata['num_fragments'] = 1 if parsed.fragment else 0
        metadata['num_subdomains'] = len(hostname_parts) - 2 if len(hostname_parts) > 2 else 0
        metadata['num_hyphens'] = url.count('-')
        metadata['num_slashes'] = url.count('/')

        # Boolean Indicators
        metadata['has_ip'] = bool(re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', parsed.netloc))
        metadata['is_https'] = parsed.scheme == 'https'
        metadata['has_port'] = bool(re.search(r':\d+', parsed.netloc))
        metadata['has_credentials'] = '@' in parsed.netloc
        metadata['has_query_string'] = bool(parsed.query)
        metadata['has_fragment'] = bool(parsed.fragment)
        metadata['has_hex_chars'] = bool(re.search(r'%[0-9a-fA-F]{2}', url))
        metadata['has_www'] = parsed.netloc.startswith('www.')
        metadata['entropy'] = entropy(url)

        # Path and Query Analysis
        metadata['path_length'] = len(parsed.path)
        metadata['query_length'] = len(parsed.query)
        metadata['avg_path_length'] = sum(len(p) for p in parsed.path.split('/') if p) / metadata['num_paths'] if metadata['num_paths'] > 0 else 0
    
    except Exception as e:
        # Return default values if parsing fails
        metadata = {k: 0 for k in [
            'url_length', 'hostname_length', 'subdomain_length', 'tld_length', 'domain_length',
            'num_digits', 'num_letters', 'num_special_chars', 'num_paths', 'num_query_params',
            'num_fragments', 'num_subdomains', 'num_underscores',
            'num_slashes', 'path_length', 'query_length', 'avg_path_length', 'digit_ratio',
            'letter_ratio', 'special_char_ratio', 'url_entropy'
        ]}
        metadata.update({k: False for k in [
            'has_ip', 'is_https', 'has_port', 'has_credentials', 'has_query_string',
            'has_fragment', 'has_hex_chars', 'has_www'
        ]})

    return metadata


def store_output(urls, output_file, batch_size: int = 1000):
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    all_metadata = []
    total_urls = len(urls)
    
    print(f"Processing {total_urls} URLs...")
    start_time = time.time()
    
    # Process URLs in batches
    for i in range(0, total_urls, batch_size):
        batch = urls[i:i + batch_size]
        batch_metadata = []
        
        # Process each URL in the batch
        for url in batch:
            metadata = process_url(url)
            metadata['url'] = url  # Include original URL in metadata
            batch_metadata.append(metadata)
        
        # Write batch to file
        with open(output_file, 'a' if i > 0 else 'w', encoding='utf-8') as f:
            if i == 0:  # Start the JSON array
                f.write('[\n')
            
            # Write each metadata entry
            for j, metadata in enumerate(batch_metadata):
                json.dump(metadata, f, indent=2)
                # Add comma if not last entry in entire dataset
                if i + j + 1 < total_urls:
                    f.write(',\n')
                else:
                    f.write('\n')
        
        # Print progress
        processed = min(i + batch_size, total_urls)
        elapsed = time.time() - start_time
        speed = processed / elapsed if elapsed > 0 else 0
        print(f"Processed {processed}/{total_urls} URLs ({speed:.2f} URLs/sec)")
    
    # Close the JSON array
    with open(output_file, 'a') as f:
        f.write(']')
    
    print(f"\nDone! Metadata written to {output_file}")
    print(f"Total time: {time.time() - start_time:.2f} seconds")



if __name__ == "__main__":
    pass
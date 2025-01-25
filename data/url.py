import re
from urllib.parse import urlparse, unquote
from math import log2
from typing import Dict
from collections import Counter

def get_url_metadata(url: str) -> Dict:
    metadata = {}

    try:
        # Basic URL cleaning
        url = url.strip().lower()
        parsed = urlparse(url)

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

        if url:
            # Character frequency ratios
            total_chars = len(url)
            metadata['digit_ratio'] = metadata['num_digits'] / total_chars
            metadata['letter_ratio'] = metadata['num_letters'] / total_chars
            metadata['special_char_ratio'] = metadata['num_special_chars'] / total_chars

            # Entropy calculation
            char_freq = {}
            for char in url:
                char_freq[char] = char_freq.get(char, 0) + 1
            entropy = 0
            for count in char_freq.values():
                prob = count / total_chars
                entropy -= prob * log2(prob)
            metadata['url_entropy'] = round(entropy, 4)

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

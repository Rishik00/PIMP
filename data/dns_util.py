import ipinfo
import dns.resolver
import tldextract

def get_ip_geolocation(ip, handler):
    try:
        details = handler.getDetails(ip)
        return details.country if details and 'country' in details.all else 'Unknown'
    except Exception as e:
        return "Unknown"

def get_dns_records_with_geolocation(url, api_key):
    # Initialize ipinfo handler
    handler = ipinfo.getHandler(api_key)

    # Extract the registered domain from the URL
    domain = tldextract.extract(url).registered_domain
    records = {}

    try:
        # A records (IP Address)
        a_records = dns.resolver.resolve(domain, 'A')
        ip_list = [r.to_text() for r in a_records]
        records['A'] = {
            'IPs': ip_list,
            'Geolocations': [get_ip_geolocation(ip, handler) for ip in ip_list]
        }
    except Exception as e:
        records['A'] = {'IPs': [], 'Geolocations': []}

    try:
        # MX records (Mail servers)
        mx_records = dns.resolver.resolve(domain, 'MX')
        mx_ips = []
        mx_geolocations = []

        for mx_record in mx_records:
            mx_host = mx_record.exchange.to_text()
            try:
                # Resolve the IPs for each mail server (MX)
                mx_a_records = dns.resolver.resolve(mx_host, 'A')
                for r in mx_a_records:
                    ip = r.to_text()
                    mx_ips.append(ip)
                    mx_geolocations.append(get_ip_geolocation(ip, handler))
            except Exception as e:
                continue

        records['MX'] = {
            'MailServers': [r.exchange.to_text() for r in mx_records],
            'IPs': mx_ips,
            'Geolocations': mx_geolocations
        }
    except Exception as e:
        records['MX'] = {'MailServers': [], 'IPs': [], 'Geolocations': []}

    return records
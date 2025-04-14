import json
import pandas as pd

def save_dns_metadata(file_name):
    with open(file_name, 'r') as f:
        json_data = json.load(f)

    dns_metadata = []

    for record in json_data:
        dns_records = record.get('dns_records', {})

        # Extract A records
        a_records = dns_records.get('A', {})
        a_ips = a_records.get('IPs', [])
        a_geolocations = a_records.get('Geolocations', [])

        # Extract MX records
        mx_records = dns_records.get('MX', {})
        mx_ips = mx_records.get('IPs', [])
        mx_geolocations = mx_records.get('Geolocations', [])

        # Determine if it has an IP (A or MX records)
        has_ip = int(bool(a_ips) or bool(mx_ips))

        # Convert lists to comma-separated strings, or use 'NaN' if empty
        metadata = {
            'url': record.get('url', ''),
            'has_ip': has_ip,
            'a_record': ', '.join(a_ips) if a_ips else 'NaN',
            'mx_record': ', '.join(mx_ips) if mx_ips else 'NaN',
            'geolocation': ', '.join(a_geolocations) if a_geolocations else 
                          ', '.join(mx_geolocations) if mx_geolocations else 'NaN'
        }

        dns_metadata.append(metadata)

    # Convert to DataFrame
    df = pd.DataFrame(dns_metadata)

    # Save to CSV
    csv_file = "/content/dns_metadata.csv"
    df.to_csv(csv_file, index=False)
    print(f"Saved CSV to {csv_file}")

    return csv_file

def save_url_info(file_name):
    f = open(file_name, 'r')
    json_data = json.load(f)
    df = pd.DataFrame(json_data)

    ofile_name = file_name.split('.json')[0] + '.csv'
    df.to_csv(ofile_name)

    return ofile_name

def main(url_file_name, dns_file_name):
    print('Post processing')

    url_file = save_url_info(url_file_name)
    dns_file = save_dns_metadata(dns_file_name)

    df1 = pd.read_csv(url_file)
    df2 = pd.read_csv(dns_file)

    merged_df = pd.merge([df1, df2], on='url')
    merged_df.tocsv_('preprocessed_data.csv')

    print('Post processing complete')

if __name__ == "__main__":
    pass
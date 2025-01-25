import http.client
import json  # For parsing JSON responses

import http.client
import json
from datetime import datetime

def whois_lookup(api_key, domain_name):
    conn = http.client.HTTPSConnection("api.whoisfreaks.com")
    headers = {}
    data = {}
    endpoint = f"/v1.0/whois?apiKey={api_key}&whois=live&domainName={domain_name}"

    try:
        conn.request("GET", endpoint, headers=headers)

        res = conn.getresponse()
        response_data = res.read()
        decoded_data = response_data.decode("utf-8")

        # Attempt to parse JSON and print in readable format
        try:
            response_json = json.loads(decoded_data)

            # Ensure the keys exist before accessing
            if 'query_time' in response_json:
                data['query_time'] = response_json['query_time']

            if 'domain_registered' in response_json:
                data['domain_registered'] = response_json['domain_registered']

                if data['domain_registered'] == 'yes' and 'registry_data' in response_json:
                    registry_data = response_json['registry_data']

                    # Accessing registry data only if keys exist
                    data['domain_name'] = registry_data.get('domain_name', 'N/A')
                    data['create_date'] = registry_data.get('create_date', 'N/A')
                    data['update_date'] = registry_data.get('update_date', 'N/A')
                    data['expiry_date'] = registry_data.get('expiry_date', 'N/A')

                    # Calculate the number of days the domain has existed
                    create_date_str = data['create_date']
                    expiry_date_str = data['expiry_date']


                    if create_date_str != 'N/A':
                        try:
                            create_date = datetime.strptime(create_date_str, '%Y-%m-%d')
                            expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d')

                            days_existed = (expiry_date - create_date).days
                            data['days_existed'] = days_existed
                        except ValueError:
                            data['days_existed'] = 'Invalid create_date format'

                    if 'domain_registrar' in registry_data:
                        registrar = registry_data['domain_registrar']
                        data['regname'] = registrar.get('registrar_name', 'N/A')
                        data['whoisserver'] = registrar.get('whois_server', 'N/A')
                        data['website_url'] = registrar.get('website_url', 'N/A')

                    if 'name_servers' in response_json:
                        if len(response_json['name_servers']) >= 2:
                            for idx, server in enumerate(response_json['name_servers'][:2]):
                                data[f"name_server_{idx}"] = server

            return data

        except json.JSONDecodeError:
            print(decoded_data)

    except Exception as e:
        print("An error occurred during the WHOIS lookup:", str(e))

    finally:
        conn.close()
## Redundant - but can keep

import requests
import json
from datetime import datetime

class WhoisClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()  # Reuse the session for efficiency
        self.base_url = "https://api.whoisfreaks.com/v1.0/whois"

    def lookup(self, domain_name):
        params = {
            "apiKey": self.api_key,
            "whois": "live",
            "domainName": domain_name
        }

        try:
            response = self.session.get(self.base_url, params=params, timeout=5)
            response.raise_for_status()  # Raise exception for HTTP errors
            response_json = response.json()

            data = {
                "query_time": response_json.get("query_time"),
                "domain_registered": response_json.get("domain_registered"),
            }

            if data["domain_registered"] == "yes":
                registry_data = response_json.get("registry_data", {})

                # Extract registry details
                data.update({
                    "domain_name": registry_data.get("domain_name", "N/A"),
                    "create_date": registry_data.get("create_date", "N/A"),
                    "update_date": registry_data.get("update_date", "N/A"),
                    "expiry_date": registry_data.get("expiry_date", "N/A")
                })

                # Calculate days existed if dates are valid
                try:
                    if data["create_date"] != "N/A" and data["expiry_date"] != "N/A":
                        create_date = datetime.strptime(data["create_date"], '%Y-%m-%d')
                        expiry_date = datetime.strptime(data["expiry_date"], '%Y-%m-%d')
                        data["days_existed"] = (expiry_date - create_date).days
                except ValueError:
                    data["days_existed"] = "Invalid date format"

                # Extract registrar details
                registrar = registry_data.get("domain_registrar", {})
                data.update({
                    "regname": registrar.get("registrar_name", "N/A"),
                    "whoisserver": registrar.get("whois_server", "N/A"),
                    "website_url": registrar.get("website_url", "N/A"),
                })

                # Extract name servers (limit to 2)
                name_servers = response_json.get("name_servers", [])
                for idx, server in enumerate(name_servers[:2]):
                    data[f"name_server_{idx}"] = server

            return data

        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

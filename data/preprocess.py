from pathlib import Path
from typing import Dict, List
import pandas as pd
import typer
import yaml
import json
import random

## Local imports
from dns_util import batch_process_csv
from url import process_urls_to_json, get_url_metadata

def validate_config(config: Dict) -> None:
    missing_fields = [field for field in config.keys() if config.get(field) is None]
    if missing_fields:
        raise ValueError(f"Missing required config fields: {', '.join(missing_fields)}")

def open_config(config_path: str, mode: str):
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    config = config_data.get(mode, {})

    if validate_config(config):
        return config
    else:
        return {}
    
def randomly_sample(df, train_size):
    size = df.shape[0]

    train_idxs = random.sample(range(size), train_size)
    train_df = df.loc(train_idxs)

    return train_df

def save_url_info(file_name):
    f = open(file_name, 'r')
    json_data = json.load(f)
    df = pd.DataFrame(json_data)

    ofile_name = file_name.split('.json')[0] + '.csv'
    df.to_csv(ofile_name)

    return f'Saved URL metadata to {ofile_name}'

def save_dns_metadata(file_name, CSV_FILE):
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
    df.to_csv(CSV_FILE, index=False)
    print(f"Saved CSV to {CSV_FILE}")

def save_files(url_file, dns_file, ofile_name):
  url_df = pd.read_csv(url_file)

  dns_df = pd.read_csv(dns_file)
  dns_df["geolocation"] = dns_df["geolocation"].str.split(",").str[0]

  # Merge on 'id' column
  merged_df = pd.merge(url_df, dns_df, on='url', how='inner')
  merged_df.to_csv(ofile_name)

  print(f'Saved data to {ofile_name}')

def run_pipeline_for_training(config):
    dataset_path = config.get('dataset_dir', '') if config else ''
    output_file = config.get('dataset_dir', '') + config.get('train_size', '')

    train_size = config.get('train_size', 70000)

    source_col = config.get('source_column', '')
    target_col = config.get('target_column', '')

    batch_size = config.get('batch_size', '')
    num_runs = config.get('num_runs', 1)

    print(f"[INFO] Loading dataset from: {dataset_path}")
    df = pd.read_csv(dataset_path)

    ## Checking whether source and target columns are correct
    if source_col in df.columns and target_col in df.columns:
        print(f'[WARNING]: {source_col} and {target_col}')
        return
    
    train_df, val_df = randomly_sample(df, train_size)

    train_urls = train_df[source_col]
    train_classes = train_df[target_col]

    for i in range(1, 2):
        print(f"[INFO] Processing {len(train_urls)} URLs to extract metadata")
        process_urls_to_json(train_urls, train_classes, f'train_url_metadata{i}.json')
        print(f"[INFO] URL metadata processing complete. Output saved to: train_url_metadata")

    print("[INFO] Geolocations are being fetched.")

    for i in range(1, 2):
        output_file_dns = f"url_dns_results_train_set{i}.json"
        batch_process_csv(
            input_csv_path=train_df,
            output_json_path=output_file_dns,
            url_column=source_col,
            batch_size=batch_size,
            num_threads=6,
        )

    for i in range(1,4):
        file_name = f'/content/test{i}_metadata.json'
        save_url_info(file_name)

    for i in range(1,4):
        file_name = f'/content/url_dns_results_train_set{i}.json'
        csv_file = f"/content/dns_metadata_train_set{i}.csv"

        save_dns_metadata(file_name, csv_file)

    for i in range(1, 4):
        metadata_path = f'/content/test{i}_metadata.csv'
        dns_path = f'/content/dns_metadata_test_set{i}.csv'
        output_file_path = f'full_test_data_set{i}.csv'
        save_files(metadata_path, dns_path, output_file_path)
    

def run_pipeling_for_inferencing():
    pass


## Entry point for pipeline
def run_pipeline(config_path, mode, load_to_hf):

    if mode == 'train':
        config = open_config(config_path, 'train_config')
        run_pipeline_for_training(config)
    else:

        #TODO: implement later
        config = open_config(config_path, 'inference_config')
        run_pipeline_for_training(config)


if __name__ == "__main__":
    typer.run(run_pipeline)
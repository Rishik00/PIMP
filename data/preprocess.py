from pathlib import Path
from typing import Dict, List
import pandas as pd
import typer
import yaml
import time

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
    
def randomly_sample(df, train_size, val_size):
    train_idxs = []
    val_idxs = []

    train_df = None
    val_df = None

    return train_df, val_df

def run_pipeline_for_training(config):
    dataset_path = config.get('dataset_dir', '') if config else ''
    output_file = config.get('dataset_dir', '') + config.get('train_size', '')

    train_size = config.get('train_size', 70000)
    val_size = config.get('val_size', 10000)

    print(f"[INFO] Loading dataset from: {dataset_path}")
    df = pd.read_csv(dataset_path)
    
    train_df, val_df = randomly_sample(df, train_size, val_size)

    train_urls = train_df['URL']
    train_classes = train_df['class']

    val_urls = val_df['URL']
    val_classes = val_df['class']

    print(f"[INFO] Processing {len(train_urls)} URLs to extract metadata - ")
    process_urls_to_json(train_urls, train_classes, 'train_url_metadata')
    print(f"[INFO] URL metadata processing complete. Output saved to: train_url_metadata")

    print(f"[INFO] Processing {len(val_urls)} URLs to extract metadata - ")
    process_urls_to_json(val_urls, val_classes, 'val_url_metadata')
    print(f"[INFO] URL metadata processing complete. Output saved to: val_url_metadata")

    if config.get('include_geolocations'):
        print("[INFO] Geolocation is enabled.")
        pass

    
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
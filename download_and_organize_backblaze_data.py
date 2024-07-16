import os
import requests
import pandas as pd
from zipfile import ZipFile
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

download_dir = 'downloads'
extract_dir = 'extracted'
datalake_dir = 'datalake'

os.makedirs(download_dir, exist_ok=True)
os.makedirs(extract_dir, exist_ok=True)
os.makedirs(datalake_dir, exist_ok=True)

base_url = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q{quarter}_{year}.zip"
quarters = ['Q1']
years = ['2019']

def download_and_extract(file_url, download_path, all_data):
    # Check if the file already exists
    if not os.path.exists(download_path):
        response = requests.get(file_url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        with open(download_path, 'wb') as file:
            for data in tqdm(response.iter_content(block_size), total=total_size//block_size, unit='KB', unit_scale=True, desc=f"Downloading {download_path}"):
                file.write(data)
    
    # Extract the ZIP file
    with ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # Process the CSV files
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if file.endswith('.csv'):
                # Ignore __MACOSX folder
                if '__MACOSX' in root:
                    continue
                src_path = os.path.join(root, file)
                print(f"Processing {src_path}")
                # Load the CSV file and append to the all_data DataFrame
                df = pd.read_csv(src_path)
                all_data.append(df)

all_data = []

with ThreadPoolExecutor(max_workers=12) as executor:
    futures = []
    for year in years:
        for quarter in quarters:
            file_url = base_url.format(quarter=quarter[-1], year=year)
            file_name = f"data_{quarter}_{year}.zip"
            download_path = os.path.join(download_dir, file_name)
            futures.append(executor.submit(download_and_extract, file_url, download_path, all_data))

    for future in as_completed(futures):
        future.result()

# Concatenate all the data frames
final_df = pd.concat(all_data, ignore_index=True)

# Save the final DataFrame to a CSV file
final_df.to_csv(os.path.join(datalake_dir, 'all_data.csv'), index=False)

print(f"Final data saved to all_data.csv")

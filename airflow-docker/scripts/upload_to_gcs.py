import os
import json
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import subprocess

# --- Load .env if exists ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Read environment variables ---
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
KAGGLE_TOKEN = os.getenv("KAGGLE_API_TOKEN")
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME", "")

if not PROJECT_ID or not BUCKET_NAME:
    raise ValueError("Set PROJECT_ID and BUCKET_NAME in environment variables")
if not KAGGLE_TOKEN:
    raise ValueError("KAGGLE_API_TOKEN not found in environment variables!")

# --- Setup Kaggle config ---
kaggle_config_dir = os.path.join(os.getcwd(), ".kaggle")
os.makedirs(kaggle_config_dir, exist_ok=True)
kaggle_json_path = os.path.join(kaggle_config_dir, "kaggle.json")
with open(kaggle_json_path, "w") as f:
    json.dump({"username": KAGGLE_USERNAME, "key": KAGGLE_TOKEN}, f)
os.chmod(kaggle_json_path, 0o600)
os.environ["KAGGLE_CONFIG_DIR"] = kaggle_config_dir


# --- Authenticate Kaggle API ---
api = KaggleApi()
api.authenticate()

# --- Datasets to download ---
datasets = {
    "mental_health": "thedevastator/uncover-global-trends-in-mental-health-disorder",
    "gdp_per_capita": "holoong9291/gdp-of-all-countries19602020",
    "unemployment": "pantanjali/unemployment-dataset"
}

download_folder = "data"
os.makedirs(download_folder, exist_ok=True)

# --- Initialize GCS client ---
client = storage.Client(project=PROJECT_ID)
bucket = client.bucket(BUCKET_NAME)

# --- Download Kaggle datasets and upload to GCS ---
rename_map = {
    "gdp_1960_2020.csv": "gdp_data.csv",
    "Mental health Depression disorder Data.csv": "mental_health_data.csv",
    "unemployment analysis.csv": "unemployment_data.csv"
}

for name, dataset in datasets.items():
    print(f"Downloading {name} from Kaggle...")
    api.dataset_download_files(dataset, path=download_folder, unzip=True)
    
    # Upload downloaded CSV files to GCS
    for file in os.listdir(download_folder):
        if file.endswith(".csv"):
            new_name = rename_map.get(file, file)
            blob = bucket.blob(f"raw/{new_name}")
            blob.upload_from_filename(os.path.join(download_folder, file))
            print(f"{file} uploaded as {new_name} to GCS!")


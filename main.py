# -*- coding: utf-8 -*-
"""
Created on Sun Oct 27 07:12:26 2024

@author: olanr
"""

from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from openai import OpenAI
from openai import APIConnectionError, RateLimitError
import asyncio
from enum import Enum
import time
from groq import Groq
import io
import pyarrow.parquet as pq
import pyarrow as pa



from azure.core.exceptions import ResourceNotFoundError

load_dotenv()

INPUT_JSONL_FOLDER = "input_jsonl"
OUTPUT_JSONL_FOLDER = "output_jsonl"
GROQ_API_KEY = os.getenv("GROQ_API_KEY")


openai_client = OpenAI()
groq_client = Groq(api_key = GROQ_API_KEY)

TIME_OFFSET = 1000
current_year = datetime.now().year
current_month = datetime.now().month
current_month_name = datetime.now().strftime("%B")
current_day = 26
# current_day = datetime.now().day
current_hour = datetime.now().hour
current_minute = datetime.now().minute
current_second = datetime.now().second


LOG_FOLDER = f"wikilogs/{current_year}/{current_month_name}/{current_day}/"
LLM_LOG_FOLDER = f"wikilogs_llm/{current_year}/{current_month_name}/{current_day}/"

EDIT_OVERVIEW_ADLS_FOLDER = f"edit_overview/{current_year}/{current_month_name}/{current_day}/"
VITAL_LABEL_ADLS_FOLDER = f"vital_labels/{current_year}/{current_month_name}/{current_day}/"
DATETIME_TRACKER_ADLS_FOLDER = f"datetime_tracker/{current_year}/{current_month_name}/{current_day}/"


logfile = f"logs_{current_year}_{current_month}_{current_day}.parquet"
llm_logfile = f"llm_logs_{current_year}_{current_month}_{current_day}.parquet"
json_filepath = f"datetime_tracker_{current_year}_{current_month}_{current_day}_{current_hour}_{current_minute}_{current_second}.json"


# Initialize the Data Lake Service Client
account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
sas_token = os.getenv("AZURE_SAS_TOKEN")
container_name = os.getenv("AZURE_CONTAINER_NAME")


folder_path = f"wikistream/year={current_year}/month={current_month}/day={current_day}"  # Adjust as necessary


def create_default_json(json_filepath):
    default_dict = {}
    default_dict["start_datetime"] = str(datetime.now() - timedelta(days= TIME_OFFSET))
    
    with open(json_filepath, "w") as json_file:
        json.dump(default_dict, json_file, indent=4)
        
    print("Initial JSON data written to ", json_filepath)
    

def read_datetime_from_json(json_filepath: str)-> Dict:
    if not os.path.isfile(json_filepath):
        create_default_json(json_filepath)
        
    with open(json_filepath, "r") as json_file:
        data = json.load(json_file)
        
    return data["start_datetime"]


def edit_datetime_in_json(json_filepath: str, update_dict: Dict):
    if os.path.isfile(json_filepath):    
        with open(json_filepath, "r") as json_file:
            json_data = json.load(json_file)
            
        json_data.update(update_dict)
        
        with open(json_filepath, "w") as json_file:
            json.dump(json_data, json_file, indent = 4)
            
        print("JSON file updated with ", update_dict)
        
    else:
        raise FileNotFoundError(f"JSON file, {json_filepath} does not exist. Please write initial data first")
            



def process_file(path: str, container_client: FileSystemClient)-> pd.DataFrame:
    """Helper function to download and process a single file."""
    file_client = container_client.get_file_client(path["name"])
    download = file_client.download_file()
    file_content = download.readall().decode("utf-8")
    json_data = json.loads(file_content)
    
    return pd.json_normalize(json_data), path["last_modified"]


def filter_files_by_date(folder_path: str, container_client: FileSystemClient, start_date: datetime)-> List:
    paths = container_client.get_paths(path=folder_path)
    
    filtered_files = []
    
    for path in paths:
        if not path.is_directory:
            # Check if the path's last_modified fits the date criteria
            last_modified = path.last_modified.replace(tzinfo=timezone.utc)

            # Apply the filtering based on start and end dates
            if (start_date is None or last_modified > start_date):
                file_info = {
                    "name": path.name,
                    "last_modified": last_modified,
                    "content_length": path.content_length,
                    "etag": path.etag,
                    "permissions": path.permissions,
                    "is_directory": path.is_directory
                }
                filtered_files.append(file_info)
    
    return filtered_files


def get_new_data(folder_path: str, container_client: FileSystemClient, start_datetime: str)-> pd.DataFrame:
    # Retrieve all paths in the specified folder
    
    try:
        start_datetime_object = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        start_datetime_object = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S%z')
        
    start_datetime_object = start_datetime_object.replace(tzinfo=timezone.utc)
    
    filtered_paths = filter_files_by_date(folder_path, container_client, start_date = start_datetime_object)
    
    # Filter out directories and non-JSON files
    json_paths = [path for path in filtered_paths if not path["is_directory"] and path["name"].endswith(".json")]
    
    df_list = []
    last_modified_dates = []
    # Use ThreadPoolExecutor to process files concurrently
    with ThreadPoolExecutor() as executor:
        # Map the process_file function to each JSON path
        futures = [executor.submit(process_file, path, container_client) for path in json_paths]
        
        # Gather the results as each thread completes
        for future in as_completed(futures):
            try:
                df, last_modified = future.result()
                df_list.append(df)
                last_modified_dates.append(last_modified)
            except Exception as e:
                print(f"Error processing file: {e}")
    
    try:
        combined_df = pd.concat(df_list)
        max_datetime = max(last_modified_dates)
    except ValueError:
        print("New dataframe is empty")
        combined_df = pd.DataFrame()
        max_datetime = start_datetime
        
    finally:
        return combined_df, max_datetime


def prepare_update_dict(last_modified_date: datetime)-> Dict:
    return {
        "start_datetime": str(last_modified_date)
        }


def preprocess_data(df: pd.DataFrame)-> pd.DataFrame:
    if df.empty:
        return df
    
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit = "s")
    
    df.rename(columns = {"length.new": "length_new"}, inplace= True)
    df.rename(columns = {"length.old": "length_old"}, inplace= True)
    
    df["length_new"] = df["length_new"].fillna(0)
    df["length_old"] = df["length_old"].fillna(0)
    
    edit_mask = df["type"].str.lower() == "edit"
    filtered_df = df.loc[edit_mask, :]
    
    filtered_df["character_difference"] = filtered_df["length_new"] - filtered_df["length_old"]
    
    relevant_columns = ["id", "title", "title_url", "wiki", "timestamp", "user", "bot", "namespace", "comment", "parsedcomment", "length_new", "length_old", "character_difference"]
    filtered_df = filtered_df[relevant_columns]
    
    filtered_df["ingestion_date"] = datetime(current_year, current_month, current_day, current_hour, current_minute, current_second)
    return filtered_df

def upload_file_to_data_lake(service_client: DataLakeServiceClient, file_system_name: str, folder_path: str, file_path: str):
    # Create a file system client
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)

    # Create or get a directory client for the target folder
    directory_client = file_system_client.get_directory_client(folder_path)
    directory_client.create_directory()  # Ensure the folder exists

    # Upload the file
    file_client = directory_client.get_file_client(file_path)
    with open(file_path, "rb") as file_data:
        file_contents = file_data.read()
        file_client.upload_data(file_contents, overwrite=True)

    print(f"File '{file_path}' uploaded successfully to '{folder_path}' in the Data Lake.")

    # Delete local file
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted local file: {file_path}")
    else:
        print(f"File {file_path} not found locally.")


def update_log_file(container_client: FileSystemClient, folder_path: str, file_name: str, df_update: pd.DataFrame)-> None:
    directory_client = container_client.get_directory_client(folder_path)
    
    directory_client.create_directory()
    
    file_client = directory_client.get_file_client(file_name)
    
    try:
        downloaded_data = file_client.download_file()
        file_client.delete_file()
        
    except ResourceNotFoundError:
        print(f"File with name {file_name} could not be found in folder {folder_path} on the ADLS\n\n")
        adls_df = pd.DataFrame()
        
    else:
        parquet_data = downloaded_data.readall()
        adls_df = pd.read_parquet(io.BytesIO(parquet_data))
        
    
    adls_df = pd.concat([adls_df, df_update])
    
    columns_list = adls_df.columns.tolist()
    columns_list.remove("ingestion_date")
    
    adls_df.drop_duplicates(inplace = True, subset = columns_list)
    
    updated_parquet_data = io.BytesIO()
    table = pa.Table.from_pandas(adls_df)
    pq.write_table(table, updated_parquet_data)
    
    updated_parquet_data.seek(0)
    
    file_client.upload_data(updated_parquet_data, overwrite=True)
    print(f"{file_name} successfully updated\n\n")
    


def generate_and_upload_llm_logs(container_client: FileSystemClient, 
                                 wikilogs_folder_path: str, 
                                 llm_folder_path: str,
                                 file_name: str,
                                 llm_file_name: str,
                                 df_update: pd.DataFrame,
                                 cutoff: int = 3000
                                 )-> Dict:
    
    directory_client = container_client.get_directory_client(wikilogs_folder_path)
    llm_directory_client = container_client.get_directory_client(llm_folder_path)
    
    directory_client.create_directory()
    llm_directory_client.create_directory()
    
    file_client = directory_client.get_file_client(file_name)
    llm_file_client = llm_directory_client.get_file_client(llm_file_name)
    
    try:
        downloaded_data = file_client.download_file()
        llm_file_client.delete_file()
        
    except ResourceNotFoundError:
        print(f"File with name {file_name} could not be found in folder {wikilogs_folder_path} on the ADLS")
        adls_df = pd.DataFrame()
        
    else:
        parquet_data = downloaded_data.readall()
        adls_df = pd.read_parquet(io.BytesIO(parquet_data))
        
    
    adls_df = pd.concat([adls_df, df_update])
    columns_list = adls_df.columns.tolist()
    columns_list.remove("ingestion_date")
    
    adls_df.drop_duplicates(inplace = True, subset = columns_list)
    
    high_edits = adls_df["character_difference"].astype(int).abs() > cutoff
    is_bot = adls_df["bot"].astype(bool) == True
    llm_df = adls_df.loc[(high_edits) & (is_bot), :]
    
    llm_df["title"] = llm_df["title"].astype(str) + "*END* "
    llm_df["title_url"] = llm_df["title_url"].astype(str) + "*END* "
    llm_df["parsedcomment"] = llm_df["parsedcomment"].astype(str) + "*END* "
    
    llm_df.drop(["wiki", "bot", "comment"], axis = 1, inplace = True)
    
    updated_parquet_data = io.BytesIO()
    table = pa.Table.from_pandas(llm_df)
    pq.write_table(table, updated_parquet_data)
    
    updated_parquet_data.seek(0)
    
    llm_file_client.upload_data(updated_parquet_data, overwrite=True)
    print(f"{llm_file_name} successfully updated to folder {llm_folder_path}")
    


"===================================== Execution ========================================================"
start_datetime: Dict = read_datetime_from_json(json_filepath)

# Create a Data Lake Service Client
service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=sas_token
)

# Get the container client
container_client: FileSystemClient = service_client.get_file_system_client(file_system=container_name)

final_df: pd.DataFrame
last_modified_date: str
final_df, last_modified_date = get_new_data(folder_path, container_client, start_datetime)
update_dict: Dict = prepare_update_dict(last_modified_date)

edit_datetime_in_json(json_filepath, update_dict)

df_update: pd.DataFrame = preprocess_data(final_df)

update_log_file(container_client, LOG_FOLDER, logfile, df_update)

generate_and_upload_llm_logs(container_client, LOG_FOLDER, LLM_LOG_FOLDER, logfile, llm_logfile, df_update)
                                 
upload_file_to_data_lake(service_client, container_name, DATETIME_TRACKER_ADLS_FOLDER, json_filepath)


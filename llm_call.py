# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 23:38:52 2024

@author: olanr
"""


from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from typing import Dict
from azure.core.exceptions import ResourceNotFoundError
import io
import os
import pandas as pd
from openai import OpenAI
from openai import APIConnectionError, RateLimitError
from datetime import datetime
from enum import Enum
import json
from dotenv import load_dotenv

load_dotenv()

current_year = datetime.now().year
current_month = datetime.now().month
current_month_name = datetime.now().strftime("%B")
current_day = 26 #datetime.now().day
current_hour = datetime.now().hour
current_minute = datetime.now().minute
current_second = datetime.now().second


llm_logfile = f"llm_logs_{current_year}_{current_month}_{current_day}.parquet"
LLM_LOG_FOLDER = f"wikilogs_llm/{current_year}/{current_month_name}/{current_day}/"


account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
sas_token = os.getenv("AZURE_SAS_TOKEN")
container_name = os.getenv("AZURE_CONTAINER_NAME")
print(f"{container_name = }")

service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=sas_token
)

# Get the container client
container_client: FileSystemClient = service_client.get_file_system_client(file_system=container_name)


def summarize_general_stats(folder_path: str, file_name: str)-> Dict:
    directory_client = container_client.get_directory_client(folder_path)
    
    directory_client.create_directory()
    
    file_client = directory_client.get_file_client(file_name)
    
    try:
        downloaded_data = file_client.download_file()
        
    except ResourceNotFoundError:
        print(f"File with name {file_name} could not be found in folder {folder_path} on the ADLS")
        return "No data to summarize"
    
    parquet_data = downloaded_data.readall()
    adls_df = pd.read_parquet(io.BytesIO(parquet_data))
    
    general_stats = adls_df.describe()
    
    return general_stats.to_dict(orient = "list")



def generate_aggregated_stats(folder_path: str, file_name: str, word_cutoff:int = 2_600)-> Dict:
    directory_client = container_client.get_directory_client(folder_path)
    
    directory_client.create_directory()
    
    file_client = directory_client.get_file_client(file_name)
    
    try:
        downloaded_data = file_client.download_file()
        
    except ResourceNotFoundError:
        print(f"File with name {file_name} could not be found in folder {folder_path} on the ADLS")
        return "No data to summarize"
    
    parquet_data = downloaded_data.readall()
    adls_df = pd.read_parquet(io.BytesIO(parquet_data))
    
    aggregated_df = adls_df.drop(["timestamp", "ingestion_date", "id"], axis = 1).groupby(["user", "namespace"]).sum()
    aggregated_df = aggregated_df.reset_index()
    
    for idxx, row in aggregated_df.iterrows():
        idx = idxx + 1
        num_words_title = len(aggregated_df.head(idx)["title"].values[0].split())
        num_words_user = len(aggregated_df.head(idx)["user"].values[0].split())
        num_words_title_url = len(aggregated_df.head(idx)["title_url"].values[0].split())
        num_parsedcomment = len(aggregated_df.head(idx)["parsedcomment"].values[0].split())
        
        total_words = num_words_title + num_words_user + num_words_title_url + num_parsedcomment
        
        if total_words > word_cutoff:
            if idx == 1:
                truncated_df = aggregated_df.head(1)
                truncated_df["parsedcomment"].astype(str).str[:50]
                
                return truncated_df.head(1).to_dict(orient = "list")
            return aggregated_df.head(idx - 1).to_dict(orient = "list")
    
    return aggregated_df.to_dict(orient = "list")
    



tools_definition = [
  {
    "type": "function",
    "function": {
      "name": "summarize_general_stats",
      "description": "Get the latest summary statistics for a parquet logfile using df.describe.",
      "parameters": {
        "type": "object",
        "properties": {
          "folder_path": {
            "type": "string",
            "description": "Folder where the required parquet logfile is located"
          },
          "file_name": {
            "type": "string",
            "description": "The parquet logfile located in the folder_path with the information required."
          }
        },
        "required": ["folder_path", "file_name"]
      }
    }
  },
  {
    "type": "function",
    "function": {
      "name": "generate_aggregated_stats",
      "description": "Get the latest aggregated statistics from parquet logfile using groupby 'user' and 'namespace'.",
      "parameters": {
        "type": "object",
        "properties": {
          "folder_path": {
            "type": "string",
            "description": "Folder where the required parquet logfile is located"
          },
          "file_name": {
            "type": "string",
            "description": "The parquet logfile located in the folder_path with the information required."
          }
        },
        "required": ["folder_path", "file_name"]
      }
    }
  }
]


def prepare_data_for_json(data):
    prepared_data = {}
    for key, values in data.items():
        # Convert Timestamp objects to ISO strings and replace NaN with None
        prepared_data[key] = [
            value.isoformat() if isinstance(value, pd.Timestamp) else (None if pd.isna(value) else value)
            for value in values
        ]
    return prepared_data


client = OpenAI()

def run_conversation(content):
    messages = [{"role": "user", "content": content}]

    response = client.chat.completions.create(model = "gpt-4o", 
                                              messages = messages, 
                                              tools = tools_definition, 
                                              tool_choice= "auto"
                                              )
    
    response_message = response.choices[0].message
    tool_calls = response_message.tool_calls
    
    if tool_calls:
        
        messages.append(response_message)
        
        available_functions = {"summarize_general_stats": summarize_general_stats,
                               "generate_aggregated_stats": generate_aggregated_stats
                               }
        
        for tool_call in tool_calls:
            print(f"Function: {tool_call.function.name}")
            print(f"Params: {tool_call.function.arguments}")
            function_name = tool_call.function.name
            function_to_call = available_functions[function_name]
            function_args = json.loads(tool_call.function.arguments)
            function_response = function_to_call(
                folder_path = function_args.get("folder_path"),
                file_name = function_args.get("file_name")
                )
            
            function_response = prepare_data_for_json(function_response)
            
            messages.append(
                {
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": function_name,
                    "content": json.dumps(function_response)
                    }
                )
    
    second_response = client.chat.completions.create(
        model = "gpt-4o",
        messages = messages,
        )
    
    return second_response.choices[0].message.content



class PromptTemplate(Enum):
    
    params = "Folder: {folder}\nFile: {file}\n\nThe file mentioned above is used to monitor the logs of a wiki stream for suspicious bot activity."
    



def ask_question(question: str):
    
    content = PromptTemplate.params.value.format(folder = LLM_LOG_FOLDER, file = llm_logfile)
    content += question

    assistant_response = run_conversation(content)
    
    return assistant_response



"======================================== Execution ========================================================"


if __name__ == "__main__":
    question = "Give me the aggregated statistics for today"
    #ask_question(question)
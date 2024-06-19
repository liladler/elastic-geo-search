import pandas as pd    
import os    
import shutil    
import glob    
import zipfile    
from elasticsearch import Elasticsearch, helpers  
from dotenv import load_dotenv      
  
# Specify base directory for the files    
base_dir = '5749846/'    
index="tripadvisor_reviews"

# Loop through the base directory and its subdirectories to find and unzip all zip files  
for dir_name, _, file_list in os.walk(base_dir):    
    for file_name in file_list:    
        if file_name.endswith('.zip'):    
            with zipfile.ZipFile(os.path.join(dir_name, file_name), 'r') as zip_ref:    
                zip_ref.extractall(dir_name)    
  
# Specify new directory for the required files    
dir_path = 'tripadvisor_reviews/'    
os.makedirs(dir_path, exist_ok=True)  # Creates new directory if it doesn't exist  
  
# List of file names to be moved to the new directory  
file_names = ["df_Paris.pickle", "df_Barcelona.pickle", "df_London.pickle", "df_Madrid.pickle", "df_NYC.pickle"]    
  
# Loop through all directories and subdirectories in base directory to find and move the required files  
for dir_name, _, file_list in os.walk(base_dir):    
    for file_name in file_names:    
        if file_name in file_list:    
            shutil.move(os.path.join(dir_name, file_name), dir_path)    
  
# Convert all pickle files in new directory to CSV and then remove the pickle files  
pickle_files = glob.glob(dir_path + '*.pickle')    
for file in pickle_files:     
    if os.path.exists(file):   
        df = pd.read_pickle(file)      
        csv_file = file.replace('.pickle', '.csv')      
        df.to_csv(csv_file, index=False)      
        os.remove(file)  
    else:  
        print(f"{file} does not exist. Skipping to next file.")  
  
# Function to load CSV data into Elasticsearch  
def csv_to_elasticsearch(es, index_name, dir_path):    
    files = os.listdir(dir_path)  # Get a list of all files in the directory  
    files = [f for f in files if f.endswith('.csv')]  # Filter the list to only include CSV files    
  
    for file_name in files:    
        file_path = os.path.join(dir_path, file_name)  # Combine directory path and file name  
        df = pd.read_csv(file_path)  # Load CSV into a DataFrame  
        df.fillna("", inplace=True)  # Replace null values with empty string  
        df_dict = df.to_dict(orient='records')  # Convert DataFrame to dictionary format for Elasticsearch  
  
        # Prepare a list of actions for Elasticsearch bulk API  
        actions = [    
            {    
                "_index": index_name,  # Specify index name  
                 "_source": {**row, "_run_ml_inference": True},  # run through ML inference
                "pipeline": index_name # Specify pipeline
            }    
            for row in df_dict  # Loop through each row in the DataFrame dictionary  
        ]    
        try:  
            helpers.bulk(es, actions)  
        except helpers.BulkIndexError as e:  
            for error in e.errors:  
                print(error)  
  
# Load environment variables from .env file  
load_dotenv()    
  
# Initialize Elasticsearch client with credentials from environment variables  
es = Elasticsearch(    
    os.getenv('CLOUD_URL'),    
    basic_auth=(os.getenv('ELASTIC_USER'), os.getenv('ELASTIC_PASSWORD'))    
)     
  
# Load data from CSV files into Elasticsearch  
csv_to_elasticsearch(es, index, dir_path)    


import os
import zipfile
from pyspark.sql import SparkSession
import pandas as pd
import shutil
from azure.storage.blob import BlobServiceClient, BlobClient

path = "../Airport_Pipeline/Airport_data"
path2 = "../Airport_Pipeline/Geonames_data"
path3 = '../Airport_Pipeline/Geonames_data/countries.csv'

def extract():
    if not os.path.exists(path):
        os.makedirs(path)

    if not os.path.exists(path2):
        os.makedirs(path2)

    for i in os.listdir(path):
        os.remove(path +'/' + i)
    #empties the directory


    for k in os.listdir(path2):
        os.remove(path2 +'/' + k)
    #empties the directory
        
    os.system('wget -P ../Airport_Pipeline/Airport_data -i airport.txt')
    os.system('wget -P ../Airport_Pipeline/Geonames_data -i geonames.txt')
    #gets the data i need using wget with the links stored in two text files (airport.txt and geonames.txt ) \
    # and saves it to the directory as indicated above

def transform():
    for k in os.listdir(path2):
        if k.endswith('.zip'):
            with zipfile.ZipFile(path2+ '/'+k) as zf:
                zf.extractall(path2)
            os.remove(path2+ '/'+k)
    #the file types downloaded were both in .zip and .txt
    #the above code checks the directory for zip files and unzips them then deletes them from the directory
    

    spark = SparkSession.builder.appName("ReadTextFile").getOrCreate()
    header_names = ["geoname_id", "name", "asciiname","alternatenames","latitude","longitude",
        "feature_class","feature_code","country_code","altcountry_code","admin1_code","admin2_code"
        ,"admin3_code","admin4_code","population","elevation","DEM","timezone","modification_date" ]
    path4 = "/workspace/Airport_Pipeline/Geonames_data/allCountries.txt"
    #using pyspark because we have 12million plus rows of data and its faster more efficient than pandas, 

    df = spark.read.option("header", "true").option("delimiter", "\t").csv(path4).toDF(*header_names)
    #the unzipped files are in txt format without column headers, headers were given and changed to a dataframe

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv("../Airport_Pipeline/Geonames_data/countries.csv")
    #this dataframe was then saved as a csv file which was partitioned and merged back as one file
    # but pyspark gives it a different name

    for l in os.listdir(path3):
        if l.endswith('.csv'):
            os.rename(path3 + '/'+ l,'../Airport_Pipeline/Geonames_data/all_countries.csv')



    os.replace("../Airport_Pipeline/Geonames_data/all_countries.csv", "../Airport_Pipeline/Airport_data/all_countries.csv")
    #checks in the directory path3 for any csv file and renames it to all_countries.csv
    shutil.rmtree('../Airport_Pipeline/Geonames_data/countries.csv')



def load_blob_storage(storage_connection_string, container_name, source_directory):
    # Create a BlobServiceClient using the storage connection string
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # List all files in the source directory
    for file_name in os.listdir(source_directory):
        source_file_path = os.path.join(source_directory, file_name)

        blob_client = container_client.get_blob_client(file_name)

        with open(source_file_path, "rb") as data:
            blob_client.upload_blob(data)

        print(f"File '{source_file_path}' uploaded to Blob Storage.")






def loading_database():
    pass

extract()
transform()

storage_connection_string = ""
container_name = "testtech"
source_directory = path
load_blob_storage(storage_connection_string, container_name, source_directory)
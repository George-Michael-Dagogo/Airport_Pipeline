import os
import zipfile
from pyspark.sql import SparkSession
import pandas as pd
import shutil
from azure.storage.blob import BlobServiceClient, BlobClient
import psycopg2
import datetime
import requests
from bs4 import BeautifulSoup

path = "../Airport_Pipeline/Airport_data/"
path2 = "../Airport_Pipeline/Geonames_data"
path3 = '../Airport_Pipeline/Geonames_data/countries.csv'

today = datetime.date.today()

def extract_data_sources():
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

def pre_process_transform():
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

    url = "https://www.geonames.org/statistics/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", class_="restable sortable")

    data = []
    headers = []
    if table:
        # Extract table headers
        th_elements = table.find_all("th")
        headers = [th.text.strip() for th in th_elements]

        # Extract table rows
        rows = table.find_all("tr")
        for row in rows:
            td_elements = row.find_all("td")
            row_data = [td.text.strip() for td in td_elements]
            if row_data:
                data.append(row_data)
    #change to dataframe
    df = pd.DataFrame(data, columns = headers)
    #change column headers
    df.rename(columns = {'':'id', 'Names':'Areas'}, inplace = True)
    #remove bottom row, dirty data
    df.drop(df.tail(2).index,inplace=True)
    df.to_csv('../Airport_Pipeline/Airport_data/geo_countries.csv',index=False)

    for file_name in os.listdir(path):
        current_path = os.path.join(path, file_name)
        
        file_extension = os.path.splitext(file_name)[1]
        
        new_file_name = file_name[:-4] + '_'+ str(today) + file_extension
        
        new_path = os.path.join(path, new_file_name)

        os.rename(current_path, new_path)

    



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

    # Database connection details
    host = 'testtech.postgres.database.azure.com'
    port = '5432'
    database = 'postgres'
    user = 'testtech'
    password = 'George1234'

    # CSV file details
    csv_file_path = 'path_to_your_csv_file.csv'
    table_name = 'your_table_name'

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Open a cursor to perform database operations
    cursor = conn.cursor()

    # Create the table if it doesn't exist (skip this step if the table is already created)
    create_table_query = """
-- Table: countries
CREATE TABLE IF NOT EXISTS countries (
    country_code CHAR(2) NOT NULL,
    country_name VARCHAR(255) NOT NULL,
    continent VARCHAR(255) NOT NULL,
    PRIMARY KEY (country_code)
);

-- Table: regions
CREATE TABLE IF NOT EXISTS regions (
    region_code VARCHAR(4) NOT NULL,
    iso_country CHAR(2) NOT NULL,
    region_name VARCHAR(255) NOT NULL,
    continent VARCHAR(255) NOT NULL,
    PRIMARY KEY (region_code),
    FOREIGN KEY (iso_country) REFERENCES countries (country_code)
);

-- Table: airports
CREATE TABLE IF NOT EXISTS airports (
    airport_id VARCHAR(10) NOT NULL,
    type VARCHAR(255) NOT NULL,
    airport_name VARCHAR(255) NOT NULL,
    latitude_deg DOUBLE NOT NULL,
    longitude_deg DOUBLE NOT NULL,
    elevation_ft INT,
    iso_country CHAR(2) NOT NULL,
    iso_region VARCHAR(4) NOT NULL,
    municipality VARCHAR(255),
    scheduled_service VARCHAR(10),
    gps_code VARCHAR(10),
    iata_code VARCHAR(3),
    local_code VARCHAR(10),
    PRIMARY KEY (airport_id),
    FOREIGN KEY (iso_country) REFERENCES countries (country_code),
    FOREIGN KEY (iso_region) REFERENCES regions (region_code)
);

-- Table: airport_frequencies
CREATE TABLE IF NOT EXISTS airport_frequencies (
    airport_frequencies_id INT NOT NULL,
    airport_id INT NOT NULL,
    freq_type VARCHAR(10),
    description VARCHAR(255),
    frequency_mhz DECIMAL(9,6),
    PRIMARY KEY (airport_frequencies_id),
    FOREIGN KEY (airport_id) REFERENCES airports (airport_id)
);

-- Table: runways
CREATE TABLE IF NOT EXISTS runways (
    runway_id INT NOT NULL,
    airport_id INT NOT NULL,
    length_ft INT,
    width_ft INT,
    surface VARCHAR(255),
    lighted TINYINT(1),
    closed TINYINT(1),
    PRIMARY KEY (runway_id),
    FOREIGN KEY (airport_id) REFERENCES airports (airport_id)
);

-- Table: runways_lower_elevation
CREATE TABLE IF NOT EXISTS runways_le (
    le_id INT NOT NULL,
    runway_id INT NOT NULL,
    le_latitude_deg DOUBLE,
    le_longitude_deg DOUBLE,
    le_elevation_ft INT,
    le_heading_degT INT,
    le_displaced_threshold_ft INT,
    PRIMARY KEY (le_id),
    FOREIGN KEY (runway_id) REFERENCES runways (runway_id)
);

-- Table: runways_higher_elevation
CREATE TABLE IF NOT EXISTS runways_he (
    he_id INT NOT NULL,
    runway_id INT NOT NULL,
    he_latitude_deg DOUBLE,
    he_longitude_deg DOUBLE,
    he_elevation_ft INT,
    he_heading_degT INT,
    he_displaced_threshold_ft INT,
    PRIMARY KEY (he_id),
    FOREIGN KEY (runway_id) REFERENCES runways (runway_id)
);

-- Table: navaids
CREATE TABLE IF NOT EXISTS navaids (
    navaid_id INT PRIMARY KEY ,
    filename VARCHAR(255),
    ident VARCHAR(10),
    type VARCHAR(255),
    name VARCHAR(255),
    frequency_khz INT,
    latitude_deg DOUBLE,
    longitude_deg DOUBLE,
    elevation_ft INT,
    iso_country CHAR(2),
    slaved_variation DOUBLE,
    magnetic_variation DOUBLE,
    usage_type VARCHAR(255),
    power DOUBLE,
    associated_airport_ident VARCHAR(10),
    FOREIGN KEY (associated_airport_ident) REFERENCES airports (airport_id),
    FOREIGN KEY (iso_country) REFERENCES countries (countries_id)
);

--Table: distance-measuring equipment
CREATE TABLE IF NOT EXISTS DME(
    dme_id INT PRIMARY KEY AUTO_INCREMENT,
    navaid_id INT,
    dme_frequency DECIMAL(9,6),
    dme_channel VARCHAR(255),
    dme_latitude_deg DOUBLE,
    dme_longitude_deg DOUBLE,
    dme_elevation_ft INT,
    FOREIGN KEY (navaid_id) REFERENCES navaids (navaid_id)
);

--Table: airport_comments
CREATE TABLE IF NOT EXISTS airport_comments(
    airport_comments_id INT PRIMARY KEY,
    airport_id INT,
    threadRef INT,
    comment_date DATE,
    memberNickname VARCHAR,
    subject VARCHAR,
    body VARCHAR,
    FOREIGN KEY (airport_id) REFERENCES airports (airport_id)
);

--Table: GEO countries
CREATE TABLE IF NOT EXISTS geo_countries(
    geo_countries_id INT PRIMARY KEY,
    areas INT,
    country VARCHAR,
    country_code VARCHAR ,
    Area_in_kmsq VARCHAR,
    Names_per_kmsq VARCHAR,
    population INT,
    Names_per1000_Inhabitants FLOAT,
    FOREIGN KEY (country_code) REFERENCES countries (country_code)
);

-- Table: Geoname
CREATE TABLE IF NOT EXISTS geoname (
    geoname_id INT PRIMARY KEY,
    name VARCHAR(255),
    asciiname VARCHAR(255),
    alternatenames VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    feature_class CHAR(1),
    feature_code VARCHAR(10),
    country_code CHAR(2),
    altcountry_code CHAR(2),
    admin1_code VARCHAR(20),
    admin2_code VARCHAR(80),
    admin3_code VARCHAR(20),
    admin4_code VARCHAR(20),
    population BIGINT,
    elevation INT,
    DEM INT,
    timezone VARCHAR(40),
    modification_date DATE
);
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Import the CSV file into the table
    import_query = f"""
        COPY {table_name}
        FROM '{csv_file_path}'
        DELIMITER ','
        CSV HEADER;
    """
    #cursor.execute(import_query)
    #conn.commit()

    # Close the cursor and database connection
    cursor.close()
    conn.close()




extract_data_sources()
pre_process_transform()

storage_connection_string = ""
#load_blob_storage(storage_connection_string,container_name = "testtech", source_directory = path)
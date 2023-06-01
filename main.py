import os
import zipfile
from pyspark.sql import SparkSession
path = "../Airport_Pipeline/Airport_data"
path2 = "../Airport_Pipeline/Geonames_data"
path3 = '../Airport_Pipeline/Geonames_data/countries.csv'

for i in os.listdir(path):
    os.remove(path +'/' + i)

for k in os.listdir(path2):
    os.remove(path2 +'/' + k)
    
os.system('wget -P ../Airport_Pipeline/Airport_data -i airport.txt')
os.system('wget -P ../Airport_Pipeline/Geonames_data -i geonames.txt')

for k in os.listdir(path2):
    if k.endswith('.zip'):
        with zipfile.ZipFile(path2+ '/'+k) as zf:
            zf.extractall(path2)
        os.remove(path2+ '/'+k)

spark = SparkSession.builder.appName("ReadTextFile").getOrCreate()
header_names = ["geoname_id", "name", "asciiname","alternatenames","latitude","longitude",
    "feature_class","feature_code","country_code","altcountry_code","admin1_code","admin2_code"
    ,"admin3_code","admin4_code","population","elevation","DEM","timezone","modification_date" ]
path4 = "/workspace/Airport_Pipeline/Geonames_data/allCountries.txt"



df = spark.read.option("header", "true").option("delimiter", "\t").csv(path4).toDF(*header_names)

df.coalesce(1).write.mode("overwrite").option("header", "true").csv("../Airport_Pipeline/Geonames_data/countries.csv")

for l in os.listdir(path3):
    if l.endswith('.csv'):
        os.rename(path3 + '/'+ l,'../Airport_Pipeline/Geonames_data/all_countries.csv')



from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("ReadTextFile").getOrCreate()
header_names = ["geoname_id", "name", "asciiname","alternatenames","latitude","longitude",
    "feature_class","feature_code","country_code","altcountry_code","admin1_code","admin2_code"
    ,"admin3_code","admin4_code","population","elevation","DEM","timezone","modification_date" ]
path4 = "/workspace/Airport_Pipeline/Geonames_data/allCountries.txt"



df = spark.read.option("header", "true").option("delimiter", "\t").csv(path4).toDF(*header_names)

df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/workspace/Airport_Pipeline/Geonames_data/countries.csv")

for k in os.listdir(path3):
    if k.endswith('.csv'):
        os.rename(path3 + '/'+ k,'../Airport_Pipeline/Geonames_data/all_countries.csv')


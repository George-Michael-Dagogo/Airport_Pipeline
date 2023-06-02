# Airport_Pipeline

## Business Problem


### The Business is doing a high profile survey to find a suitable area to safe guard the Board of Directors in an event of a global war break out. Some geographical information is required. The Business needs to know the following:


### 1. How many airports, airfields and heliports exist in each country and continent?  :heavy_check_mark:
### 2. What is the average elevation of the airports, airfields and heliports in each country? :heavy_check_mark:
### 3. What is the estimated population of each country? :heavy_check_mark:
### 4. How many cities/towns/settlements in each country? :heavy_check_mark:
### 5. What is the min, max and average elevation of the cities per country? :heavy_check_mark:
### 6. Which are the highest and lowest elevated cities in the world with populations > 100000? :heavy_check_mark:
### 7. Which are the highest and lowest elevated airports, airfields and heliports on the planet? :heavy_check_mark:


# This should be a repeatable solution as many more questions will be derived from the data provided and from the insight gained from the questions above.


## Data sources
### http://download.geonames.org/export/dump/
### Details on geonames can also be found in the link
### https://ourairports.com/data
### Details on ourairports data is found here: https://ourairports.com/help/data-dictionary.html

-------

# Solution 


# Overview
## Both a Data Engineering and a Data Analytics project, 
## I initially addressed the business questions using Pyspark and pandas. You can find the solutions implemented in the following notebook: https://github.com/George-Michael-Dagogo/Airport_Pipeline/blob/main/solutions_with_pandas_%26_pyspark.ipynb. 
## However, I also extended my analysis by answering the same questions using SQL with Snowflake. I accomplished this by creating views that generate tables containing the answers. You can access the solutions implemented in Snowflake through this notebook: https://github.com/George-Michael-Dagogo/Airport_Pipeline/blob/main/solutions_with_snowflake_SQL.ipynb
# .
# Details

## Extraction:
##  I saved the URLs of the necessary data in a text file and utilized the wget library to download all of this data simultaneously into a specific directory. 
## Among these files, some were in zip format, so I extracted the text files from the zip archives and removed the unnecessary zip files.
## The text files were loaded into a dataframe, where appropriate headers were assigned, and subsequently saved as a CSV file.
# .

## Solutions
## 1. Initially, I addressed the business questions using pandas, but as the dataset grew to 12 million rows of data, I extended my analysis to incorporate Pyspark for more efficient processing.

## 2. To expand the scope of the project, I established a Snowflake warehouse that connected to an Azure Blob storage. Within Snowflake, I created the necessary databases and tables to store the data. Handling the large dataset posed challenges; utilizing pandas proved to be slow, taking up to 30 minutes for processing. Even after partitioning the data, the performance did not improve. Attempting to leverage Pyspark to push the dataframe resulted in errors.

## 3. The most effective solution, which could be automated, involved staging the data first—a beneficial feature of Snowflake—before pushing it. However, during this process, some rows encountered errors. To circumvent this issue, I employed the "ON_ERROR=CONTINUE" command, enabling the skipping of problematic rows.

## 4. To enhance accessibility and streamline the Python code, I created views of the solutions on Snowflake. These views allow for easy retrieval of the desired data and contribute to a cleaner and more organized approach when invoking them in Python. By encapsulating the complex queries and transformations within the Snowflake views, the Python code can focus on utilizing the results efficiently, leading to improved code readability and maintainability.


## Data model for the airport/geonames database
![alt text](https://github.com/George-Michael-Dagogo/Airport_Pipeline/blob/main/airpot_database_model.jpg)

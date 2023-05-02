# Airport_Pipeline
-------
## Business Problem


### The Business is doing a high profile survey to find a suitable area to safe guard the Board of Directors in an event of a global war break out. Some geographical information is required. The Business needs to know the following:
-------

### 1. How many airports, airfields and heliports exist in each country and continent?
### 2. What is the average elevation of the airports, airfields and heliports in each country?
### 3. What is the estimated population of each country?
### 4. How many cities/towns/settlements in each country?
### 5. What is the min, max and average elevation of the cities per country?
### 6. Which are the highest and lowest elevated cities in the world with populations > 100000?
### 7. Which are the highest and lowest elevated airports, airfields and heliports on the planet?

-------
# This should be a repeatable solution as many more questions will be derived from the data provided and from the insight gained from the questions above.

-------
## Data sources
### http://download.geonames.org/export/dump/
### https://ourairports.com/data
### Details on ourairports data is found here: https://ourairports.com/help/data-dictionary.html


-------
# Solution 

## Data model for thie airport database
![alt text](https://github.com/George-Michael-Dagogo/Airport_Pipeline/blob/main/airport_data_model.jpg)

## Files :
-------
### XX.zip                   : features for country with iso code XX, see 'geoname' table for columns.'no-country' for features not belonging to a country.
### allCountries.zip         : all countries combined in one file, see 'geoname' table for columns
### cities500.zip            : all cities with a population > 500 or seats of adm div down to PPLA4 (ca 185.000), see 'geoname' table for columns
### cities1000.zip           : all cities with a population > 1000 or seats of adm div down to PPLA3 (ca 130.000), see 'geoname' table for columns
### cities5000.zip           : all cities with a population > 5000 or PPLA (ca 50.000), see 'geoname' table for columns
### cities15000.zip          : all cities with a population > 15000 or capitals (ca 25.000), see 'geoname' table for columns
### alternateNamesV2.zip     : alternate names with language codes and geonameId, file with iso language codes, with new columns from and to
### alternateNames.zip       : obsolete use V2, this file does not have the new columns to and from and will be removed in the future
### admin1CodesASCII.txt     : names in English for admin divisions. Columns: code, name, name ascii, geonameid
### admin2Codes.txt          : names for administrative subdivision 'admin2 code' (UTF8), Format : concatenated codes <tab>name <tab> asciiname <tab> geonameId
### iso-languagecodes.txt    : iso 639 language codes, as used for alternate names in file alternateNames.zip
### featureCodes.txt         : name and description for feature classes and feature codes 
### timeZones.txt            : countryCode, timezoneId, gmt offset on 1st of January, dst offset to gmt on 1st of July (of the current year), rawOffset without DST
### countryInfo.txt          : country information : iso codes, fips codes, languages, capital ,...
                          
### modifications-<date>.txt : all records modified on the previous day, the date is in yyyy-MM-dd format. You can use this file to daily synchronize your own geonames database.
### deletes-<date>.txt       : all records deleted on the previous day, format : geonameId <tab> name <tab> comment.

### alternateNamesModifications-<date>.txt : all alternate names modified on the previous day,
### alternateNamesDeletes-<date>.txt       : all alternate names deleted on the previous day, format : alternateNameId <tab> geonameId <tab> name <tab> comment.
### userTags.zip		: user tags , format : geonameId <tab> tag.
### hierarchy.zip		: parentId, childId, type. The type 'ADM' stands for the admin hierarchy modeled by the admin1-4 codes. The other entries are entered with the user interface. The relation toponym-adm hierarchy is not included in the file, it can instead be built from the admincodes of the toponym.
### adminCode5.zip		: the new adm5 column is not yet exported in the other files (in order to not break import scripts). Instead it is availabe as separate file. columns: geonameId,adm5code

### The main 'geoname' table has the following fields :
---------------------------------------------------
### geonameid         : integer id of record in geonames database
### name              : name of geographical point (utf8) varchar(200)
### asciiname         : name of geographical point in plain ascii characters, varchar(200)
### alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
### latitude          : latitude in decimal degrees (wgs84)
### longitude         : longitude in decimal degrees (wgs84)
### feature class     : see http://www.geonames.org/export/codes.html, char(1)
### feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
### country code      : ISO-3166 2-letter country code, 2 characters
### cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
### admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
### admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
### admin3 code       : code for third level administrative division, varchar(20)
### admin4 code       : code for fourth level administrative division, varchar(20)
### population        : bigint (8 byte int) 
### elevation         : in meters, integer
### dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
### timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
### modification date : date of last modification in yyyy-MM-dd format
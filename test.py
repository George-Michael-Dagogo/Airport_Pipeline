import requests
from bs4 import BeautifulSoup
import pandas as pd
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

df = pd.DataFrame(data, columns = headers)
df.rename(columns = {'':'id', 'Names':'Areas'}, inplace = True)
df.drop(df.tail(2).index,inplace=True)
print(df)
#df.to_csv('../Airport_Pipeline/Airport_data/geo_countries.csv',index=False)

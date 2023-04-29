import os
path = "../Airport_Pipeline/Airport_data"
for i in os.listdir(path):
    os.remove(path +'/' +i)
    
os.system('wget -P ../Airport_Pipeline/Airport_data https://davidmegginson.github.io/ourairports-data/airports.csv')
os.system('wget -P ../Airport_Pipeline/Airport_data https://davidmegginson.github.io/ourairports-data/airport-frequencies.csv')
os.system('wget -P ../Airport_Pipeline/Airport_data https://davidmegginson.github.io/ourairports-data/airport-comments.csv')
os.system('wget -P ../Airport_Pipeline/Airport_data https://davidmegginson.github.io/ourairports-data/runways.csv')
os.system('wget -P ../Airport_Pipeline/Airport_data https://davidmegginson.github.io/ourairports-data/navaids.csv')
os.system('wget -P ../Airport_Pipeline/Airport_data https://davidmegginson.github.io/ourairports-data/countries.csv')
os.system('wget -P ../Airport_Pipeline/Airport_data https://davidmegginson.github.io/ourairports-data/regions.csv')
#/Airport_Pipeline/Airport_data

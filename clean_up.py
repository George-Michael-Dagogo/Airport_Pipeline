import os

path = "../Airport_Pipeline/Airport_data"
path2 = "../Airport_Pipeline/Geonames_data"
for i in os.listdir(path):
    os.remove(path +'/' + i)

for k in os.listdir(path2):
    os.remove(path2 +'/' + k)
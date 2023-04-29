import os 
import zipfile 
path2 = "../Airport_Pipeline/Geonames_data"
for k in os.listdir(path2):
    if k.endswith('.zip'):
        with zipfile.ZipFile(path2+ '/'+k) as zf:
            zf.extractall(path2)
        os.remove(path2+ '/'+k)

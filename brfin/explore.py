import pandas as pd
import io
file_path = '/home/crcj/GitHub/BrFin/data/processed/dataset.pkl'
with open(file_path, 'rb') as f:
    myFile = io.BytesIO(f.read())

df = pd.read_pickle(myFile, compression='zstd')
print(df)


df = pd.read_pickle(myFile, compression='zstd')
print(df)
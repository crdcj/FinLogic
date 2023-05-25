from pathlib import Path
import pandas as pd

DATA_PATH = Path(__file__).parent / "data"
DF_PATH = DATA_PATH / "main_df.pickle"
Path.mkdir(DATA_PATH, parents=True, exist_ok=True)

if DF_PATH.is_file():
    DF = pd.read_pickle(DF_PATH, compression="zstd")
else:
    DF = pd.DataFrame()

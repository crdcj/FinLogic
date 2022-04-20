from pathlib import Path
import pandas as pd

base_dir = Path(__file__).parent
DATA_PATH = base_dir / "data"
MAIN_DF_PATH = DATA_PATH / "main_df.pkl.zst"
CVM_DF_PATH = DATA_PATH / "cvm_df.pkl"

# Start/load main dataframe
if MAIN_DF_PATH.is_file():
    main_df = pd.read_pickle(MAIN_DF_PATH)
else:
    main_df = pd.DataFrame()

# Start/load CVM files data
if CVM_DF_PATH.is_file():
    cvm_df = pd.read_pickle(CVM_DF_PATH)
else:
    columns = ["filename", "file_size", "etag", "last_modified"]
    cvm_df = pd.DataFrame(columns=columns)

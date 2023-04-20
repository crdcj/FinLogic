from pathlib import Path
import pandas as pd

base_dir = Path(__file__).parent
DATA_PATH = base_dir / "data"
FINLOGIC_DF_PATH = DATA_PATH / "finlogic_df.pkl.zst"
CVM_DF_PATH = DATA_PATH / "cvm_df.pkl"

LANGUAGE_DF_PATH = DATA_PATH / "interim/pten_df.csv.zst"
URL_LANGUAGE = "https://raw.githubusercontent.com/fe-lipe-c/finlogic_datasets/master/data/pten_df.csv"  # noqa

RAW_DIR = DATA_PATH / "raw"
PROCESSED_DIR = DATA_PATH / "processed"
INTERIM_DIR = DATA_PATH / "interim"

CHECKMARK = "\033[32m\u2714\033[0m"


# Start/load main dataframe
if FINLOGIC_DF_PATH.is_file():
    finlogic_df = pd.read_pickle(FINLOGIC_DF_PATH)
else:
    finlogic_df = pd.DataFrame()

# Start/load CVM files data
if CVM_DF_PATH.is_file():
    cvm_df = pd.read_pickle(CVM_DF_PATH)
else:
    columns = ["filename", "file_size", "etag", "last_modified"]
    cvm_df = pd.DataFrame(columns=columns)

# Start/load language file data
if LANGUAGE_DF_PATH.is_file():
    language_df = pd.read_csv(LANGUAGE_DF_PATH)
else:
    language_df = pd.DataFrame()

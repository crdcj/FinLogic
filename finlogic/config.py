from pathlib import Path
import pandas as pd
import duckdb as ddb

base_dir = Path(__file__).parent
DATA_PATH = base_dir / "data"
RAW_DIR = DATA_PATH / "raw"
INTERIM_DIR = DATA_PATH / "interim"
FINLOGIC_DB_PATH = DATA_PATH / "finlogic.db"
LANGUAGE_DF_PATH = DATA_PATH / "interim/pten_df.csv.zst"
URL_LANGUAGE = "https://raw.githubusercontent.com/fe-lipe-c/finlogic_datasets/master/data/pten_df.csv"  # noqa
CHECKMARK = "\033[32m\u2714\033[0m"

con = ddb.connect(database=f"{FINLOGIC_DB_PATH}")
# Create data folders if they do not exist.
Path.mkdir(RAW_DIR, parents=True, exist_ok=True)

# Start/load language file data
if LANGUAGE_DF_PATH.is_file():
    language_df = pd.read_csv(LANGUAGE_DF_PATH)
else:
    language_df = pd.DataFrame()

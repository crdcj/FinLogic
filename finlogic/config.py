from pathlib import Path
import pandas as pd

DATA_PATH = Path(__file__).parent / "data"
INTERIM_DIR = DATA_PATH / "interim"
LANGUAGE_DF_PATH = DATA_PATH / "interim/pten_df.csv"
URL_LANGUAGE = "https://raw.githubusercontent.com/fe-lipe-c/finlogic_datasets/master/data/pten_df.csv"  # noqa
CHECKMARK = "\033[32m\u2714\033[0m"

# Create interim folder if itdoes not exist.
Path.mkdir(INTERIM_DIR, parents=True, exist_ok=True)

# Start/load language file data
if LANGUAGE_DF_PATH.is_file():
    language_df = pd.read_csv(LANGUAGE_DF_PATH)
else:
    language_df = pd.DataFrame()

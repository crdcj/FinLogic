from pathlib import Path
import pandas as pd
from . import config as cfg

INTERIM_DIR = cfg.DATA_PATH / "interim"
LANGUAGE_DF_PATH = INTERIM_DIR / "pten_df.csv"
URL_LANGUAGE = "https://raw.githubusercontent.com/fe-lipe-c/finlogic_datasets/master/data/pten_df.csv"  # noqa

# Create interim folder if itdoes not exist.
Path.mkdir(INTERIM_DIR, parents=True, exist_ok=True)

# Start/load language file data
if LANGUAGE_DF_PATH.is_file():
    language_df = pd.read_csv(LANGUAGE_DF_PATH)
else:
    language_df = pd.DataFrame()


def process_language_df():
    """Process language dataframe."""
    language_df = pd.read_csv(URL_LANGUAGE)
    language_df.to_csv(LANGUAGE_DF_PATH, index=False)

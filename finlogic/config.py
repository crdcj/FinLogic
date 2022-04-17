import os
from pathlib import Path
import pandas as pd

base_dir = Path(__file__).parent
DATA_PATH = base_dir / "data"
MAIN_DF_PATH = DATA_PATH / "main_df.pkl.zst"

if os.path.isfile(MAIN_DF_PATH):
    main_df = pd.read_pickle(MAIN_DF_PATH)
else:
    # Boolean value of an empty dataframe is False
    main_df = pd.DataFrame()

import os
import pandas as pd

script_dir = os.path.dirname(__file__)
DIR_DATA = script_dir + "/data"
PATH_MAIN_DF = DIR_DATA + "/main_df.pkl.zst"

if os.path.isfile(PATH_MAIN_DF):
    main_df = pd.read_pickle(PATH_MAIN_DF)
else:
    # Boolean value of an empty dataframe is False
    main_df = pd.DataFrame()

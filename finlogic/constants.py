import os
import pandas as pd

script_dir = os.path.dirname(__file__)
DIR_DATA = script_dir + "/data/"
DIR_RAW = DIR_DATA + "raw/"
DIR_PROCESSED = DIR_DATA + "processed/"
PATH_MAIN_DF = DIR_DATA + "main_df.pkl.zst"
if os.path.isfile(PATH_MAIN_DF):
    MAIN_DF = pd.read_pickle(PATH_MAIN_DF)
else:
    # Boolean value of an empty dataframe is False
    MAIN_DF = pd.DataFrame()

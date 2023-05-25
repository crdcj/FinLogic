from pathlib import Path

DATA_PATH = Path(__file__).parent / "data"
DF_PATH = DATA_PATH / "main_df.pickle"
Path.mkdir(DATA_PATH, parents=True, exist_ok=True)

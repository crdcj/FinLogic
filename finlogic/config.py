from pathlib import Path

DATA_PATH = Path(__file__).parent / "data"
REPORTS_PATH = DATA_PATH / "reports.pickle"
Path.mkdir(DATA_PATH, parents=True, exist_ok=True)

CVM_RAW_DIR = DATA_PATH / "cvm" / "raw"
CVM_PROCESSED_DIR = DATA_PATH / "cvm" / "processed"
# Create CVM folders if they do not exist
Path.mkdir(CVM_RAW_DIR, parents=True, exist_ok=True)
Path.mkdir(CVM_PROCESSED_DIR, parents=True, exist_ok=True)

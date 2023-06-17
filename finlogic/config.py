from pathlib import Path
import pandas as pd

DATA_PATH = Path(__file__).parent / "data"
REPORTS_PATH = DATA_PATH / "reports.pickle"
INDICATORS_PATH = DATA_PATH / "indicators.pickle"
Path.mkdir(DATA_PATH, parents=True, exist_ok=True)

CVM_RAW_DIR = DATA_PATH / "cvm" / "raw"
CVM_PROCESSED_DIR = DATA_PATH / "cvm" / "processed"
# Create CVM folders if they do not exist
Path.mkdir(CVM_RAW_DIR, parents=True, exist_ok=True)
Path.mkdir(CVM_PROCESSED_DIR, parents=True, exist_ok=True)

# Load last session data
URL_LAST_SESSION = (
    "https://raw.githubusercontent.com/crdcj/FinLogic/main/data/last_session_data.csv"
)
LAST_SESSION_DF = pd.read_csv(URL_LAST_SESSION)
LISTED_COMPANIES = sorted(LAST_SESSION_DF["cvm_id"])

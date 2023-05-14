from pathlib import Path

DATA_PATH = Path(__file__).parent / "data"

CVM_RAW_DIR = DATA_PATH / "cvm" / "raw"
CVM_PROCESSED_DIR = DATA_PATH / "cvm" / "processed"
# Create CVM folders if it does not exist
Path.mkdir(CVM_RAW_DIR, parents=True, exist_ok=True)
Path.mkdir(CVM_PROCESSED_DIR, parents=True, exist_ok=True)

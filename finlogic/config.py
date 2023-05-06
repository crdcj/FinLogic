from pathlib import Path
import duckdb as ddb

DATA_PATH = Path(__file__).parent / "data"
# Create data folder if it does not exist.
Path.mkdir(DATA_PATH, parents=True, exist_ok=True)

# Start FinLogic Database connection
FINLOGIC_DB_PATH = DATA_PATH / "finlogic.db"
fldb = ddb.connect(database=f"{FINLOGIC_DB_PATH}")

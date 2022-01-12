import importlib
import cvm as cvm
importlib.reload(cvm)

files_updated = cvm.update_raw_dataset()
print(files_updated)
import importlib
import cvm
importlib.reload(cvm)

files_updated = cvm.update_dataset()
print(files_updated)

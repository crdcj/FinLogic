import importlib
import dataset

importlib.reload(dataset)

urls = dataset.list_urls()
urls = dataset.update_raw_dataset()
# for url in urls:
#     print(url)
dataset.update_processed_dataset()

import importlib
import dataset

importlib.reload(dataset)

# urls = dataset.list_urls()
# for url in urls:
#     print(url)

urls = dataset.update_raw_dataset()
for url in urls:
    print(url)

dataset.update_processed_dataset()


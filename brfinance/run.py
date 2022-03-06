import importlib
import dataset

importlib.reload(dataset)

print('')
print('Updating dataset ...')
# urls = dataset.list_urls()
# urls = dataset.update_raw_dataset()
# for url in urls:
#     print(url)
print('')
print('Processing dataset ...')
dataset.update_processed_dataset()

import pandas as pd
import database

time1 = pd.Timestamp.now()
# database.update_database()
database.update_database(reset_data=True)
# print(database.database_info())
print(database.search_company('petro'))
delta = round((pd.Timestamp.now() - time1).total_seconds(), 1)
print('total time =', delta)

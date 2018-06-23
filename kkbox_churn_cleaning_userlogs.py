import numpy as np
import pandas as pd
import sqlalchemy

# -Show dataStructure
#data = pd.read_csv("sample_submission_zero.csv", nrows=10)
#print(data)

# -Creat engine!!!
csv_database = sqlalchemy.create_engine('sqlite:///csv_database.db', echo=True)
# ==================================================
#SQL Alchemy SQLITE relative path
# Relative path is the path 'raw' after the three initial slashses
#e = create_engine('sqlite:///path/to/database.db')
# Absolute path is a slash after the three initial slashses
#e = create_engine('sqlite:////tmp/absolute_path_database.db')
# ==================================================

# =====
# https://stackoverflow.com/questions/39053628/open-selected-rows-with-pandas-using-chunksize-and-or-iterator
# http://pythondata.com/working-large-csv-files-python/
# ====
chunksize = 50
#i = 0
#j = 1

#pd.DataFrame(np.random.randn(30, 3), columns=list('abc')).to_csv('fn.csv', index=False)
#for chunk in pd.read_csv('fn.csv', chunksize=10):
#    print(chunk)
#    chunk.to_sql('test',csv_database,if_exists='append')
#sample_submission_zero.csv
for df in pd.read_csv("./kkboxData/user_logs.csv", chunksize=chunksize, iterator=True):
     #df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
#     df.index += j
#     i+=1
    df.to_sql('test', csv_database, if_exists='append')
#      j = df.index[-1] + 1
#    print(df)
#df = pd.read_sql_query('SELECT msno FROM test', csv_database)
#print(df)

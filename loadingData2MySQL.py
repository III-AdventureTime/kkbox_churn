import pandas as pd
df = pd.read_csv('members_v3.csv')
df.head()

df.loc[:,'gender']=df.loc[:,'gender'].fillna('nan')

#dfLess = df.loc[4000:8000]
#print(len(dfLess))

import sqlalchemy
print(sqlalchemy.__version__)
# connection string
# mysql+pymysql://<username>:<password>@<host>:<port>/<database>?charset=utf8mb4
mysqlURL = "10.120.27.103"
database_name = "kkbox"
table_name = "members"

engineSQL = sqlalchemy.create_engine("mysql+pymysql://root:123456@{}:3306".format(mysqlURL))
engineSQL.execute("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

engineSQL = sqlalchemy.create_engine("mysql+pymysql://root:123456@{}:3306/{}".format(mysqlURL,database_name))

engineSQL.execute("drop table if exists {}".format(table_name))

#Declare a Mapping
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, String, DateTime, func
class Members(Base):
    __tablename__ = 'members'
    id = Column(Integer, primary_key=True)
    msno   = Column(String(44),unique=True)
    city   = Column(Integer)
    bd     = Column(Integer)
    gender = Column(String(6))
    registered_via = Column(Integer)
    registration_init_time = Column(DateTime)
    
#     def __repr__(self):
#        return "<Members(name='%s', fullname='%s', password='%s')>" % (self.name, self.fullname, self.password)

#we can use MetaData to issue CREATE TABLE statements to the database for all tables that don’t yet exist. 
Base.metadata.create_all(engineSQL)




#Creating a Session
import time
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engineSQL)
session = Session()


#Create an Instance of the Mapped Class
for index, row in df.iterrows():
    member = Members(id=index+1, msno=row['msno'], city=row['city'], bd=row['bd'], gender=row['gender'], 
                     registered_via=row['registered_via'], registration_init_time=row['registration_init_time'])
    session.add(member)
    if (index+1) % 1000 == 0:
        session.commit()
        # time.sleep(0.2)
        print(index+1)
session.commit()


print(session.query(func.count('*')).select_from(Members).scalar())
session.close()

import sqlalchemy
print(sqlalchemy.__version__)
# connection string
# mysql+pymysql://<username>:<password>@<host>:<port>/<database>?charset=utf8mb4
mysqlURL = "localhost"
database_name = "kkbox"
table_name = "userlog"

engineSQL = sqlalchemy.create_engine("mysql+pymysql://root:123456@{}:3306".format(mysqlURL))
engineSQL.execute("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

engineSQL = sqlalchemy.create_engine("mysql+pymysql://root:123456@{}:3306/{}".format(mysqlURL,database_name))

engineSQL.execute("drop table if exists {}".format(table_name))

#Declare a Mapping
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, String, DateTime, Float
class Userlog(Base):
    __tablename__ = 'userlog'
    id = Column(Integer, primary_key=True)
    msno   = Column(String(44))
    date   = Column(DateTime)
    num25  = Column(Integer)
    num50  = Column(Integer)
    num75  = Column(Integer)
    num985 = Column(Integer)
    num100 = Column(Integer)
    numunq = Column(Integer)
    totalsecs = Column(Float)
    
#     def __repr__(self):
#        return "<Members(name='%s', fullname='%s', password='%s')>" % (self.name, self.fullname, self.password)

#we can use MetaData to issue CREATE TABLE statements to the database for all tables that don’t yet exist. 
Base.metadata.create_all(engineSQL)


#Creating a Session
import time
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engineSQL)
session = Session()


import csv
with open('../kkboxData/user_logs.csv', newline='') as f:
    reader = csv.reader(f)
    next(reader)
    for index, row in enumerate(reader):
        userlog = Userlog(id=index+1, msno=row[0], date=row[1], num25=row[2], num50=row[3], num75=row[4], num985=row[5], num100=row[6], numunq=row[7], totalsecs = row[8])
        session.add(userlog)
        if (index+1) % 10000 == 0:
            session.commit()
            print(index+1)
session.commit()


#    for row in reader:
#        print(row)
#    row1 = next(reader)
#    print(row1)
#    row1 = next(reader)
#    print(row1)

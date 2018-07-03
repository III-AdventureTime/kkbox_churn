# kkbox

### kaggle data download

To use the Kaggle API, sign up for a Kaggle account at [https://www.kaggle.com](https://www.kaggle.com/). Then go to the `Account` tab of your user profile (`https://www.kaggle.com/<username>/account`) and select `'Create API Token`. This will trigger the download of `kaggle.json`, a file containing your API credentials. Place this file in the location `~/.kaggle/kaggle.json` (on Windows in the location `C:\Users\<Windows-username>\.kaggle\kaggle.json`). 

For your security, ensure that other users of your computer do not have read access to your credentials. On Unix-based systems you can do this with the following command:

`chmod 600 ~/.kaggle/kaggle.json`

Downloading data by kaggle command line

`kaggle competitions download -c kkbox-churn-prediction-challenge `

Install 7-zip

`sudo apt-get install p7zip-full`

unzip 7-zip

`7z x file.7z`


### Upload kkbox data to hdfs
Create folder kkbox/kkbox_data:

`hadoop fs -mkdir kkbox`

`hadoop fs -mkdir kkbox/kkbox_data`

put data to hdfs:

`hadoop fs -put <file> kkbox/kkbox_data`





### Open pyspark

`pyspark --master spark://10.120.27.103:7077`



### Clearning Data(train.csv)

- ##### load data

  `rowdatawithheader = sc.textFile("hdfs://master:9000/user/master/kkbox/kkbox_data/train.csv")`

- ##### remove header

  `header = rowdatawithheader.first()`

  `rowdata = rowdatawithheader.filter(lambda line: line != header)`

- ##### check "is_churn" data range 

  `lines = rowdata.map(lambda x:x.split(','))`

  `lines.map(lambda fie:fie[1]).distinct().collect()` 

- ##### and check the number of  "is_churn" data  

  `rowdata.count()`

  `rowdata.map(lambda x:x.split(',')[1]).filter(lambda x:x=='1').count()`

  `rowdata.map(lambda x:x.split(',')[1]).filter(lambda x:x=='0').count()`

- #### check "id" data

  `rowdata.map(lambda x:(len(x.split(',')[0]))).filter(lambda x:x==44).count()`



### Clearning Data(user_logs.csv)

`data = sc.textFile("hdfs://master:9000//kkbox_churn/raw_data/user_logs.csv")`





### Clearning Data(memvers_v3.csv)

> 'msno,city,bd,gender,registered_via,registration_init_time'

`rowdatawithheader = sc.textFile("hdfs://master:9000/user/master/kkbox/kkbox_data/members_v3.csv")`

`header = rowdatawithheader.first()`

`rowdata = rowdatawithheader.filter(lambda line: line != header)`

`lines = rowdata.map(lambda x:x.split(','))`



- How to impute missing values for a variable like Gender?

    > Based on the problem at hand, we can try to do one of the following:
    >
    > 1. Mode is one of the option which can be used
    > 2. Missing values can be treated as a separate category by itself. We can create another category for the missing values and use them as a different level
    > 3. If the number of missing values are lesser compared to the number of samples and also the total number of samples is high, we can also choose to remove those rows in our analysis
    > 4. We can also try to do an imputation based on the values of other variables in the given dataset. We can identify related rows to the given row and then use them for imputation
    > 5. We can also run a model to predict the missing values using all other variables as inputs.
    >
    > Depending on the nature of the problem and the dataset, we could choose to use the one that seems more suitable.



### Import data into MySQL (However if the rows > 100m, then GO SPARK!!!!)

- ###  Run a MySQL docker

  ```docker run --rm --name mysql -p 3306:3306 -v ~/Code/kkbox/mysqlDb:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.7.22```

- #### Huge dataset(>1G), via Python with open()

  ```python
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
  
  #we can use MetaData to issue CREATE TABLE statements to the database for all tables that don’t yet exist.
  Base.metadata.create_all(engineSQL)
  
  #Creating a Session
  from sqlalchemy.orm import sessionmaker
  Session = sessionmaker(bind=engineSQL)
  session = Session()
  
  import csv
  with open('../kkboxData/user_logs.csv', newline='') as f:
      reader = csv.reader(f)
      next(reader)
      for index, row in enumerate(reader):
          userlog = Userlog(id=index+1, msno=row[0], date=row[1], num25=row[2], num50=row[3], 					num75=row[4], num985=row[5], num100=row[6], numunq=row[7], totalsecs = 						row[8])
          session.add(userlog)
          # The maximum rows per insert should be 100,000
          if (index+1) % 10000 == 0:
              session.commit()
              print(index+1)
  session.commit()
  ```




- #### Dataset(<100M), via Python Pandas or 

    ```python
    import sqlalchemy
    conn = sqlalchemy.create_engine("mysql+pymysql://root:123456@localhost:3306/dbName?charset=utf8mb4")
    df.to_sql('rightpet', conn, if_exists='replace')
    ```


- #### Via LOAD DATA INFILE

  If you’re looking for raw performance, this is indubitably your solution of choice. `LOAD DATA INFILE` is a highly optimized, MySQL-specific statement that directly inserts data into a table from a CSV / TSV file.

  There are two ways to use `LOAD DATA INFILE`. You can copy the data file to the server's data directory (typically `/var/lib/mysql-files/`) and run:

  ```
  LOAD DATA INFILE '/path/to/products.csv' INTO TABLE products;
  ```

  This is quite cumbersome as it requires you to have access to the server’s filesystem, set the proper permissions, etc.

  The good news is, you can also store the data file *on the client side*, and use the `LOCAL` keyword:

  ```
  LOAD DATA LOCAL INFILE '/path/to/products.csv' INTO TABLE products;
  ```

  In this case, the file is read from the client’s filesystem, transparently copied to the server’s temp directory, and imported from there. All in all, **it’s almost as fast as loading from the server’s filesystem directly**. You do need to ensure that this [option](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_local_infile) is enabled on your server, though.

  There are many options to `LOAD DATA INFILE`, mostly related to how your data file is structured (field delimiter, enclosure, etc.). Have a look at the [documentation](https://dev.mysql.com/doc/refman/5.7/en/load-data.html) to see them all.

  While `LOAD DATA INFILE` is your best option performance-wise, it requires you to have your data ready as delimiter-separated text files. If you don’t have such files, you’ll need to spend additional resources to create them, and will likely add a level of complexity to your application. Fortunately, there’s an alternative.



- #### Set MySQL maximum allowed packet variable

  To view what the default value is for max_allowed_packet variable, execute the following command in in MySQL:

  `show variables like 'max_allowed_packet';`

  Standard MySQL installation has a default value of 1048576 bytes (1MB). This can be increased by setting it to a higher value for a session or connection.

  This sets the value to 500MB for everyone (that's what GLOBAL means):

  `SET GLOBAL max_allowed_packet=524288000;`

  check your change in new terminal with new connection:

  `show variables like 'max_allowed_packet';`

  





## **Troubleshooting** 

#### [Randomness of hash of string should be disabled via PYTHONHASHSEED](https://stackoverflow.com/questions/36798833/what-does-exception-randomness-of-hash-of-string-should-be-disabled-via-pythonh)

> if sys.version >= '3.3' and 'PYTHONHASHSEED' not in os.environ:    raise Exception("Randomness of hash of string should be disabled via PYTHONHAdSHSEED")

- Solve1(Not working):

  `echo "export PYTHONHASHSEED=0" >> /root/.bashrc`

  `echo "spark.executorEnv.PYTHONHASHSEED=0" >> spark-defaults.conf`

- Solve2(Not working):

  upgrade spark to above spark 2.2.0 
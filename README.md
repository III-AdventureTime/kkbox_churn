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



### Run a MySQL docker

`docker run --rm --name mysql -p 3306:3306 -v ~/Code/kkbox/mysqlDb:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.7.22`







### Import data into MySQL

- #### Via Python with open()

   

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


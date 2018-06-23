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


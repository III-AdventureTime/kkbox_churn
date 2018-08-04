from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    dir0 = 'file:///home/cloudera/2.kkbox_churn/data01/big_table_01/'
    infile = dir0 + 'all_features.cleaned'
    
    ##
    spark = SparkSession.builder.getOrCreate()
    
    df0 = spark.read.format('parquet').load(infile)
    df1 = df0.where(col('has_expired') & ~col('auto_renew_mode'))
    
    df1.write.format('parquet').save(dir0 + 'all-features_has-expired_auto-renew-0')
    
## END ##


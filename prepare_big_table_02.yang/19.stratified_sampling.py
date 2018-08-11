from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import re

if __name__ == "__main__":
    dir0 = 'file:///home/cloudera/2.kkbox_churn/data01/big_table_01/'
    infile = dir0 + '12.big_table_clean2'
    
    filter_has_expired = True
    sample_ratio = 0.8
    use = 'my_is_churn'  # 'my_is_churn' or 'is_churn'

    #
    subdir = 'has_expired/' if filter_has_expired else ''
    outfile_train = dir0 + subdir + '19a.train_set'
    outfile_test = dir0 + subdir + '19b.test_set'


    ##
    spark = SparkSession.builder.getOrCreate()
    df0 = spark.read.format('parquet').load(infile)
    
    ##
    if filter_has_expired:
        df0 = df0.where('has_expired')
    
    ##
    df_train = df0.sampleBy(use, {True: sample_ratio, False: sample_ratio}, seed=101207)
    df_test = df0.join(df_train, df0['msno']==df_train['msno'], 'left_anti')
    
    df_train.write.format('parquet').save(outfile_train)
    df_test.write.format('parquet').save(outfile_test)
    
## END ##


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, udf, lit

if __name__ == "__main__":
    dir0 = '/home/cloudera/2.kkbox_churn/data01/big_table_01/has-expired_auto-renew-0/'
    infile = dir0 + '00.data'

    outfile_has_record = dir0 + 'last-1-week_has-record'
    outfile_no_record = dir0 + 'last-1-week_no-record'
    
    ##
    spark = SparkSession.builder.getOrCreate()
    df0 = spark.read.format('parquet').load(infile)

    ##
    df1 = df0.where('last1WeekOnLine > 0').where('w1_Mtotal > 0')
    df1.write.format('parquet').save(outfile_has_record)
    
    ##
    df2 = df0.where('last1WeekOnLine = 0').where('w1_Mtotal = 0')
    df2.write.format('parquet').save(outfile_no_record)
    
    

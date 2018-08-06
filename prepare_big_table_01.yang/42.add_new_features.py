from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, round, lit

if __name__ == "__main__":
    dir0 = '/home/cloudera/2.kkbox_churn/data01/big_table_01/has-expired_auto-renew-0/'
    subdir = 'last-1-week_has-record/'
    infile = dir0 + subdir + '00.data'
    outfile = dir0 + subdir + '01.added_features'

    ##
    spark = SparkSession.builder.getOrCreate()
    df0 = spark.read.format('parquet').load(infile)

    df1 = df0.withColumn('last1WeekOnLine_D_last12WeekOnLine',
                         round(col('last1WeekOnLine')/col('last12WeekOnLine'), 2)) \
             .withColumn('w1_Mtotal_D_w12_Mtotal',
                         round(col('w1_Mtotal')/col('w12_Mtotal'), 2))
    
    ##
    df1.write.format('parquet').save(outfile)
    
    

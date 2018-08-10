from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import re

        
if __name__ == "__main__":
    dir0 = 'file:///home/cloudera/2.kkbox_churn/data01/big_table_01/'
    infile = dir0 + '11.all_features_clean1'
    outfile = dir0 + '12.big_table_clean2'

    ##
    spark = SparkSession.builder.getOrCreate()
    df0 = spark.read.format('parquet').load(infile)
    
    ## correct lastXXWeekOnLine
    onlineDays = {}
    for ww in ['1', '2', '4', '6', '9', '12']:
        c = when(col('w'+ww+'_Mtotal')==0, 0) \
            .otherwise(col('last'+ww+'WeekOnLine')) \
            .alias('last'+ww+'WeekOnLine')
        onlineDays[ww] = c

    cols = []
    for c in df0.columns:
        m = re.match('last(\d+)WeekOnLine', c)
        if m:
            cols.append(onlineDays[m.groups()[0]])
        #elif c == 'is_churn':
        #    c2 = when(col('is_churn')=='1', True).otherwise(False).alias('is_churn')
        #    cols.append(c2)
        else:
            cols.append(c)

    df1 = df0.select(*cols)
    
    ## output
    df1.write.format('parquet').save(outfile)
    
## END ##


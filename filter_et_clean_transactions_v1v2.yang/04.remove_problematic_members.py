from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, monotonically_increasing_id

if __name__ == "__main__":
    dir0 = 'file:///home/cloudera/2.kkbox_churn/data01/from_raw_transactions-v1+v2/'
    infile = dir0 + '03.clean3_dates_corrections'
    outfile = dir0 + '04.clean4_probl_members'
    
    ##
    spark = SparkSession.builder.getOrCreate()
    
    ##
    df0 = spark.read.format('csv').option('header','true').load(infile)
      # columns:  msno, pay_method, list_price, actual_paid, is_auto_renew, is_cancel, 
      #           trans_date, corr_start_date, corr_exp_date
      # a record:
      #     ++5BmBHS2ebe4Whfg/7KhGkj/sQ6rtNHsktLxsI01KE=,39,149,149,1,0,2016-09-30,2016-10-02,2016-11-01
      
    ## find problematic members -- members with one date on which there are >= 3 transactions, of
    ## which at least one is not a cancel and at least one is a cancel
    df1 = df0.select('msno', 
                     'trans_date',
                     col('is_cancel').cast('int').alias('is_cancel'))
    df2 = df1.groupBy('msno', 'trans_date').agg(count('*').alias('count_records'),
                                                sum('is_cancel').alias('sum_cancel'))
    df3 = df2.where('count_records >= 3') \
             .where('sum_cancel >= 1') \
             .where('sum_cancel < count_records') \
             .selectExpr('msno')
    
    ## remove problematic members
    df4 = df0.join(df3, df0['msno']==df3['msno'], 'left_anti')
    
    ## add tid
    df5 = df4.withColumn('tid', monotonically_increasing_id())
    
    ## output
    df5.write.format('csv').option('header','true').save(outfile)
    
## END ##


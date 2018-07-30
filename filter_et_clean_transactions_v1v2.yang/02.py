from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

if __name__ == "__main__":
    infile_transactions_clean1 \
      = 'file:///home/cloudera/2.kkbox_churn/data01/from_raw_transactions-v1+v2/01.clean1'
    infile__plan_days_vs_list_price \
      = 'file:///home/cloudera/2.kkbox_churn/data01/from_raw_transactions-v1+v2/plan-days_vs_list-price'
    outfile = 'file:///home/cloudera/2.kkbox_churn/data01/from_raw_transactions-v1+v2/02.clean2_plan-days-0'

    ##
    spark = SparkSession.builder.getOrCreate()
    
    
    ## read data
    df0 = spark.read.format('csv').option('header','true').load(infile_transactions_clean1)
      # columns:  msno, payment_method_id, payment_plan_days, plan_list_price, 
      #           actual_amount_paid, is_auto_renew, transaction_date, membership_expire_date, 
      #           is_cancel
      
      # a record:
      #      ++4RuqBw0Ss6bQU4oMxaRlbBPoWzoEiIZaxPM04Y4+U=,41,30,149,149,1,20161213,20170113,0

    df_plandays_vs_listprice \
        = spark.read.format('csv').option('header','true').load(infile__plan_days_vs_list_price) \
          .selectExpr('plan_list_price AS list_price_',
                      'payment_plan_days AS plan_days_')
      # columns: list_price_, plan_days_
      # contains only list_price_ != 0
    
    
    ## deal with plan_days = 0: guess plan_days from actual_paid (if != 0)
    df1 = df0.withColumn('actual_paid_q',
                         when(col('payment_plan_days')=='0', col('actual_amount_paid')) \
                           .otherwise(lit('na')))
                           
    df2 = df1.join(df_plandays_vs_listprice,
                   df1['actual_paid_q']==df_plandays_vs_listprice['list_price_'],
                   'left_outer') \
          .drop('list_price_', 'actual_paid_q') \
          .withColumnRenamed('payment_plan_days', 'plan_days_orig') \
          .withColumnRenamed('plan_days_', 'plan_days_guess')
          
    df3 = df2.withColumn('plan_days',
                         when(col('plan_days_guess').isNull(), col('plan_days_orig')) \
                           .otherwise(col('plan_days_guess'))) \
          .drop('plan_days_orig', 'plan_days_guess')
    
    
    ## output
    df3.write.format('csv').option('header','true').save(outfile)
    



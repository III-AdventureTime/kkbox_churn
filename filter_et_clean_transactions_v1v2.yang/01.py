from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    infile_transactions = 'file:///home/cloudera/2.kkbox_churn/raw_data/transactions_v1+v2'
    outfile = 'file:///home/cloudera/2.kkbox_churn/data01/from_raw_transactions-v1+v2/01.clean1'


    ##
    spark = SparkSession.builder.getOrCreate()
    
    
    ## read data
    df0 = spark.read.format('csv').option('header','true').load(infile_transactions)
      # columns:  msno, payment_method_id, payment_plan_days, plan_list_price, 
      #           actual_amount_paid, is_auto_renew, transaction_date, membership_expire_date, 
      #           is_cancel
      
      # 1st record:
      #      YyO+tlZtAXYXoZhNr3Vg3+dfVQvrBVGO8j1mfqe4ZHc=,41,30,129,129,1,20150930,20151101,0

    
    ## remove members who have a record with is_cancel=1 and is_auto_renew=0
    df_p = df0.where(col('is_cancel') == '1').where(col('is_auto_renew') == '0')
    df1 = df0.join(df_p, df0['msno']==df_p['msno'], 'left_anti')
    
    
    ## remove members who have a record with plan_days=0, is_cancel=0, and actual_paid=0
    df_p = df0.where(col('payment_plan_days') == '0') \
        .where(col('is_cancel') == '0') \
        .where(col('actual_amount_paid') == '0')
    df2 = df1.join(df_p, df1['msno']==df_p['msno'], 'left_anti')
    
    
    ## output
    df2.write.format('csv').option('header','true').save(outfile)



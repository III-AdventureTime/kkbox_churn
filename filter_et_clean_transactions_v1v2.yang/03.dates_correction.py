from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_date, udf, col, when
from pyspark.sql.types import DateType, ArrayType

from find_start_date import find_start_date

def dates_correction(is_cancel, trans_date, expire_date, plan_days):
    '''
    `is_cancel`: boolean
    `trans_date`, `expire_date`: datetime.date objects
    `plan_days`: integer
    
    Returns [corrected-start-date, corrected-expiration-date]. If `is_cancel` is True, 
    corrected-start-date will always be None. If `is_cancel` is False, corrected-start-date
    and corrected-expiration-date may both be None.
    '''
    d_start = None
    d_exp = None
    if is_cancel:
        d_start = None
        d_exp = expire_date if trans_date <= expire_date  else trans_date
    else: # (not is_cancel)
        start_date = find_start_date(expire_date, plan_days)
        if start_date is None:
            d_start = None
            d_exp = None
        elif trans_date <= start_date:
            d_start = start_date
            d_exp = expire_date
        elif trans_date <= expire_date:
            d_start = trans_date
            d_exp = expire_date
        else:
            d_start = None
            d_exp = None
            
    return [d_start, d_exp]


#----------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    dir0 = 'file:///home/cloudera/2.kkbox_churn/data01/from_raw_transactions-v1+v2/'
    infile = dir0 + '02.clean2_plan-days-0'
    outfile = dir0 + '03.clean3_dates_corrections'
        
    ##
    spark = SparkSession.builder.getOrCreate()
    
    ##
    df0 = spark.read.format('csv').option('header','true').load(infile)
      # columns: msno, payment_method_id, plan_list_price, actual_amount_paid, is_auto_renew, 
      #                transaction_date, membership_expire_date, is_cancel, plan_days
      # a record: 
      #          ++Jv+7YFiQv05MJ+Ep3wKS4QxojHEu78P3JOsR9djlo=,41,149,0,1,20150102,20150301,0,30
      
    ## casting columns
    df1 = df0.select('msno', 
                     expr('payment_method_id AS pay_method'),
                     expr('plan_list_price AS list_price'),
                     expr('actual_amount_paid AS actual_paid'),
                     'is_auto_renew',
                     expr('CAST(is_cancel AS boolean) AS is_cancel'),
                     to_date('transaction_date', 'yyyyMMdd').alias('trans_date'),
                     expr('CAST(plan_days AS int) AS plan_days'),
                     to_date('membership_expire_date', 'yyyyMMdd').alias('exp_date')
                    )
                    
    ## date corrections          
    udf_dates_correction = udf(dates_correction, ArrayType(DateType()))
    df2 = df1.withColumn('corr_start_exp_dates',
                     udf_dates_correction('is_cancel', 'trans_date', 'exp_date', 'plan_days')) \
      .withColumn('corr_start_date', expr('corr_start_exp_dates[0]')) \
      .withColumn('corr_exp_date', expr('corr_start_exp_dates[1]')) \
      .drop('corr_start_exp_dates')
      
    df3 = df2.where(~col('corr_exp_date').isNull()) 
    
    ## casting columns
    df4 = df3.select('msno', 'pay_method', 'list_price', 'actual_paid', 'is_auto_renew',
                     when(col('is_cancel'), '1').otherwise('0').alias('is_cancel'),
                     'trans_date', 'corr_start_date', 'corr_exp_date')

    ## output
    df4.write.format('csv').option('header','true').save(outfile)


## END ##


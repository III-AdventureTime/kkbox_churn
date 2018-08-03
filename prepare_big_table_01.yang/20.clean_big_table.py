from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, udf, lit
from pyspark.sql.types import IntegerType
import re


def get_ww(col_name):
    m = re.search('last(\d+)WeekOnLine', col_name)
    if m:
        return m.groups()[0]
    
    m = re.search('^w(\d+)_', col_name)
    if m:
        return m.groups()[0]
    else:
        return None

def block_no(col_name):
    ''' Returns 0 - 13 '''
    
    wws = ['12', '9', '6', '4', '2', '1']
    
    if col_name == 'is_churn':
        return 13
    
    ww = get_ww(col_name)
    if ww:
        if '_D_' in col_name:
            return wws.index(ww)*2 + 2
        else:
            return wws.index(ww)*2 + 1
    else:
        return 0


def to_int(v_str):
    if v_str is None:
        return 0
    else:
        return int(v_str.replace(',', ''))
        


######################################################################################################
        
if __name__ == "__main__":
    dir0 = 'file:///home/cloudera/2.kkbox_churn/data01/big_table_01/'
    infile = dir0 + 'all_features'
    outfile = dir0 + 'temp' #'all_features.cleaned'

    ##
    spark = SparkSession.builder.getOrCreate()
    df0 = spark.read.format('parquet').load(infile)

    ## 
    udf_to_int = udf(to_int, IntegerType())
    
    ##
    new_columns = []
    for c in df0.columns:
        if c in ['has_long_gap', 'has_expired', 'auto_renew_mode', 'is_churn']:
            cc = when(col(c)=='1', True).otherwise(False).alias(c)
            
        elif block_no(c) == 0:
            if c.endswith('_cnt_uniq'):
                cc = col(c).cast('int').alias(c)
            elif c in ['age','days_since_init_regist', 'gaps_count', 'total_gap_len', 'last_plan_days',
                       'last_list_price', 'last_actual_paid', 'last_discount', 'actual_paid_mode', 
                       'discount_mode', 'list_price_mode', 'plan_days_mode', 'actual_paid_sum', 
                       'discount_sum', 'list_price_sum']:
                cc = col(c).cast('int').alias(c)
            elif c in ['last_paid_per_day', 'actual_paid_per_sub_day',
                       'discount_per_sub_day', 'list_price_per_sub_day']:
                cc = col(c).cast('double').alias(c)
            else:
                cc = col(c)
                
        elif block_no(c) in [1,3,5,7,9,11]:
            if c.endswith('Mtotal') or c.endswith('WeekOnLine'):
                cc = col(c).cast('int').alias(c)
            else:
                cc = udf_to_int(col(c)).alias(c)
            
        elif block_no(c) in [2,4,6,8,10,12]:
            cc = col(c).cast('double').alias(c)
            
        else:
            cc = col(c)
        new_columns.append(cc)
       
    ##    
    df1 = df0.select(*new_columns)   
    
    ## output
    df1.write.format('parquet').save(outfile)
    
## END ##


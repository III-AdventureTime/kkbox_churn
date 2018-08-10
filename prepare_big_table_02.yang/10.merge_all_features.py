from pyspark.sql import SparkSession
from pyspark.sql.functions import col
        
if __name__ == "__main__":
    dir0 = '/home/cloudera/2.kkbox_churn/data01/feature_creation_01/'
    
    infiles, fileTypes = [], []
    infiles.append( dir0 + 'members' )
    fileTypes.append('csv')
    
    infiles.append( dir0 + 'transactions' )
    fileTypes.append('csv')
    
    infiles.append( dir0 + 'userlogs.snappy.parquet' )
    fileTypes.append('parquet')
    
    infiles.append( '/home/cloudera/2.kkbox_churn/data01/from_raw_transactions-v1+v2/11.is-churn_across-201702-03_members-exp-in-201702' )
    fileTypes.append('parquet')
              
    infiles.append( '/home/cloudera/2.kkbox_churn/raw_data/train.csv' )
    fileTypes.append('csv')
    
    outfile = dir0 + 'all_features'


    ##
    spark = SparkSession.builder.getOrCreate()
    
    ## read files
    dfs = []
    for i in range(len(infiles)):
        if fileTypes[i] == 'csv':
            df = spark.read.format('csv').option('header','true').load(infiles[i])
        elif fileTypes[i] == 'parquet':
            df = spark.read.format('parquet').load(infiles[i])
        dfs.append(df)
        
    ## join all tables
    df0 = dfs[0]
    for i in range(1, len(dfs)):
        df = dfs[i]
        
        col_msno = [c for c in df.columns if c.startswith('msno')][0]
        df = df.withColumnRenamed(col_msno, 'msno_')
        
        if i == 3:
            df = df.withColumnRenamed('is_churn', 'my_is_churn')
            
        df0 = df0.join(df, df0['msno']==df['msno_'], 'inner').drop('msno_')
            
    ## output
    df0.write.format('parquet').save(outfile)            
            
## END ##


from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, collect_list
import datetime as DT


def add_one_day(date_str):
    t = DT.datetime.strptime(date_str, '%Y-%m-%d') + DT.timedelta(days=1)
    return t.strftime('%Y-%m-%d')

def subtract_one_day(date_str):
    t = DT.datetime.strptime(date_str, '%Y-%m-%d') - DT.timedelta(days=1)
    return t.strftime('%Y-%m-%d')

def find_indeterminate_period(tinfo1, tinfo2):
    tinfo_cancel, tinfo_noncancel = (tinfo1, tinfo2)  if tinfo1[2] == '1'  else (tinfo2, tinfo1)
    dc = tinfo_cancel[4]
    if dc >= tinfo_noncancel[4]:
        return None
    elif dc >= tinfo_noncancel[3]:
        return (add_one_day(dc), tinfo_noncancel[4])
    else: # dc < tinfo_noncancel[3]
        return (tinfo_noncancel[3], tinfo_noncancel[4])
    
def reduce_subscription_periods(subscrip_periods, cut_date):
    '''
    cut all periods in `subscrip_periods` to before `cut_date`
    '''
    subscrip_periods_copy = subscrip_periods[:]
    pre_cut_date = subtract_one_day(cut_date)
    
    for p in subscrip_periods_copy:
        if cut_date <= p[1] :
            subscrip_periods.remove(p)
            
    for i in range(0, len(subscrip_periods)):
        p = subscrip_periods[i]
        if cut_date <= p[2]:
            subscrip_periods[i] = (p[0], p[1], pre_cut_date)

def has_no_cancel(tinfo_records):
    r = True
    for is_cancel in map(lambda info_rec: info_rec[2]=='1', tinfo_records):
        if is_cancel:
            r = False
            break
    return r

def reduce_trans_infos(trans_infos_strlist):
    '''
    Deals with cancels and determines effective subscription periods.
    
    Returns (subscription-periods, indeterminate-periods), where subscription-periods is 
    a list of tuples (tid, start-date, end-date), and indeterminate-periods is a list of 
    tuples (start-date, end-date).
    '''
    trans_infos = [s.split('/')  for s in trans_infos_strlist]
        # trans_infos[trans-index][item-index]  
        # item-index  --  0: tid;  1: transaction date;  2: is cancel;  
        #                 3: start date;  4: end (expiration) date
    
    ##
    if has_no_cancel(trans_infos):
        subscrip_periods = [(tinfo[0], tinfo[3], tinfo[4])  for tinfo in trans_infos]
        return (subscrip_periods, [])
            
    ## separate the transaction records by transaction dates having a cancel
    ### get transaction dates having a cancel
    trans_dates_w_cancel_s = set()
    last_trans_date = '1970-01-01'
    for tinfos in trans_infos:
        if tinfos[2] == '1':
            trans_dates_w_cancel_s.add(tinfos[1])
        if tinfos[1] > last_trans_date:
            last_trans_date = tinfos[1]
        
    separating_dates = sorted(list(trans_dates_w_cancel_s)) + [last_trans_date]

    ###
    trans_info_groups = []
    for sdate in separating_dates:
        group = [tinfos  for tinfos in trans_infos  if tinfos[1] <= sdate]
        if group != []:
            trans_info_groups.append(group)
            for info in group:
                trans_infos.remove(info)
        # now `trans_infos` is destroyed
        # use `trans_info_groups` instead
        
    ## deal with each group
    subscrip_periods = []
    indeterm_periods = []
    for tinfos in trans_info_groups:
        assert tinfos != []
        
        ## sort by transaction date
        tinfos.sort(key=(lambda tinfo: tinfo[1]))
                
        ##
        if has_no_cancel(tinfos):
            subscrip_periods_1 = [(tinfo[0], tinfo[3], tinfo[4])  for tinfo in tinfos]
            subscrip_periods.extend(subscrip_periods_1)
            continue
            
        ## get transaction infos before last transaction date
        last_tdate = tinfos[-1][1]
        
        ind_pre_last_date = -1
        for i in range(len(tinfos)-1, -1, -1):
            if tinfos[i][1] != last_tdate:
                ind_pre_last_date = i
                break
        tinfos_pre_last_date = tinfos[0:ind_pre_last_date+1]  
        assert (ind_pre_last_date+1) < len(tinfos)
        
        ## transform `tinfos_pre_last_date` into subscription periods
        subscrip_periods_1 = [(tinfo[0], tinfo[3], tinfo[4])  for tinfo in tinfos_pre_last_date]
            
        ## find whether there is non-cancel transaction on the last transaction date
        has_non_cancel = any([tinfo[2]=='0'  for tinfo in tinfos[ind_pre_last_date+1:]])
        
        ##
        indet_period = None
        if has_non_cancel:  # one cancel and one non-cancel on `last_tdate`
            indet_period = find_indeterminate_period(tinfos[-2], tinfos[-1])
            if indet_period is not None:
                cut_date = indet_period[0]
                reduce_subscription_periods(subscrip_periods_1, cut_date)
        else:  # only cancels on `last_tdate`
            cut_date = min([tinfo[4] for tinfo in tinfos[ind_pre_last_date+1:]])
            reduce_subscription_periods(subscrip_periods_1, cut_date)
        
        ## store results
        subscrip_periods.extend(subscrip_periods_1)
        if indet_period is not None:
            indeterm_periods.append(indet_period)
    ## END of looping trans_info_groups
    
    return (subscrip_periods, indeterm_periods)

def pack_data(msno, subscrip_periods, indeterm_periods):
    result = []
    for it in subscrip_periods:
        result.append(Row(msno=msno, tid=it[0], start_date=it[1], end_date=it[2]))
    for it in indeterm_periods:
        result.append(Row(msno=msno, tid='indeterm', start_date=it[0], end_date=it[1]))
    return result
    
def map_func(row):
    subscrip_periods, indeterm_periods = reduce_trans_infos(row['trans_infos'])
    return pack_data(row['msno'], subscrip_periods, indeterm_periods)
    

#-------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    dir0 = 'file:///home/cloudera/2.kkbox_churn/data01/from_raw_transactions-v1+v2/'
    infile = dir0 + '04.clean4_probl_members'
    outfile = dir0 + '10.effective_subscr_periods'
    
    ##
    spark = SparkSession.builder.getOrCreate()
    
    ##
    df0 = spark.read.format('csv').option('header','true').load(infile) \
          .select('msno', 'tid', 'trans_date', 'is_cancel', 'corr_start_date', 'corr_exp_date')
    
    ## collect infos for each member
    df1 = df0.fillna('na', ['corr_start_date']) \
          .select('msno',
                  concat_ws('/', 'tid', 'trans_date', 'is_cancel', 'corr_start_date', 'corr_exp_date')
                    .alias('info'))
    df2 = df1.groupBy('msno').agg(collect_list('info').alias('trans_infos')) 
      # |msno            |trans_infos                                                                          |
      # |++4RuqBw0Ss6... |[101/2016-12-13/0/2016-12-14/2017-01-13, 102/2016-11-13/0/2016-11-14/2016-12-13, ...]|
      
    ## reduce transaction infos to effective subscription periods (and indeterminate periods) by dealing
    ## with all cancels
    rdd3 = df2.rdd.flatMap(map_func)  
    
    ## output
    df4 = rdd3.toDF().select('msno', 'tid', 'start_date', 'end_date')
    df4.write.format('parquet').save(outfile)
    
## END ##


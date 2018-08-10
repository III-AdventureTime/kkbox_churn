import datetime as DT

def add_one_day(date_str):
    t = DT.datetime.strptime(date_str, '%Y-%m-%d') + DT.timedelta(days=1)
    return t.strftime('%Y-%m-%d')


def subtract_one_day(date_str):
    t = DT.datetime.strptime(date_str, '%Y-%m-%d') - DT.timedelta(days=1)
    return t.strftime('%Y-%m-%d')


def count_days(start_date, end_date):
    delta = DT.datetime.strptime(end_date, '%Y-%m-%d') - DT.datetime.strptime(start_date, '%Y-%m-%d')
    return delta.days + 1
    

def take_union(date_intervals):
    '''
    `date_intervals`: a list of date ranges, each being [start-date, end-date] or (start-date, end-date),
                      where start-date <= end-date, end dates are inclusive, and the date format is 
                      '%Y-%m-%d'. The ranges can be overlapping and is not necessarily in order.
                      
    Return a list of date ranges [start-date, end-date]. The list will be in ascending order and there 
    will be no overlapping or consecutive ranges.
    '''
    shifts_dict = {}  # {time: shift-amount, ...}
    for p in date_intervals:
        assert p[0] <= p[1]
        if p[0] in shifts_dict:
            shifts_dict[p[0]] += 1
        else:
            shifts_dict[p[0]] = 1
            
        t_end = add_one_day(p[1])
        if t_end in shifts_dict:
            shifts_dict[t_end] -= 1
        else:
            shifts_dict[t_end] = -1
    
    shifts = sorted(shifts_dict.items())
    union_intervals = []  # [[start-time, end-time], ...]
    v = 0
    start = None
    for d_sh in shifts:
        v_prev = v
        v += d_sh[1]
        if v_prev == 0 and v > 0:
            start = d_sh[0]
        elif v_prev > 0 and v == 0:
            end = d_sh[0]
            union_intervals.append([start, end])
            
    return [[p[0], subtract_one_day(p[1])]  for p in union_intervals]
    
    
def find_gaps(periods, init_date, final_date, min_gap_len):
    '''
    The difference of [init_date, final_date] to `periods` can be represented as a union of
    disjoint "gaps". This method finds gaps that are no shorter than `min_gap_len` days.
    
    `periods`: a list of date ranges [start-date, end-date], where start-date <= end-date,
               end dates are inclusive, and the date format is '%Y-%m-%d'. It can have 
               overlapping date ranges and is not necessarily in order.
               
    Return a list of gaps [gap-start-date, gap-end-date].
    '''
    
    min_date = min([p[0] for p in periods] + [subtract_one_day(init_date)])
    max_date = max([p[1] for p in periods] + [add_one_day(final_date)])
    periods += [[min_date, subtract_one_day(init_date)], [add_one_day(final_date), max_date]]
    disjoint_periods = take_union(periods)
    
    gaps = []
    for i in range(len(disjoint_periods)-1):
        a_gap = [add_one_day(disjoint_periods[i][1]), subtract_one_day(disjoint_periods[i+1][0])]
        if count_days(a_gap[0], a_gap[1]) >= min_gap_len:
            gaps.append(a_gap)
        
    return gaps  
    
## END ##


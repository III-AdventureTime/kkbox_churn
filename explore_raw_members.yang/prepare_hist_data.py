import math
import operator
import numpy

def prepare_histogram_data(rdd, vmin, vmax, Nbin):
    '''
    arguments:
       * rdd -- a list of numeric values
       * Nbin -- a positive integer
       
    return: tuple (counts_list, underflow_count, overflow_count), where counts_list is
            a list of (bin_center, count)
    '''
    
    dv = float(vmax - vmin) / Nbin
    list1 = rdd.map(lambda v: min(vmax + dv/2.0, max(vmin - dv/2.0, float(v)))) \
               .map(lambda v: (int(math.floor((float(v) - vmin) / dv)), 1)) \
               .reduceByKey(operator.add) \
               .collect()
    list1.sort()
       
    cnt_underflow, cnt_overflow = (0, 0)
    list2 = [0] * Nbin
    for p in list1:
        if p[0] <= -1:
            cnt_underflow = p[1]
        elif p[0] >= Nbin:
            cnt_overflow = p[1]
        else:
            list2[p[0]] = p[1]
    
    bin_centers = numpy.linspace(vmin + dv/2.0, vmax - dv/2.0, num=Nbin, endpoint=True)

    return {'bin_centers_counts': list(zip(bin_centers, list2)),
            'underflow': cnt_underflow, 'overflow': cnt_overflow}


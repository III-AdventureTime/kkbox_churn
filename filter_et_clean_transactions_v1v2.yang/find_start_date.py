import datetime as DT
import calendar

def get_years_months_days(plan_days):
    if plan_days == 0:
        years = None
        months = None
        days = None
    elif plan_days == 360:
        years = 0
        months = 11
        days = 25
    else:
        years = plan_days // 365
        months = (plan_days % 365) // 30
        days = (plan_days % 365) % 30
    return (years, months, days)


def normalize_year_month(year, month):
    year = year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    return (year, month)

#--------------------------------------------------------------------------------------

def find_start_date(expire_date, plan_days):
    '''
    `expire_date`: a datetime.date object
    `plan_days`: integer
    Return start date as a datetime.date object (or None if there's problem)
    '''
    pl_years, pl_months, pl_days = get_years_months_days(plan_days)
    if pl_years is None:
        return None
    
    start_yr, start_mn, start_day = (None, None, None)
    d1 = DT.datetime.combine(expire_date, DT.time()) - DT.timedelta(days=pl_days)
    if d1.day == calendar.monthrange(d1.year, d1.month)[1]:
        start_day = 1
        start_yr, start_mn = normalize_year_month(d1.year-pl_years, d1.month-pl_months+1)
    else:
        start_day = d1.day + 1
        start_yr, start_mn = normalize_year_month(d1.year-pl_years, d1.month-pl_months)
        if start_day > calendar.monthrange(start_yr, start_mn)[1]:
            start_day = 1
            start_yr, start_mn = normalize_year_month(start_yr, start_mn+1)
        
    return DT.date(start_yr, start_mn, start_day)



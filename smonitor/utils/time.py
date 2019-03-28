# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

from collections import namedtuple
from datetime import datetime, timedelta


TimeSpan = namedtuple('TimeSpan', ['start','end'])

def date_range(start_date, end_date, span='day'):
    
    # Factor for timedelta in days
    delta_factor = 1
    time_range = None

    if span == 'day':
        for n in range(int((end_date - start_date).days)):
            yield TimeSpan(start_date + timedelta(days=n*delta_factor), start_date + timedelta(days=(n+1)*delta_factor))

    elif span == 'week':
        week_begin = start_date - timedelta(days=start_date.weekday())
        week_end = end_date - timedelta(days=end_date.weekday()) + timedelta(days=7)
        week_count = (week_end - week_begin).days / 7

        for n in range(week_count):
            if n == 0:
                yield TimeSpan(start_date, week_begin + timedelta(days=7))
            else:
                if ((week_begin + timedelta(days=(n+1)*7)) - end_date).days > 0:
                    yield TimeSpan(week_begin + timedelta(days=n*7), end_date)
                else:
                    yield TimeSpan(week_begin + timedelta(days=n*7), week_begin + timedelta(days=(n+1)*7))
    
    elif span == 'month':
        month_begin = datetime(year=start_date.year, month=start_date.month, day=1)
        month_end = datetime(year=end_date.year, month=end_date.month, day=1)
        
        year_diff = max(month_end.year - month_begin.year - 1, 0)
        if year_diff > 0:
            month_count =  (13-month_begin.month) + month_end.month + year_diff*12
        else:
            month_count =  month_end.month - month_begin.month + 1

        for n in range(month_count):
            c_month =  ((month_begin.month + n - 1) % 12) + 1
            c_year = (month_begin.year) + ((month_begin.month + n - 1) / 12)
            
            if n == 0:
                n_year = month_begin.year if month_begin.month < 12 else month_begin.year + 1
                n_month = month_begin.month + 1 if month_begin.month < 12 else 1

                yield TimeSpan(start_date, datetime(year=n_year, month=n_month, day=1))
            elif n == month_count-1:
                yield TimeSpan(datetime(year=c_year, month=c_month, day=1), end_date)
            else:
                n_month =  ((month_begin.month + n) % 12) + 1
                n_year = (month_begin.year) + ((month_begin.month + n) / 12)

                yield TimeSpan(datetime(year=c_year, month=c_month, day=1), datetime(year=n_year, month=n_month, day=1))

    else:
        raise ValueError('Invalid span value. Valid values: day, week, month, year')

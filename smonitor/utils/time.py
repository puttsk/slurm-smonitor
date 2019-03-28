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
        delta_factor = 1
        time_range = range(int((end_date - start_date).days))
    else:
        raise ValueError('Invalid span value. Valid values: day, week, month, year')

    for n in time_range:
        yield TimeSpan(start_date + timedelta(days=n*delta_factor), start_date + timedelta(days=(n+1)*delta_factor))
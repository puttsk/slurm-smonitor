# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import subprocess

from ..utils.time import date_range
from ..slurm.parser import SlurmParser

def query_utilization(begin_date, end_date, freq='day', time_unit='min'):

    if freq and freq not in ['day', 'week', 'month', 'year']:
        raise ValueError('Invalid freq value')

    if time_unit not in ['sec', 'min', 'hour']:
        raise ValueError('Invalid time_unit value')

    for d in date_range(begin_date, end_date, freq=freq):
        sreport_command = 'sreport -P -t {} cluster utilization start={} end={}'.format(time_unit ,d.start.strftime('%Y-%m-%d'), d.end.strftime('%Y-%m-%d'))
        sreport_output = subprocess.check_output(sreport_command.split(' '), universal_newlines=True)
        sreport_results = SlurmParser.parse_output(sreport_output)

        if len(sreport_results) > 0:
            utilization = sreport_results[0]
            utilization['StartDate'] = d.start.strftime('%Y-%m-%d')
            utilization['EndDate'] = d.end.strftime('%Y-%m-%d')
            utilization['Utilization'] = utilization['Allocated'] / float(utilization['Reported'])
            utilization['Unit'] = time_unit

            yield utilization
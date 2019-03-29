# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import subprocess

from ..utils.time import date_range
from ..slurm.parser import SlurmParser

def report_utilization(begin_date, end_date, freq):
    output = []

    for d in date_range(begin_date, end_date, freq=freq):
        sreport_command = 'sreport -P -t min cluster utilization start={} end={}'.format(d.start.strftime('%Y-%m-%d'), d.end.strftime('%Y-%m-%d'))
        sreport_output = subprocess.check_output(sreport_command.split(' '), universal_newlines=True)
        sreport_results = SlurmParser.parse_output(sreport_output)

        if len(sreport_results.results) > 0:
            utilization = sreport_results.results[0]
            utilization['StartDate'] = d.start.strftime('%Y-%m-%d')
            utilization['EndDate'] = d.end.strftime('%Y-%m-%d')
            utilization['Utilization'] = utilization['Allocated'] / float(utilization['Reported'])

            output.append(utilization)

    return output
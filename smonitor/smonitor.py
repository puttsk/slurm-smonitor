#!/usr/bin/env python
# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import re
import json
import argparse
import subprocess

from datetime import datetime, timedelta
from pprint import pprint

from .config import __version__

from .slurm.parser import SlurmParser
from .utils.time import date_range

def parse_args():
    slurm_version = str(subprocess.check_output(['sinfo', '--version']))
    version = 'smonitor v.{} with {}'.format(__version__, slurm_version)

    parser = argparse.ArgumentParser(prog='smonitor', description='Slurm monitoring tools')
    parser.add_argument(
        'type', action='store', nargs='?', help="Monitoring metric. Valid values: 'utility'")
    parser.add_argument(
        '--format', action='store', help="Output format. Valid values: 'json'")
    parser.add_argument(
        '-o','--output', action='store', help="output file")
    parser.add_argument(
        '-V', '--version', action='version', version=version)
    parser.add_argument(
        '-v', '--verbose', action='count', help="verbose mode (multiple -v's increase verbosity)")

    return parser

def main():
    parser = parse_args()
    args = parser.parse_args()

    if args.verbose:
        def verbose_print(*a, **k):
            if k.pop('level', 0) <= args.verbose:
                pprint(*a, **k)
    else:
        verbose_print = lambda *a, **k: None

    if args.type == 'utilization':
        output = []
        begin_date = datetime.strptime("2019-02-21", '%Y-%m-%d')

        for d in date_range(begin_date, datetime.now(), span='day'):
            sreport_command = 'sreport -P -t min cluster utilization start={} end={}'.format(d.start.strftime('%Y-%m-%d'), d.end.strftime('%Y-%m-%d'))
            sreport_output = subprocess.check_output(sreport_command.split(' '), universal_newlines=True)
            sreport_results = SlurmParser.parse_output(sreport_output)

            utilization = sreport_results.results[0]
            utilization['StartDate'] = d.start.strftime('%Y-%m-%d')
            utilization['EndDate'] = d.end.strftime('%Y-%m-%d')
            utilization['Utilization'] = utilization['Allocated'] / float(utilization['Reported'])

            output.append(utilization)

        if args.format == 'json':
            if args.output:
                with open(args.output, 'wt') as f:
                    json.dump(output, f,indent=4, sort_keys=True)
            else:
                print(json.dumps(output, indent=4, sort_keys=True))
        else:
            pprint(output)
    else:
        parser.print_help()
    


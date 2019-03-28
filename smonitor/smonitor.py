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
from .slurm.utils import print_cpu_usage

def parse_args():
    slurm_version = str(subprocess.check_output(['sinfo', '--version']))
    version = 'smonitor v.{} with {}'.format(__version__, slurm_version)

    parser = argparse.ArgumentParser(prog='smonitor', description='Slurm monitoring tools')
    parser.add_argument(
        '-S', '--summary', action='store_true', help="summarize system state.")
    parser.add_argument(
        '--output', action='store', help="Output format. Only json is currently supported")
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

    if args.summary:
        sreport_command = 'sreport -P -t min cluster utilization'
        sreport_output = subprocess.check_output(sreport_command.split(' '), universal_newlines=True)
        sreport_results = SlurmParser.parse_output(sreport_output)

        utilization = sreport_results.results[0]

        yesterday = datetime.now() - timedelta(days=1)
        utilization['date'] = yesterday.strftime('%Y-%m-%d')
        
        print(json.dumps(utilization))
    else:
        parser.print_help()
    


#!/usr/bin/env python
# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import json
import argparse
import subprocess
import sys

from pprint import pprint
from datetime import datetime

from .config import __version__, SERVICE_BEGIN_DATE

from .utils.io import generate_output
from .utils.data import flattern_nested_dict
from .query.utilization import query_utilization
from .query.usage import query_usage, query_group_usage

def parse_args():
    def list_str(values):
        return values.split(',')

    slurm_version = str(subprocess.check_output(['sinfo', '--version']))
    version = 'smonitor v.{} with {}'.format(__version__, slurm_version)

    parser = argparse.ArgumentParser(prog='smonitor', description='Slurm monitoring tools')
    parser.add_argument(
        'type', action='store', nargs='?', help="Monitoring metric. Valid values: 'utilization', 'usage'")
    parser.add_argument(
        '-A','--account', default=None, dest='account_list', action='store', type=list_str, help="a comma separated list of account to be displayed")
    parser.add_argument(
        '--fields', default=None, action='store', type=list_str, help="a comma separated list of fields.")
    parser.add_argument(
        '--groups_by', default=None, action='store', type=list_str, help="grouping result using a comma separated list of fields.")
    parser.add_argument(
        '--groups_by_fields', default=None, action='store', type=list_str, help="a comma separated list of fields to be displayed after grouping.")
    parser.add_argument(
        '--format', action='store', help="Output format. Valid values: 'json'")
    parser.add_argument(
        '--start', action='store', help="Period start for report. Supported format: YYYY-MM-DD.")
    parser.add_argument(
        '--end', action='store', help="Period ending for report. Supported format: YYYY-MM-DD.")
    parser.add_argument(
        '--freq', action='store', default=None, help="Report frequency. Valid values: 'day', 'week', 'month', 'year'. Default: 'day'")
    parser.add_argument(
        '-t', '--time-unit', dest='unit', action='store', default='min', help="Report unit. Valid values: 'sec', 'min', 'hour'. Default: 'min'")
    parser.add_argument(
        '-o','--output', action='store', help="output file")
    parser.add_argument(
        '-V', '--version', action='version', version=version)
    parser.add_argument(
        '-v', '--verbose', action='count', help="verbose mode (multiple -v's increase verbosity)")

    return parser

def validate_args(args, parser):
    supported_format = ['json', 'table', 'csv']
    supported_freq = ['day', 'week', 'month', 'year']
    supported_unit = ['sec', 'min', 'hour']

    if args.format:
        if args.format not in supported_format:
            parser.error("argument --format: invalid value '{}'. valid values: {}".format(args.format, ', '.join(map(lambda x: "'{}'".format(x), supported_format))))

    if args.freq and args.freq not in supported_freq:
        parser.error("argument --freq: invalid value '{}'. valid values: {}".format(args.freq, ', '.join(map(lambda x: "'{}'".format(x), supported_freq))))
    
    if args.unit not in supported_unit:
        parser.error("argument --unit: invalid value '{}'. valid values: {}".format(args.unit, ', '.join(map(lambda x: "'{}'".format(x), supported_unit))))

    if args.start:
        try:
            args.start = datetime.strptime(args.start, '%Y-%m-%d')
        except:
            parser.error("argument --start: invalid format '{}'. valid format: YYYY-MM-DD e.g. {}".format(args.start, datetime.now().strftime('%Y-%m-%d')))

    if args.end:
        try:
            args.end = datetime.strptime(args.end, '%Y-%m-%d')
        except:
            parser.error("argument --end: invalid format '{}'. valid format: YYYY-MM-DD e.g. {}".format(args.end, datetime.now().strftime('%Y-%m-%d')))

def main():
    parser = parse_args()
    args = parser.parse_args()
    validate_args(args, parser)

    if args.verbose:
        def verbose_print(*a, **k):
            if k.pop('level', 0) <= args.verbose:
                pprint(*a, **k)
    else:
        verbose_print = lambda *a, **k: None

    if args.type == 'utilization':
        begin_date = args.start if args.start else datetime.strptime(SERVICE_BEGIN_DATE, '%Y-%m-%d')
        end_date = args.end if args.end else datetime.now()
        
        output = query_utilization(begin_date, end_date, freq=args.freq, time_unit=args.unit)
        generate_output(output, args.format, args.output)
    elif args.type == 'usage':
        begin_date = args.start if args.start else datetime.strptime(SERVICE_BEGIN_DATE, '%Y-%m-%d')
        end_date = args.end if args.end else datetime.now()

        if args.groups_by:
            output = query_group_usage(
                begin_date, 
                end_date, 
                account_list=args.account_list, 
                groups_by=args.groups_by,
                groups_by_fields=args.groups_by_fields,
                freq=args.freq
            )
        else:
            output = query_usage(
                begin_date, 
                end_date, 
                account_list=args.account_list,
                fields=args.fields,
                freq=args.freq
            )
        
        if args.format != 'json':
            flatten_output = [] 
            for o in output:
                flatten_output += flattern_nested_dict(o)
            generate_output(flatten_output, args.format, args.output)
        else:
            generate_output(output, args.format, args.output)
    else:
        parser.print_help()

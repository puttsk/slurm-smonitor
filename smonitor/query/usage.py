# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import subprocess
import math
import re

from pprint import pprint
from collections import namedtuple, OrderedDict
from datetime import timedelta, datetime

from ..utils.time import date_range
from ..utils.string import to_snake_case
from ..slurm.parser import SlurmParser

SACCT_FIELDS=[
    'Account',
    'AdminComment',
    'AllocCPUS',
    'AllocGRES',
    'AllocNodes',
    'AllocTRES',
    'AssocID',
    'AveCPU',
    'AveCPUFreq',
    'AveDiskRead',
    'AveDiskWrite',
    'AvePages',
    'AveRSS',
    'AveVMSize',
    'BlockID',
    'Cluster',
    'Comment',
    'ConsumedEnergy',
    'ConsumedEnergyRaw',
    'CPUTime',
    'CPUTimeRAW',
    'DerivedExitCode',
    'Elapsed',
    'ElapsedRaw',
    'Eligible',
    'End',
    'ExitCode',
    'GID',
    'Group',
    'JobID',
    'JobIDRaw',
    'JobName',
    'Layout',
    'MaxDiskRead',
    'MaxDiskReadNode',
    'MaxDiskReadTask',
    'MaxDiskWrite',
    'MaxDiskWriteNode',
    'MaxDiskWriteTask',
    'MaxPages',
    'MaxPagesNode',
    'MaxPagesTask',
    'MaxRSS',
    'MaxRSSNode',
    'MaxRSSTask',
    'MaxVMSize',
    'MaxVMSizeNode',
    'MaxVMSizeTask',
    'McsLabel',
    'MinCPU',
    'MinCPUNode',
    'MinCPUTask',
    'NCPUS',
    'NNodes',
    'NodeList',
    'NTasks',
    'Priority',
    'Partition',
    'QOS',
    'QOSRAW',
    'ReqCPUFreq',
    'ReqCPUFreqMin',
    'ReqCPUFreqMax',
    'ReqCPUFreqGov',
    'ReqCPUS',
    'ReqGRES',
    'ReqMem',
    'ReqNodes',
    'ReqTRES',
    'Reservation',
    'ReservationId',
    'Reserved',
    'ResvCPU',
    'ResvCPURAW',
    'Start',
    'State',
    'Submit',
    'Suspended',
    'SystemCPU',
    'SystemComment',
    'Timelimit',
    'TimelimitRaw',
    'TotalCPU',
    'TRESUsageInAve',
    'TRESUsageInMax',
    'TRESUsageInMaxNode',
    'TRESUsageInMaxTask',
    'TRESUsageInMin',
    'TRESUsageInMinNode',
    'TRESUsageInMinTask',
    'TRESUsageInTot',
    'TRESUsageOutAve',
    'TRESUsageOutMax',
    'TRESUsageOutMaxNode',
    'TRESUsageOutMaxTask',
    'TRESUsageOutMin',
    'TRESUsageOutMinNode',
    'TRESUsageOutMinTask',
    'TRESUsageOutTot',
    'UID',
    'User',
    'UserCPU',
    'WCKey',
    'WCKeyID',
    'WorkDir'
]

SACCT_PARAMS = map(to_snake_case, SACCT_FIELDS) 

datetime_regex = re.compile(r'((?P<day>\d+)-)?(?P<hr>\d+):(?P<min>\d+):(?P<sec>\d+)')

def __conv(val):
    if type(val) is str:
        try:
            val = float(val)
            if val.is_integer():
                return int(val)
            else:
                return val
        except ValueError:
            return val
    return val

def __preprocess_job(job):
    
    if job.get('__processed', None):
        return job

    if job['req_tres']:
        job['req_tres'] = {k:__conv(v) for k,v in (x.split('=') for x in job['req_tres'].strip().split(','))}
    else:
        job['req_tres'] = None

    job['elasped_mins'] = 0
    job['core_hour'] = 0
    job['su_usage'] = 0
    
    try:
        datetime_match = datetime_regex.match(job['reserved'])
        job['reserved'] = timedelta(
            days = int(datetime_match.group('day')) if datetime_match.group('day') else 0,
            hours = int(datetime_match.group('hr')),
            minutes = int(datetime_match.group('min')),
            seconds = int(datetime_match.group('sec'))
        ).total_seconds()
    except AttributeError:
        job['reserved'] = timedelta(0).total_seconds()

    if not isinstance(job['end'], datetime):
        try:
            job['end'] = datetime.strptime(job['end'], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            job['end'] = None

    if job['alloc_tres']:
        job['alloc_tres'] = {k:__conv(v) for k,v in (x.split('=') for x in job['alloc_tres'].strip().split(','))}
        job['elasped_mins'] = int(job['elapsed_raw']) / 60.0
        job['core_hour'] = int(job['elapsed_raw']) / 3600.0 * job['alloc_tres'].get('cpu', 0)
        job['su_usage'] = job['alloc_tres'].get('billing', 0) * job['elasped_mins']

    job['__processed'] = True

    return job

def query_usage(begin_date, end_date, account_list=None, fields=None, freq=None):
    optional_options = ''
    if account_list:
        optional_options = optional_options + '-A {}'.format(','.join(account_list))
    
    sacct_command = 'sacct -P -aX --format={} --start={} --end={} {}'.format(
        ','.join(SACCT_FIELDS), 
        begin_date.strftime('%Y-%m-%dT%H:%M:%S'), 
        end_date.strftime('%Y-%m-%dT%H:%M:%S'),
        optional_options
    ).strip()
    
    sacct_output = subprocess.check_output(sacct_command.split(' '), universal_newlines=True)
    sacct_results = SlurmParser.parse_output(sacct_output, convert_key=to_snake_case)

    for d in date_range(begin_date, end_date, freq=freq):
        output = {}

        output['start_date'] = d.start.strftime('%Y-%m-%dT%H:%M:%S')
        output['end_date'] = d.end.strftime('%Y-%m-%dT%H:%M:%S')
        output['results'] = []

        for job in sacct_results:

            job = __preprocess_job(job)

            if (not job['end']) and (job['state'] != "RUNNING"):
                continue

            if (job['end']) and (not(d.start <= job['end'] < d.end)):
                continue

            if fields:
                output['results'].append({ f: job[f] for f in fields })
            else:
                output['results'].append(job)
        
        yield output

def __update_dict(src, val):
    for key in src:
        if not isinstance(val, dict):
            return None

        if isinstance(src[key], dict):
            __update_dict(src[key], val=val[key])
        else:
            src[key] = src[key] + val[key]
            
def query_group_usage(begin_date, end_date, groups_by, groups_by_fields, account_list=None, freq=None):
    optional_options = ''
    if account_list:
        optional_options = optional_options + '-A {}'.format(','.join(account_list))

    sacct_command = 'sacct -P -aX --noconvert --format={} --start={} --end={} {}'.format(
        ','.join(SACCT_FIELDS), 
        begin_date.strftime('%Y-%m-%dT%H:%M:%S'), 
        end_date.strftime('%Y-%m-%dT%H:%M:%S'),
        optional_options
    ).strip()

    sacct_output = subprocess.check_output(sacct_command.split(' '), universal_newlines=True)
    sacct_results = SlurmParser.parse_output(sacct_output, convert_key=to_snake_case)

    for d in date_range(begin_date, end_date, freq=freq):
        
        output = {}

        output['start_date'] = d.start.strftime('%Y-%m-%dT%H:%M:%S')
        output['end_date'] = d.end.strftime('%Y-%m-%dT%H:%M:%S')
        output['result'] = {}
        output['fields'] = groups_by

        for job in sacct_results:
            if job.get('__counted', None):
                continue

            job = __preprocess_job(job)
            
            if (not job['end']) and (job['state'] != "RUNNING"):
                continue

            if (job['end']) and (not(d.start <= job['end'] < d.end)):
                continue

            data = { field: job[field] for field in groups_by_fields }
            
            output_ptr = output['result']
            for key in groups_by:
                if job[key] in output_ptr:
                    output_ptr = output_ptr[job[key]]
                else:
                    output_ptr[job[key]] = {}
                    output_ptr = output_ptr[job[key]]

            if output_ptr:
                for key in data:
                    if isinstance(output_ptr[key], dict):
                        __update_dict(output_ptr[key], data[key])
                    else:
                        output_ptr[key] = output_ptr[key] + data[key]

                    if 'count' in groups_by_fields:
                        output_ptr['count'] = output_ptr['count'] + 1
            else:
                output_ptr.update(data)
                
                if 'count' in groups_by_fields:
                    output_ptr['count'] = 1

            job['__counted'] = True

        if 'su_usage' in groups_by_fields:
            __update_su(output)

        yield output
            
def __update_su(d):
    for key in d:
        if isinstance(d[key], dict):
            __update_su(d[key])
        elif 'su_usage' in d.keys():
            d['su_usage'] = math.ceil(d['su_usage'])

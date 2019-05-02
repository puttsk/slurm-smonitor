# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import subprocess
import math
import re

from pprint import pprint
from collections import namedtuple
from datetime import timedelta

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
    job['req_tres'] = {k:__conv(v) for k,v in (x.split('=') for x in job['req_tres'].strip().split(','))}
    job['elasped_mins'] = 0
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

    if job['alloc_tres']:
        job['alloc_tres'] = {k:__conv(v) for k,v in (x.split('=') for x in job['alloc_tres'].strip().split(','))}
        job['elasped_mins'] = int(job['elapsed_raw']) / 60.0
        job['su_usage'] = job['alloc_tres'].get('billing', 0) * job['elasped_mins']

def query_usage(begin_date, end_date, account_list=None, fields=None):
    sacct_command = 'sacct -P -aX --format={} --start={} --end={}'.format(
        ','.join(SACCT_FIELDS), 
        begin_date.strftime('%Y-%m-%dT%H:%M:%S'), 
        end_date.strftime('%Y-%m-%dT%H:%M:%S')
    )
    
    sacct_output = subprocess.check_output(sacct_command.split(' '), universal_newlines=True)
    sacct_results = SlurmParser.parse_output(sacct_output, convert_key=to_snake_case)

    for job in sacct_results:
        if account_list:
            if job['account'] not in account_list:
                continue

        __preprocess_job(job)
        
        if fields:
            yield { f: job[f] for f in fields }
        else:
            yield job

def query_group_usage(begin_date, end_date, groups_by, groups_by_fields, account_list=None):
    sacct_command = 'sacct -P -aX --format={} --start={} --end={}'.format(
        ','.join(SACCT_FIELDS), 
        begin_date.strftime('%Y-%m-%dT%H:%M:%S'), 
        end_date.strftime('%Y-%m-%dT%H:%M:%S')
    )
    
    sacct_output = subprocess.check_output(sacct_command.split(' '), universal_newlines=True)
    sacct_results = SlurmParser.parse_output(sacct_output, convert_key=to_snake_case)

    output = {}

    for job in sacct_results:
        if account_list:
            if job['account'] not in account_list:
                continue

        __preprocess_job(job)
        
        data = { field: job[field] for field in groups_by_fields }
        
        output_ptr = output
        for key in groups_by:
            if job[key] in output_ptr:
                output_ptr = output_ptr[job[key]]
            else:
                output_ptr[job[key]] = {}
                output_ptr = output_ptr[job[key]]

        if output_ptr:
            for key in data:
                output_ptr[key] = output_ptr[key] + data[key]
                output_ptr['__count'] = output_ptr['__count'] + 1
        else:
            output_ptr.update(data)
            output_ptr['__count'] = 1
            
    return output
            
                
        
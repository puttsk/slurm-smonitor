#!/usr/bin/env python
# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import re
import json

from pprint import pprint
from collections import OrderedDict

class SlurmParser(object):
    APPEND = 'APPEND'
    FIRST = 'FIRST'
    REPLACE = 'REPLACE'

    entry_regex = re.compile(r'(\w[^\s=]+)=([^\s]*)')
    subentry_regex = re.compile(r'(\w[^\s=]+)=([^\s,]*)')
    sreport_comment_regex = re.compile(r'^-*$')

    @staticmethod
    def __conv(v):
        if v in ['(null)', 'None']: 
            return None
        
        if type(v) is str:
            if v.isdigit():
                return int(v)
        
        return v.strip() 

    @staticmethod
    def parse_job_info(slurm_output, key=None):
        if type(slurm_output) is str:
            slurm_output = slurm_output.splitlines()

        if key: 
            output = {}
        else:
            output = []

        entry = None

        for line in slurm_output:
            leading_space = len(line) - len(line.lstrip())
            line = line.lstrip()

            if len(line) == 0 and leading_space == 0:
                continue

            # New scontrol entry
            if len(line) > 0 and leading_space == 0:
                if entry:
                    if key:
                        output[entry[key]] = entry    
                    else:
                        output.append(entry)

                entry = {}

            params_list = SlurmParser.entry_regex.findall(line)
            params = {y[0]:(SlurmParser.__conv(y[1]) if y[1] else None) for y in params_list}
            for p in params:
                if params[p]:
                    subentry_list = SlurmParser.subentry_regex.findall(params[p])
                    if len(subentry_list) > 0:
                        params[p] = {y[0]:(SlurmParser.__conv(y[1]) if y[1] else None) for y in subentry_list}
                if entry.get(p, None):
                    if type(entry[p]) is list:
                        entry[p].append(params[p])
                    else:
                        entry[p] = [entry[p], params[p]]
                else:
                    entry[p] = params[p]

        if entry:
            if key:
                output[entry[key]] = entry    
            else:
                output.append(entry)

        return output

    @staticmethod
    def parse_output(slurm_output, headers=None, key=None, key_conflict=APPEND):
        if type(slurm_output) is str:
            slurm_output = slurm_output.splitlines()

        # Remove sreport information sections between two horizontal lines.
        #
        # Example:
        # --------------------------------------------------------------------------------
        # Cluster Utilization 2019-03-26T00:00:00 - 2019-03-26T23:59:59
        # Usage reported in CPU Minutes
        # --------------------------------------------------------------------------------
        # Cluster|Allocated|Down|PLND Down|Idle|Reserved|Reported
        # tara|3084213|0|0|2047833|1203954|6336000
        #
        
        if SlurmParser.sreport_comment_regex.match(slurm_output[0]):
            slurm_output.pop(0)
            while not SlurmParser.sreport_comment_regex.match(slurm_output[0]):
                slurm_output.pop(0)
            slurm_output.pop(0)

        if not headers:
            headers = slurm_output.pop(0).split('|')

        output_map = [zip(headers,line.split('|')) for line in slurm_output]

        if not key:            
            return [{k.strip():SlurmParser.__conv(v) for k,v in record} for record in output_map]

        if key: 
            ret = {}
            for d in ([{k.strip():SlurmParser.__conv(v) for k,v in record} for record in output_map]):
                if ret.get(d[key], None):
                    if key_conflict == SlurmParser.APPEND:
                        if type(ret[d[key]]) is dict: 
                            ret[d[key]] = [ret[d[key]], d]
                        else:
                            ret[d[key]].append(d)
                    elif key_conflict == SlurmParser.FIRST:
                        continue
                    elif key_conflict == SlurmParser.REPLACE:
                        ret[d[key]] = d   
                    else:
                        raise ValueError('Invalid key_conflict.') 
                else:
                    ret[d[key]] = d
            return ret
#!/usr/bin/env python
# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import re
import json

from pprint import pprint
from collections import OrderedDict

class SlurmResult(object):
    def __init__(self, results, fields=None):
        self.results = results
        self.fields = fields

class SlurmParser(object):
    APPEND = 'APPEND'
    FIRST = 'FIRST'
    REPLACE = 'REPLACE'

    entry_regex = re.compile(r'(\w[^\s=]+)=([^\s]*)')
    tres_regex = re.compile(r'(\w[^\s=]+)=([^\s,]*)')

    @staticmethod
    def __conv(v):
        return v.strip() if v not in ['(null)', 'None'] else None

    @staticmethod
    def parse_job_info(slurm_output):
        if type(slurm_output) is str:
            slurm_output = slurm_output.splitlines()

        output = []
        entry = None

        for line in slurm_output:
            print(line)
            leading_space = len(line) - len(line.lstrip())
            line = line.lstrip()

            if len(line) == 0 and leading_space == 0:
                continue

            # New entry
            if len(line) > 0 and leading_space == 0:
                if entry:
                    output.append(entry)
                    break
                entry = {}
                

            params_list = SlurmParser.entry_regex.findall(line)
            print(params_list)
            params = {y[0]:(SlurmParser.__conv(y[1]) if y[1] else None) for y in params_list}
            for p in params:
                if params[p]:
                    tres_list = SlurmParser.tres_regex.findall(params[p])
                    if len(tres_list) > 0:
                        params[p] = {y[0]:(SlurmParser.__conv(y[1]) if y[1] else None) for y in tres_list}
            
            entry.update(params)
        return SlurmResult(output)

    @staticmethod
    def parse_output(slurm_output, headers=None, key=None, key_conflict=APPEND):
        if type(slurm_output) is str:
            slurm_output = slurm_output.splitlines()

        if not headers:
            headers = slurm_output.pop(0).split('|')

        output_map = [zip(headers,line.split('|')) for line in slurm_output]

        if not key:            
            return SlurmResult([{k.strip():SlurmParser.__conv(v) for k,v in record} for record in output_map], fields=header)

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
            return SlurmResult(ret, fields=headers)
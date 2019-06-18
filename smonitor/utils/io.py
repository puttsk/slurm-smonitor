# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import json
import types
import csv

from pprint import pprint

def generate_output(data, format='table', output_file=None, indent_size=4):
    if format == 'json':
        if output_file:
            with open(output_file, 'wt') as f:
                if isinstance(data, types.GeneratorType):
                    data = list(data)
                json.dump(data, f, indent=indent_size, sort_keys=True)
        else:
            # Print output to STDOUT
            if isinstance(data, types.GeneratorType):
                data = list(data)
            print(json.dumps(data, indent=indent_size, sort_keys=True))
    elif format == 'csv':
        if output_file:
            with open(output_file, 'wb') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                for d in data:
                    writer.writerow(d)
    else:
        if isinstance(data, types.GeneratorType) or isinstance(data, list):
            __print_table_from_list(data, indent_size)
        else:
            raise NotImplementedError()

def __print_table_from_list(data, indent_size=4):
    headers = None
    if isinstance(data, types.GeneratorType):
        d = data.next()
        if isinstance(d, dict):
            headers = d.keys()
    elif isinstance(data, list):
        d = data[0]
        if isinstance(d, dict):
            headers = d.keys()
    
    data_width = [len(str(x)) if len(str(x)) > 12 else 12 for x in [d[y] for y in headers]]
    header_width = [len(x) for x in headers]
    
    col_width = [max(x,y) for x,y in zip(data_width, header_width)]
    columns = zip(headers, col_width)
    
    line = '|'
    separator = '+'
    for c in columns: 
        line = line + ' {:>{width}.{width}s} |'.format(c[0], width=c[1])
        separator = separator + '-'*(c[1]+2) + '+'

    print(separator)
    print(line)
    print(separator)

    line = '|'

    if isinstance(data, types.GeneratorType):
        for c in columns: 
            line = line + ' {:>{width}.{width}s} |'.format(str(d[c[0]]), width=c[1])
        print(line)

    for d in data:
        line = '|'
        for c in columns: 
            line = line + ' {:>{width}.{width}s} |'.format(str(d[c[0]]), width=c[1])
        print(line)
    
    print(separator)    
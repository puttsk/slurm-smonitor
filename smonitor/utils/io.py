# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import json
import types

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
    else:
        headers = None
        if isinstance(data, types.GeneratorType):
            d = data.next()
            if isinstance(d, dict):
                headers = d.keys()

        
        data_width = [len(str(x)) for x in [d[y] for y in headers]]
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
        for c in columns: 
            line = line + ' {:>{width}.{width}s} |'.format(str(d[c[0]]), width=c[1])
        print(line)

        for d in data:
            line = '|'
            for c in columns: 
                line = line + ' {:>{width}.{width}s} |'.format(str(d[c[0]]), width=c[1])
            print(line)
        
        print(separator)
        

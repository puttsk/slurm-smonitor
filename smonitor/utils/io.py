# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import json
import types

from pprint import pprint

def generate_output(data, output_format, output_file=None, indent_size=4):
    if output_format == 'json':
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
        pprint([x for x in data])
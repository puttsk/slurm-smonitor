# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import json

from pprint import pprint

def generate_output(data, output_format, output_file=None):
    if output_format == 'json':
        if output_file:
            with open(output_file, 'wt') as f:
                json.dump(data, f, indent=4, sort_keys=True)
        else:
            print(json.dumps(data, indent=4, sort_keys=True))
    else:
        pprint(data)
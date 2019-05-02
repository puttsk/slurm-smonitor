# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

import re

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')

def to_snake_case(val):
    s1 = first_cap_re.sub(r'\1_\2', val)
    return all_cap_re.sub(r'\1_\2', s1).lower()

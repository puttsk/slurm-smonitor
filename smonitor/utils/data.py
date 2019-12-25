# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
from __future__ import print_function

from pprint import pprint

def flattern_nested_dict(d, outer_fields=[], outer_data=[], field_name_key='fields'):
    if field_name_key in d:
        field_name = d[field_name_key]
        inner_field = []
        inner_data = []

        output = []

        for key in d:
            if key == field_name_key:
                continue
            if not isinstance(d[key], dict):
                inner_field.append(key)
                inner_data.append(d[key])

        for key in d:
            if isinstance(d[key], dict):
                output +=   flattern_nested_dict(
                                d[key], 
                                outer_fields= outer_fields + inner_field + [field_name],
                                outer_data= outer_data + inner_data + [key]
                            )
        return output

    else:
        output_fields = outer_fields + d.keys()
        output_data = outer_data + d.values()
    
        return [{k:v for k,v in zip(output_fields, output_data)}]
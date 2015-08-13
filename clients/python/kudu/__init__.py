# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

from kudu.client import *

def schema_from_list(columns, num_key_columns):
    return Schema.create(columns, num_key_columns)

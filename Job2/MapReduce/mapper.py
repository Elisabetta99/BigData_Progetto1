#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    line = line.strip()
    columns = line.split(',')

    try:
        user_id = columns[2]
        helpfulness_num = int(columns[4])
        helpfulness_den = int(columns[5])

        if helpfulness_den != 0:
            usefulness = helpfulness_num / helpfulness_den
            print('%s \t%s' % (user_id, usefulness))

    except(ValueError, IndexError):
        pass


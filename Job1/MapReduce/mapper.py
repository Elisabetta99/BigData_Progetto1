#!/usr/bin/env python3
"""mapper.py"""

import sys
import re
import string
from datetime import datetime

for line in sys.stdin:
        
        line = line.strip().split(",")
        try:
            productId = line[1].strip()
            time = line[7].strip()
            text = line[9].strip()

            time = datetime.utcfromtimestamp(int(time)).strftime('%Y')

            print("%s\t%s\t%s\t%i" % (productId, time, text, 1))
        except:
            pass



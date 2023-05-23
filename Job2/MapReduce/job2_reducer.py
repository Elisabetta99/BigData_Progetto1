#!/usr/bin/env python3
"""reducer.py"""

import sys

userAppreciation = {}

for line in sys.stdin:
    line = line.strip()
    user_id, usefulness = line.split('\t')

    try:
        usefulness = float(usefulness)

        if user_id not in userAppreciation:
            userAppreciation[user_id] = {
                'usefulness_sum': usefulness,
                'review_count': 1
            }
        else:
            userAppreciation[user_id]['usefulness_sum'] += usefulness
            userAppreciation[user_id]['review_count'] += 1

    except ValueError:
        pass

for user in userAppreciation:
    usefulness_sum = userAppreciation[user]['usefulness_sum']
    review_count = userAppreciation[user]['review_count']

    if review_count != 0:
        appreciation = (usefulness_sum / review_count)
    else:
        appreciation = 0

    userAppreciation[user] = appreciation

sorted_user = sorted(userAppreciation.items(), key=lambda x: (x[1], x[0]), reverse=True)

for user, appreciation in sorted_user:
    print("%s \t%s" % (user, appreciation))

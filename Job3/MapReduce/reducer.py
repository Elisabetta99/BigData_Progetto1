#!/usr/bin/env python3
"""reducer.py"""

import sys
from itertools import combinations, groupby

userProducts = {}
affinityGroups = []
processedUsers = set()

for line in sys.stdin:
    productId, userId = line.split("\t")

    try:
        if userId not in userProducts:
            userProducts[userId] = set()
        userProducts[userId].add(productId)

    except ValueError:
        pass

for user1 in userProducts:
    if user1 in processedUsers:
        continue

    group = {user1}
    commonProducts = userProducts[user1]

    for user2 in userProducts:
        if user1 != user2 and user2 not in processedUsers:
            if len(commonProducts.intersection(userProducts[user2])) >= 3:
                group.add(user2)
                commonProducts = commonProducts.intersection(userProducts[user2])
                processedUsers.add(user2)

    if len(group) > 1:
        affinityGroups.append((sorted(list(group)), commonProducts))
        processedUsers.update(group)

sortedGroups = sorted(affinityGroups, key=lambda x: x[0][0])

for group, commonProducts in sortedGroups:
    print("Group: %s" % ", ".join(group))
    print("Common Products:")

    for product in commonProducts:
        print(product)
    print()

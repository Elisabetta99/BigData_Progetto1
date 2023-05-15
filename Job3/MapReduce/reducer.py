#!/usr/bin/env python3
"""reducer.py"""

import sys
from collections import defaultdict

# Dizionario per memorizzare i prodotti recensiti da ogni utente
userProducts = defaultdict(set)

# Leggi i dati di input dal mapper
for line in sys.stdin:
    # Analizza la riga in userId e productId
    userId, productId = line.strip().split("\t")

    # Aggiungi il productId all'utente corrispondente
    userProducts[userId].add(productId)

# Filtra gli utenti che hanno recensito almeno 3 prodotti
filteredUsers = [userId for userId, products in userProducts.items() if len(products) >= 3]

# Dizionario per memorizzare gli utenti con gusti affini
affinityGroups = []

# Trova gli utenti con 3 prodotti in comune
for i, user1 in enumerate(filteredUsers):
    group = {user1}
    commonProducts = userProducts[user1]

    for user2 in filteredUsers[i + 1:]:
        if len(commonProducts.intersection(userProducts[user2])) >= 3:
            group.add(user2)
            commonProducts = commonProducts.intersection(userProducts[user2])

    if len(group) > 1:
        affinityGroups.append((sorted(list(group)), commonProducts))

# Ordina gli affinity group in base all'UserId del primo elemento del gruppo
sortedGroups = sorted(affinityGroups, key=lambda x: x[0][0])

# Rimuovi i duplicati dai gruppi
uniqueGroups = []
processedUsers = set()

for group, commonProducts in sortedGroups:
    if not any(user in processedUsers for user in group):
        uniqueGroups.append((group, commonProducts))
        processedUsers.update(group)

# Stampa il risultato
for group, commonProducts in uniqueGroups:
    print("Group: %s" % ", ".join(group))
    print("Common Products:")
    for product in commonProducts:
        print(product)
    print()

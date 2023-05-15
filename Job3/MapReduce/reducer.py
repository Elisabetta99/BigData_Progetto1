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
    for user2 in filteredUsers[i + 1:]:
        commonProducts = userProducts[user1].intersection(userProducts[user2])
        if len(commonProducts) >= 3:
            affinityGroups.append((user1, user2, commonProducts))

# Ordina gli affinity group in base all'UserId del primo elemento del gruppo
sortedGroups = sorted(affinityGroups, key=lambda x: x[0])

# Rimuovi i duplicati dai gruppi
uniqueGroups = []
processedUsers = set()

for group in sortedGroups:
    user1, user2, commonProducts = group
    if user1 not in processedUsers and user2 not in processedUsers:
        uniqueGroups.append(group)
        processedUsers.add(user1)
        processedUsers.add(user2)

# Stampa il risultato
for group in uniqueGroups:
    user1, user2, commonProducts = group
    print("Group: %s, %s" % (user1, user2))
    print("Common Products:")
    for product in commonProducts:
        print(product)
    print()

#!/usr/bin/env python3
"""reducer.py"""

import sys

reviews_year = {}

def words_count(product, testo):
	dict_word = {}
	parole = testo.split()
	for p in parole:
		if ( len(p) >=4):
			if p not in dict_word:
				dict_word[p] = 1
			else:
				dict_word[p] += 1
		else:
			continue

	return dict_word



for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip().split("\t")

    # parse the input elements
    #productId, time, text, val  = line.split(",")

    productId = line[0]
    time = line[1]
    text = line[2]
    val = line[-1]

    try:
        val = int(val)
        time = int(time)
    except ValueError:
        continue

    if time not in reviews_year:
        reviews_year[time] = {}



    if productId not in reviews_year[time]:
    	reviews_year[time][productId] = {}
    	reviews_year[time][productId][0] = val
    	reviews_year[time][productId][1] = text
    else:
    	reviews_year[time][productId][0] += val
    	reviews_year[time][productId][1] += text


for time in sorted(reviews_year, reverse=True):
	print('%i' % (time))
	for productId in sorted(reviews_year[time].items() , key=lambda x: x[1][0], reverse = True)[:10]:
		dictWords = words_count(productId[0],reviews_year[time][productId[0]][1])
		print('\t%s\t%i\t' % (productId[0], reviews_year[time][productId[0]][0]))

		for parole in sorted(dictWords.items(), key = lambda x: x[1], reverse = True)[:5]:
			print('\t%s\t%i' % (parole[0], parole[1] ))




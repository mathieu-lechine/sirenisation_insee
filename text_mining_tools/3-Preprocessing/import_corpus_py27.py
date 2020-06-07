#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  8 17:31:34 2018

@author: mathieu.lechine
"""

import nltk
nltk.download('all')

#--------------------  Brown Corpus -------------------------------------------
#load the Brown Corpus
from nltk.corpus import brown

print(brown.categories())

#tokenized sentences
sentences = brown.sents(categories='mystery')
sentences = [' '.join(s) for s in sentences]
# POS tagged sentences
brown.tagged_sents(categories='mystery')

#count the most frequent nouns in the mystery
tagged_words = brown.tagged_sents(categories='mystery')
flatten_list = lambda l: [item for sublist in l for item in sublist]
tagged_words = flatten_list(tagged_words)
#get nouns from tagged words: NN or NP
nouns = [(word, tag) for word, tag in tagged_words if tag in set(['NN', 'NP'])] 
#build frequency distributions
nouns_freq = nltk.FreqDist([word for word, _ in nouns])
nouns_freq.most_common(10)


#-------------------- Reuters Corpus -------------------------------------------
#Common corpus for machine learning: texts are divided in Test and Training set
#load the Reuters Corpus
from nltk.corpus import reuters
print(reuters.categories())
#tokenized sentences
sentences = reuters.sents(categories='jobs')
sentences = [' '.join(s) for s in sentences]
print(sentences[:1])
#filesid based acces
print(reuters.fileids(categories=[u'housing', u'income']))
print(reuters.sents(fileids=[u"test/16118", u"training/1035"]))


#-------------------- Reuters Corpus -------------------------------------------
#Common corpus for machine learning: texts are divided in Test and Training set
#load the Reuters Corpus
from nltk.corpus import wordnet as wn
word = u'hike'
#get word synsets
word_synsets = wn.synsets(word)
print(word_synsets)
#get details for each synonym in synset
for synset in word_synsets:
    print('Name:', synset.name())
    print('POS tag:', synset.pos())
    print('Definition:', synset.definition())
    print('Examples:', synset.examples())


#The Zen of Python
import this

#Python refresher
#numeric types
a=4
id(a)
type(a)
print(a)
bin(a)
hex(4)
oct(4)
1e1+2
cnum = 5+7j
type(cnum)
cnum.real
cnum.imag

#map, reduce, filter
import time
def square(x):
    return(x*x)
l=list(range(10000))
start_time = time.time()
[square(x) for x in l]
print("Computation time: {}".format(time.time()-start_time))
start_time = time.time()
map(square, l)
print("Computation time: {}".format(time.time()-start_time))
#map faster than list comprehension
lambda_even = lambda x: x%2 == 0
start_time = time.time()
[x for x in l if x%2==0]
print("Computation time: {}".format(time.time()-start_time))
start_time = time.time()
filter(lambda_even, l)
print("Computation time: {}".format(time.time()-start_time))
#filter faster than list comprehension
#reduce
lambda_sum = lambda x,y: x+y
reduce(lambda_sum, l)

#iterators: rewrite a for loop with iterators 
numbers = range(6)
iter_obj = iter(numbers)
while True:
    try:
        print(next(iter_obj))
    except StopIteration:
        print("Reached en of sequence")
        break
#comprehension applies to list, set and dictionary
s=set([1,2, -2])
set(i*i for i in s)

#generators: memory-efficient and low execution time compared to list
numbers = range(6)
def generate_square(numbers):
    for number in numbers:
        yield number**2
gen_obj = generate_square(numbers)
for item in gen_obj:
    print(item)

gen_obj = (number**2 for number in numbers)
for item in gen_obj:
    print(item)



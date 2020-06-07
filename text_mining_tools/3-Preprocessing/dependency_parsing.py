#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 20:48:26 2018

@author: mathieu.lechine
"""

#%%
#*****************************************************************************
# DEPENDENCY PARSER
#*****************************************************************************

sentence = 'The brown fox is quick and he is jumping over the lazy dog'

from spacy.lang.en import English
parser = English()
parsed_sent = parser(unicode(sentence))

dependency_pattern = '{left}<---{word}[{w_type}]--->{right}\n--------'
for token in parsed_sent:
    print dependency_pattern.format(word=token.orth_, 
                                  w_type=token.dep_,
                                  left=[t.orth_ 
                                            for t 
                                            in token.lefts],
                                  right=[t.orth_ 
                                             for t 
                                             in token.rights])
                                             








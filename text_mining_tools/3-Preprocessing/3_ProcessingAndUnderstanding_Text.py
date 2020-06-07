#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 16:34:33 2018

@author: mathieu.lechine
"""

import inspect
import os
import time

#inspect.getsource() #get the source code of the function
#print(nltk.tag.__file__) #get the filename of the source code
#Ressource NLTK: http://www.ling.helsinki.fi/kit/2009s/clt231/NLTK/book/

os.getcwd()
os.chdir("/Users/mathieu.lechine/Google Drive/Lexbase/Text_Analytics/")
#%%
#*****************************************************************************
# TEXT TOKENIZATION: Sentence Tokenization
#*****************************************************************************

import nltk
from nltk.corpus import gutenberg
from pprint import pprint

alice = gutenberg.raw(fileids=u'carroll-alice.txt')
print "Number of characters: {}".format(len(alice))

#nltk.sent_tokenize: trained on several languages
default_st = nltk.sent_tokenize
alice_sentences = default_st(text=alice, language='english') #german, french
print 'Number of sentences: {}'.format(len(alice_sentences))

#PunktSentenceTokenizer is the abstract class for the default sentence tokenizer, 
#i.e. sent_tokenize(), provided in NLTK. It is an implmentation of Unsupervised 
#Multilingual Sentence Boundary Detection (Kiss and Strunk (2005). 

#RegexpTokenizer for regular expressions


#%%
#*****************************************************************************
# TEXT TOKENIZATION: Word Tokenization
#*****************************************************************************
sentence = "The brown fox wasn't that quick and he couldn't win the race"

# default word tokenizer
default_wt = nltk.word_tokenize
words = default_wt(sentence, language="english")
print words
    
# treebank word tokenizer
treebank_wt = nltk.TreebankWordTokenizer()
words = treebank_wt.tokenize(sentence)
print words

#other word tokenizer based on user-defined regex: WordPunctTokenizer, WhitespaceTokenizer

#%%
#*****************************************************************************
# TEXT NORMALIZATION (text cleansing/wrangling)
#*****************************************************************************

import nltk
import re
import string
from pprint import pprint

corpus = ["The brown fox wasn't that quick and he couldn't win the race",
          "Hey that's a great deal! I just bought a phone for $199",
          "@@You'll (learn) a **lot** in the book. Python is an amazing language!@@"]

#Tokenizing Text
def tokenize_text(text, language="english"):
    sentences = nltk.sent_tokenize(text=text, language=language)
    word_tokens = [nltk.word_tokenize(sentence, language=language) for sentence in sentences]
    return word_tokens

token_list = [tokenize_text(text) for text in corpus]

#%%
#*****************************************************************************
# REMOVING SPECIAL CHARACTERS
#*****************************************************************************

#remove special characters after tokenization
def remove_characters_after_tokenization(tokens):
    pattern = re.compile('[{}]'.format(re.escape(string.punctuation)))
    filtered_tokens = filter(None, [pattern.sub('', token) for token in tokens])
    return filtered_tokens
    
filtered_list_1 =  [filter(None,[remove_characters_after_tokenization(tokens) 
                                for tokens in sentence_tokens]) 
                    for sentence_tokens in token_list]
print filtered_list_1

#remove special characters before tokenization
def remove_characters_before_tokenization(sentence,
                                          keep_apostrophes=False):
    sentence = sentence.strip()
    if keep_apostrophes:
        PATTERN = r'[?|$|&|*|%|@|(|)|~]'
        filtered_sentence = re.sub(PATTERN, r'', sentence)
    else:
        PATTERN = r'[^a-zA-Z0-9 ]'
        filtered_sentence = re.sub(PATTERN, r'', sentence)
    return filtered_sentence
    
filtered_list_2c = [remove_characters_before_tokenization(sentence) 
                    for sentence in corpus]    
print filtered_list_2

cleaned_corpus = [remove_characters_before_tokenization(sentence, keep_apostrophes=True) 
                  for sentence in corpus]
print cleaned_corpus

#%%
#*****************************************************************************
# EXPANDING CONTRACTIONS
#*****************************************************************************

from contractions import CONTRACTION_MAP
    
def expand_contractions(sentence, contraction_mapping):
    
    contractions_pattern = re.compile('({})'.format('|'.join(contraction_mapping.keys())), 
                                      flags=re.IGNORECASE|re.DOTALL)
    def expand_match(contraction):
        match = contraction.group(0)
        first_char = match[0]
        expanded_contraction = contraction_mapping.get(match)\
                                if contraction_mapping.get(match)\
                                else contraction_mapping.get(match.lower())  
        #keep the first character which can be in capital letter
        expanded_contraction = first_char+expanded_contraction[1:]
        return expanded_contraction
        
    expanded_sentence = contractions_pattern.sub(expand_match, sentence)
    return expanded_sentence
    
expanded_corpus = [expand_contractions(sentence, CONTRACTION_MAP) 
                    for sentence in cleaned_corpus]    
print expanded_corpus
    
#%%
#*****************************************************************************
# CASE CONVERSION: upper or lower
#*****************************************************************************

print expanded_corpus[0].lower()
print expanded_corpus[0].upper()

expanded_corpus = [sentence.lower() for sentence in expanded_corpus]

#%%
#*****************************************************************************
# REMOVING STOPWORDS: 
# !!!! negations will be removed -> don't remove stop words for sentiment 
# analysis
#*****************************************************************************

def remove_stopwords(tokens):
    stopword_list = nltk.corpus.stopwords.words('english')
    filtered_tokens = [token for token in tokens if token not in stopword_list]
    return filtered_tokens

expanded_corpus_tokens = [tokenize_text(sentence) for sentence in expanded_corpus]
filtered_list_3 = [remove_stopwords(tokens for tokens in expanded_corpus_tokens)]

#%%
#*****************************************************************************
# CORRECTING REPEATING CHARACTERS 
#*****************************************************************************
sample_sentence = 'My schooool is realllllyyy amaaazingggg'
sample_sentence_tokens = tokenize_text(sample_sentence)[0]

from nltk.corpus import wordnet

def remove_repeated_characters(tokens):
    repeat_pattern = re.compile(r'(\w*)(\w)\2(\w*)')
    match_substitution = r'\1\2\3'
    def replace(old_word):
        if wordnet.synsets(old_word, lang=u'eng'): #presence of the word in dictionary
            return old_word
        new_word = repeat_pattern.sub(match_substitution, old_word)
        return replace(new_word) if new_word != old_word else new_word
            
    correct_tokens = [replace(word) for word in tokens]
    return correct_tokens

print remove_repeated_characters(sample_sentence_tokens) 
#contre-exemple
remove_repeated_characters([u"reaally", u"reallyy"])
#we can improve the function to test all possibilities: removing and adding repeating characters

#%%
#*****************************************************************************
# CORRECTING SPELLINGS 
#*****************************************************************************
#Google director of research: http://norvig.com/spell-correct.html

import pattern.en
#documentation: https://www.clips.uantwerpen.be/pages/pattern
print pattern.en.suggest("Fianlly")
print pattern.en.suggest("flaot")
#the function suggest is not available for french
import pattern.fr

#enchant library (almost no online documentation)
import enchant
import enchant.checker
help(enchant)
a = "Ceci est un text avec beuacuop d'ereurs et pas snychro"
chkr = enchant.checker.SpellChecker("fr_FR")
chkr.set_text(a)
for err in chkr:
    print err.word
    sug = err.suggest()[0]
    print sug
    err.replace(sug)
c = chkr.get_text()#returns corrected text
print c

#%%
#*****************************************************************************
# STEMMING 
#*****************************************************************************
ls_test = [u"strange", u"lying", u"down", u"gave", "dead", u"given", u"gived", u"uncomfortable"]
#Most famous stemmer: Porter Stemmer
from nltk.stem import PorterStemmer
ps = PorterStemmer(mode=u'NLTK_EXTENSIONS') 
start_time = time.time()
print map(ps.stem,ls_test)
print "Time: {}".format(time.time()-start_time)

#Lancaster Stemmer: a little bit faster than PorterStemmer
from nltk.stem import LancasterStemmer
ls = LancasterStemmer()
start_time = time.time()
print map(ls.stem,ls_test)
print "Time: {}".format(time.time()-start_time)

#user-defined stemmer with regex
from nltk.stem import RegexpStemmer
rs = RegexpStemmer('ing$|s$|ed$', min=4)
print map(rs.stem,ls_test)

#Snowball: http://snowballstem.org/ (the fastest+multilanguage)
from nltk.stem import SnowballStemmer
print 'Supported Languages:', SnowballStemmer.languages
ss = SnowballStemmer("english")
start_time = time.time()
print map(ls.stem,ls_test)
print "Time: {}".format(time.time()-start_time)

ss = SnowballStemmer("french")
print map(ls.stem,[u"mangea", u"hérissons", u"boeufs", u"virent"])

#%%
#*****************************************************************************
# LEMMATIZATION
# similar to Stemming but slower
# the (root) lemma is always a word in the dictionary
#*****************************************************************************
ls_test_w = [u'cars', u'men', u'running', u'ate', u'saddest']
ls_test_pos = ['n', 'n', u'v', u'v', u'a']

from nltk.stem import WordNetLemmatizer
wnl = WordNetLemmatizer()
start_time = time.time()
print map(wnl.lemmatize, ls_test_w,ls_test_pos)
print "Time: {}".format(time.time()-start_time)
#no french lemmatizer apparently

#https://spacy.io/usage/
#https://spacy.io/docs/#getting-started
import spacy

#%%
#*****************************************************************************
# POS TAGGING
#*****************************************************************************
#recommended POS-tagger

sentence = u'The brown fox is quick and he is jumping over the lazy dog'
sentence_fr = u"Le renard brun est rapide et il saute sur le chien fainéant"

# recommended tagger based on PTB
import nltk
tokens = nltk.word_tokenize(sentence)
tagged_sent = nltk.pos_tag(tokens, tagset='universal')
print tagged_sent

from pattern.en import tag
tagged_sent = tag(sentence)
print tagged_sent

from pattern.fr import tag
tagged_sent = tag(sentence_fr)
print tagged_sent #les résultats ne sont pas super sur la phrase française


#Build your own POS Taggers
# preparing the data
from nltk.corpus import treebank
data = treebank.tagged_sents()
train_data = data[:3000]
test_data = data[3000:]
print train_data[0]

# default tagger
from nltk.tag import DefaultTagger
dt = DefaultTagger('NN')
print dt.evaluate(test_data)
print dt.tag(tokens)

# regex tagger
from nltk.tag import RegexpTagger
# define regex tag patterns
patterns = [
        (r'.*ing$', 'VBG'),               # gerunds
        (r'.*ed$', 'VBD'),                # simple past
        (r'.*es$', 'VBZ'),                # 3rd singular present
        (r'.*ould$', 'MD'),               # modals
        (r'.*\'s$', 'NN$'),               # possessive nouns
        (r'.*s$', 'NNS'),                 # plural nouns
        (r'^-?[0-9]+(.[0-9]+)?$', 'CD'),  # cardinal numbers
        (r'.*', 'NN')                     # nouns (default) ... 
]
rt = RegexpTagger(patterns)
print rt.evaluate(test_data)
print rt.tag(tokens)    

## N gram taggers
from nltk.tag import UnigramTagger
from nltk.tag import BigramTagger
from nltk.tag import TrigramTagger
ut = UnigramTagger(train_data)
bt = BigramTagger(train_data)
tt = TrigramTagger(train_data)
print ut.evaluate(test_data)
print ut.tag(tokens)
print bt.evaluate(test_data)
print bt.tag(tokens)
print tt.evaluate(test_data)
print tt.tag(tokens)
## N gram taggers combined
def combined_tagger(train_data, taggers, backoff=None):
    for tagger in taggers:
        backoff = tagger(train_data, backoff=backoff)
    return backoff
ct = combined_tagger(train_data=train_data, 
                     taggers=[UnigramTagger, BigramTagger, TrigramTagger],#[UnigramTagger, BigramTagger, TrigramTagger]
                     backoff=rt)
print ct.evaluate(test_data)        
print ct.tag(tokens)

#Supervised tagger
from nltk.classify import NaiveBayesClassifier, MaxentClassifier
from nltk.tag.sequential import ClassifierBasedPOSTagger
nbt = ClassifierBasedPOSTagger(train=train_data,
                               classifier_builder=NaiveBayesClassifier.train)
print nbt.evaluate(test_data)
print nbt.tag(tokens)    


# try this out for fun!
#met = ClassifierBasedPOSTagger(train=train_data,
#                               classifier_builder=MaxentClassifier.train)
#print met.evaluate(test_data)                           
#print met.tag(tokens)


#%%
#*****************************************************************************
# SHALLOW PARSING
#*****************************************************************************
#recommended POS-tagger: visualisation of the tree with NLTK

from pattern.en import parsetree, Chunk
from nltk.tree import Tree

sentence = 'The brown fox is quick and he is jumping over the lazy dog'

tree = parsetree(sentence)
print tree
#IOB notation: Inside Outside Beginning

for sentence_tree in tree:
    print sentence_tree.chunks
    
for sentence_tree in tree:
    for chunk in sentence_tree.chunks:
        print chunk.type, '->', [(word.string, word.type) 
                                 for word in chunk.words]
        

def create_sentence_tree(sentence, lemmatize=False):
    sentence_tree = parsetree(sentence, 
                              relations=True, 
                              lemmata=lemmatize)
    return sentence_tree[0]
    
def get_sentence_tree_constituents(sentence_tree):
    return sentence_tree.constituents()
    
def process_sentence_tree(sentence_tree):
    
    tree_constituents = get_sentence_tree_constituents(sentence_tree)
    processed_tree = [
                        (item.type,
                         [
                             (w.string, w.type)
                             for w in item.words
                         ]
                        )
                        if type(item) == Chunk
                        else ('-',
                              [
                                   (item.string, item.type)
                              ]
                             )
                             for item in tree_constituents
                    ]
    
    return processed_tree
    
def print_sentence_tree(sentence_tree):
    

    processed_tree = process_sentence_tree(sentence_tree)
    processed_tree = [
                        Tree( item[0],
                             [
                                 Tree(x[1], [x[0]])
                                 for x in item[1]
                             ]
                            )
                            for item in processed_tree
                     ]

    tree = Tree('S', processed_tree )
    print tree
    
def visualize_sentence_tree(sentence_tree):
    
    processed_tree = process_sentence_tree(sentence_tree)
    processed_tree = [
                        Tree( item[0],
                             [
                                 Tree(x[1], [x[0]])
                                 for x in item[1]
                             ]
                            )
                            for item in processed_tree
                     ]
    tree = Tree('S', processed_tree )
    tree.draw()
    
    
t = create_sentence_tree(sentence)
print t

pt = process_sentence_tree(t)
pt

print_sentence_tree(t)
visualize_sentence_tree(t)

#%%
#Building your own shallow parsing

from nltk.corpus import treebank_chunk
data = treebank_chunk.chunked_sents()
train_data = data[:4000]
test_data = data[4000:]
print train_data[7]

simple_sentence = 'the quick fox jumped over the lazy dog'

from nltk.chunk import RegexpParser
from pattern.en import tag

tagged_simple_sent = tag(simple_sentence)
print tagged_simple_sent

chunk_grammar = """
NP: {<DT>?<JJ>*<NN.*>}
"""
rc = RegexpParser(chunk_grammar)
c = rc.parse(tagged_simple_sent)
print c

chink_grammar = """
NP: {<.*>+} # chunk everything as NP
}<VBD|IN>+{
"""
rc = RegexpParser(chink_grammar)
c = rc.parse(tagged_simple_sent)
print c

tagged_sentence = tag(sentence)
print tagged_sentence

grammar = """
NP: {<DT>?<JJ>?<NN.*>}  
ADJP: {<JJ>}
ADVP: {<RB.*>}
PP: {<IN>}      
VP: {<MD>?<VB.*>+}
"""
rc = RegexpParser(grammar)
c = rc.parse(tagged_sentence)

print c

print rc.evaluate(test_data)


   


from nltk.chunk.util import tree2conlltags, conlltags2tree

train_sent = train_data[7]
print train_sent

wtc = tree2conlltags(train_sent)
wtc

tree = conlltags2tree(wtc)
print tree
    

def conll_tag_chunks(chunk_sents):
  tagged_sents = [tree2conlltags(tree) for tree in chunk_sents]
  return [[(t, c) for (w, t, c) in sent] for sent in tagged_sents]
  
def combined_tagger(train_data, taggers, backoff=None):
    for tagger in taggers:
        backoff = tagger(train_data, backoff=backoff)
    return backoff
  
from nltk.tag import UnigramTagger, BigramTagger
from nltk.chunk import ChunkParserI

class NGramTagChunker(ChunkParserI):
    
  def __init__(self, train_sentences, 
               tagger_classes=[UnigramTagger, BigramTagger]):
    train_sent_tags = conll_tag_chunks(train_sentences)
    self.chunk_tagger = combined_tagger(train_sent_tags, tagger_classes)

  def parse(self, tagged_sentence):
    if not tagged_sentence: 
        return None
    pos_tags = [tag for word, tag in tagged_sentence]
    chunk_pos_tags = self.chunk_tagger.tag(pos_tags)
    chunk_tags = [chunk_tag for (pos_tag, chunk_tag) in chunk_pos_tags]
    wpc_tags = [(word, pos_tag, chunk_tag) for ((word, pos_tag), chunk_tag)
                     in zip(tagged_sentence, chunk_tags)]
    return conlltags2tree(wpc_tags)


ntc = NGramTagChunker(train_data)
print ntc.evaluate(test_data)

tree = ntc.parse(tagged_sentence)
print tree
tree.draw()


from nltk.corpus import conll2000
wsj_data = conll2000.chunked_sents()
train_wsj_data = wsj_data[:7500]
test_wsj_data = wsj_data[7500:]
print train_wsj_data[10]

tc = NGramTagChunker(train_wsj_data)
print tc.evaluate(test_wsj_data)

















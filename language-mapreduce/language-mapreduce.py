#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
    language-mapreduce: tokenizes text, counts token neighbor and sentence position
    Copyright (C) 2012  David Van de Ven

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses
''' 
import string
import cPickle as pickle
import gzip
import os
import argparse
import multiprocessing
from multiprocessing import Pool, Manager
import mmap
import contextlib
import sys
import re
from types import *
import itertools
import collections

num_cpus = multiprocessing.cpu_count()
crap_threshold = 44
sentence_list = []
output_dir = "pyprob_maps"
current_dir = os.getcwd()
dict_file = '-language_map.pkl.gz'
parser = argparse.ArgumentParser()
parser.add_argument('files', action='append', nargs='+',
		    help='Filenames to parse')
parser.add_argument('-top', '--top',
		    help='Number of sorted tokens to display as output'
		    , default=20, required=False)
parser.add_argument('-fslice', '--fslice',
		    help='Overrides cpu detection and sets number of slices for job'
		    , default=0, required=False)
parser.add_argument('-trimpositions', '--trimpositions',
		    help='drops all position entries during Reduce where position count < arg'
		    , default=0, required=False)
parser.add_argument('-trimneighbors', '--trimneighbors',
		    help='drops all neighbor entries during Reduce where neighbor count < arg'
		    , default=1, required=False)
		    
args = parser.parse_args()

'''
 Utility function for Mapper
 Increments token's total count and position count   
'''
def incrementToken(token_map, i, token):
    positions = token_map[1]
    count = token_map[0]+1
    neighbors = token_map[2]
    if not i > crap_threshold:
	cur_pos = positions.get(i,0)
	positions[i] = cur_pos +1
    return (count, positions , neighbors)

'''
 Utility function for Mapper
 Increment neighbor score for all words in sentence  
'''
def mapNeighbors(token_map, sentence, token):
    count = token_map[0]
    positions = token_map[1]
    neighbors = token_map[2]
    for word in sentence:
	if word.isalpha():
	    if not word == token:
		if not word in neighbors:
		    neighbors[word] = 1
		else:
		    neighbors[word] +=1
		
    return (count, positions, neighbors)

def init(data):
    global sentence_list
    sentence_list = data
    
'''
    trims dicts with key,int where int < n
'''
def trimdict(tofilter, n):
    remove = [k for k in tofilter if tofilter[k] < n]
    for k in remove: del tofilter[k]
    return tofilter
'''
    Tokenizes and maps job slice
'''
def Map(L):
    local_map = collections.defaultdict(lambda:(0,{},{}))
    # Prepare and partition data file
    data_file = load (L[1])
    # Prepare local slice of sentence list
    offset = len(data_file)/num_cpus
    chunk = lambda L,n:L[n*offset:(n+1)*offset]
    # Parse sentences with tokenizer
    from pattern.en import parse
    #sentence_parser = SentenceTokenizer()
    #sentence_list = sentence_parser.segment_text(chunk(data_file,L[0]))
    sentence_list = parse(chunk(data_file,L[0]),
     tokenize = True,  # Tokenize the input, i.e. split punctuation from words.
         tags = False,  # Find part-of-speech tags.
       chunks = False,  # Find chunk tags, e.g. "the black cat" = NP = noun phrase.
    relations = False,  # Find relations between chunks.
      lemmata = False,  # Find word lemmata.
        light = False).encode('ascii', 'ignore').split('\n')
 
    print multiprocessing.current_process().name, 'to map',len(sentence_list),"sentences"
    crap_sentences = 0
    # Map the tokens
    for sentence in sentence_list:
	# Add list of words in sentence to counts for each word in sentence
	# after accumulating word and position counts
	for i, word in enumerate(sentence.split()):
	    # Don't position/neighbor map for unlikely candidates for a "good" sentence
	    # Just count the word occurrence
	    if i == crap_threshold:
		    crap_sentences += 1
	    if word.isalpha():
		local_map[word] = incrementToken(local_map[word],\
		    i, word)
		if not i > crap_threshold:
		    local_map[word] = mapNeighbors(local_map[word],\
			sentence.split(), word)
    out = []
    total_tokens = 0
    # spin accumulated token and data-carrying tuple to list of tuples for Reduce
    for key, value in local_map.items():
	if key is not '' and value is not None:
		total_tokens += value[0]
		out.append((key, value))
    # Let user know we're done
    print multiprocessing.current_process().name, 'mapped tokens:', \
	total_tokens,"sentences:",len(sentence_list)
    if crap_sentences > 0:
	crap = len(sentence_list)/crap_sentences
	print 'junk sentence %:', crap
    return out


def mergeDicts(*L):
    n = collections.defaultdict(int)
    # handle case of list of tuples
    if type(L) is tuple:
	for t in L:
	    for k, v in t.iteritems():
		n[k] += v
    else:
    # handle case of dict of keys/tuples
	for k, v in itertools.chain( item.iteritems() for item in L ):
	    n[k] += v    
    return n

def Partition(L):
    tf = collections.defaultdict(lambda:(0,{},{}))
    for sublist in L:
	if sublist is not None:
	    for p in sublist:
		n = tf.get(p[0])
		if not isinstance(n, tuple):
		    # tuple doesn't exist in tf: grab it from p
		    tf[p[0]] = (p[1][0], p[1][1], p[1][2])
		else:
		    positions = mergeDicts(n[1],p[1][1])
		    neighbors = mergeDicts(n[2],p[1][2])
		    tf[p[0]] = (p[1][0] + n[0], positions, neighbors)
		    
    return tf

def Reduce(Mapping):
    # Reduce position
    positions = trimdict(mergeDicts(Mapping[1][1]), args.trimpositions)
	    
    # Reduce neighbors

    neighbors = trimdict(mergeDicts(Mapping[1][2]), args.trimneighbors)
    # Return new tuples for each token, ('key', count, {positions})
    return (Mapping[0], Mapping[1][0], positions, neighbors)


def load(path):
    with open(path, 'rt') as f:
	mapped = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
    return mapped

def tuple_sort(a, b):
    if a[1] < b[1]:
	return 1
    elif a[1] > b[1]:
	return -1
    else:
	return cmp(a[0], b[0])


def dumpdictionary(known_words):
    os.chdir(output_dir)
    output = open('plaintext_wordlist_' + os.path.basename(infile)
		+ '.txt', 'wt')
    temp = ('', )
    for (idx, value) in enumerate(known_words):
	if idx % 2 == 0 and idx > 1:
	    output.write('%30s %d:%d %30s %d:%d \n' %\
			(temp[0],idx-1, temp[1],
			value[0],idx, value[1]))
	else:
	    temp = (value[0],value[1] )

    output.close()
    output = gzip.open(os.path.basename(infile)+dict_file, 'wb')
    pickle.dump(known_words, output)
    output.close()
    print 'stored dictionary from:', len(known_words), 'unique tokens'
    os.chdir(current_dir)

def humansize(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
	if num < 1024.0:
	    return '%3.1f %s' % (num, x)
	num /= 1024.0


def percentages(L):
    positions = L[2]
    percentages = []
    total = L[1]
    for k in positions.keys():
	p = 100 * float(positions[k]) / float(total)
	percentages.append((k, p))
	
    return percentages

def neighbors(L):
    neighbors = L[3]
    out = []
    for key, value in neighbors.iteritems():
	out.append((key ,value))
    return out

def dumpstats(term_frequencies):
    # Output human_readable term_frequencies
    os.chdir(output_dir)
    output = open('term_frequencies_' + os.path.basename(infile)\
		+ '.txt', 'wt')
    for pair in term_frequencies:
	output.write(' %s : %d ' % (pair[0], pair[1]))
	output.write('\n\n')
	for n in percentages(pair):
	    output.write(' %1d:  %3.2f%% ' % (n[0], n[1]))
	    
	output.write('\n\n')
	for n in neighbors(pair):
	    output.write(' %1s %2d' % (n[0], n[1]))
	output.write('\n\n')    
    os.chdir(current_dir)
    
'''
   Makes sure we're using an appropriately sized pool
'''
def getcpus(data_file):
    fslice = int(args.fslice)
    if fslice > 0:
	return fslice
    elif len(data_file)  < 5000:
	return 1
    else:
	return multiprocessing.cpu_count()
    
    
def process(infile):
    # prepare and load file
    print '\nStarted job', infile
    data_file = load(infile)
    # get either fslice or num_cpus
    global num_cpus
    num_cpus = getcpus(data_file)
    manager = Manager()
    manager.file = data_file

    # Partition job into appropriate slices, prepare argument tuple for Map
    print "Parsing job to sentences"
    offset = len(data_file) / num_cpus
    print 'partitioned',humansize(len(data_file)),'job to', num_cpus,\
	'slices of', humansize(offset)
    
    # Generate count tuples for matched words from dictionary
    print 'Preparing job', infile
    # Build a pool of num_cpus processes
    pool = Pool(processes=int(num_cpus), initializer=init, initargs=(data_file,))
    
    # Map the job
    single_count_tuples = pool.map(Map,((i, infile) for i in xrange(num_cpus)))
    data_file.close()
    
    # Organize the count tuples; lists of tuples by token key
    token_to_tuples = Partition(single_count_tuples)
    # Collapse the lists of tuples into total term frequencies

    print 'Reducing job', infile
    term_frequencies = pool.map(Reduce, token_to_tuples.items())

    # Sort the term frequencies in nonincreasing order

    term_frequencies.sort(tuple_sort)

    # dump dictionary from count, we want this sorted

    dumpdictionary(term_frequencies)

    # Output top term frequencies to console

    print 'top %d tokens by frequency' % int(args.top)
    for (index, pair) in enumerate(term_frequencies[:int(args.top)]):
	print '%1d %2s %3d' % (index + 1, pair[0] ,pair[1]) 
    
    # Dump human-readable statistics
    dumpstats(term_frequencies)


if __name__ == '__main__':
    files = args.files[0]
    if not os.path.isdir(output_dir):
	os.mkdir(output_dir)
    for infile in files:
	process(infile)


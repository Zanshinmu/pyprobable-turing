#!/usr/bin/python
# -*- coding: utf-8 -*-

import string
import pickle
import os
import argparse
import multiprocessing
from multiprocessing import Pool, Manager
import mmap
import contextlib
import sys
import re
import StringIO

data_file = file
dict_file = 'dictionary.pkl'
num_cpus = multiprocessing.cpu_count()
parser = argparse.ArgumentParser()
parser.add_argument('files', action='append', nargs='+',
                    help='Filenames to parse')
parser.add_argument('-top', '--top',
                    help='Number of sorted tokens to display as output'
                    , default=20, required=False)
parser.add_argument('-fslice', '--fslice',
                    help='Overrides cpu detection,sets number of partitions for job'
                    , default=num_cpus, required=False)
args = parser.parse_args()
num_cpus = int(args.fslice)
sentenceEnds = re.compile('([A-Z][^\.!?]*[\.!?])')
crap = re.compile('\-{2,}|\[|\(|\:|\/|[0-9]')

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def prepjob(L):
    #data_file = load(L[0])
    #fd = open(temp_file, 'wt', 65534)
    #fd.write(data_file[L[1]:L[2]])
    #fd.close()
    return StringIO.StringIO(data_file[L[1]:L[2]])
    #print multiprocessing.current_process().name, \
     #   'prepared mmaped file', temp_file


##def get_tempfile(L):
##    return L[0] + '_' + str(L[2]) + '.tmp'


def splitSentence(line):
    line = crap.sub(' ', line)
    sentenceList = sentenceEnds.split(line)
    return sentenceList


def sanitize(w):

    # Strip punctuation from the front

    while len(w) > 0 and not w[0].isalnum():
        w = w[1:]

    # Strip punctuation from the back

    while len(w) > 0 and not w[-1].isalnum():
        w = w[:-1]
        
    return w


def incrementTuple(i, T):
    position = {}
    try:

  # grab word total count

        count = T[0] + 1

  # grab the positional count dict

        position = T[1]
    except IndexError:
        count = 1

  # Now, let's set the position dict and index if they don't exist

    cur = position.get(i, 0)
    position[i] = cur + 1
    T = (count, position)
    return T


def init(data):
    global data_file
    data_file = data


def Map(L):
    sentence_max = 0
    #temp_file = get_tempfile(L)
    text = prepjob(L)
    #data_file = load(temp_file)
    local_words = {}
    print multiprocessing.current_process().name, 'to map region:', \
        L[1],"to",L[2]
    while True:
        raw_line = text.readline()
        if not raw_line:
            break
        pass
        for sentence in splitSentence(raw_line):
            for (i, word) in enumerate(sentence.split()):
                if i > sentence_max:
                    sentence_max = i
                if not word.isspace():
                    sanitized = sanitize(word).lower()
                    local_words[sanitized] = incrementTuple(i,
                            local_words.get(sanitized, (0, {})))

    out = []
    sum = 0
    for (key, value) in local_words.items():
        if key is not '' and value is not None:
            sum += value[0]
            out.append((key, value))

    print multiprocessing.current_process().name, 'mapped tokens:', \
        sum, 'sentence max:', sentence_max
    #data_file.close()
    #os.remove(temp_file)
    return out


def Partition(L):
    tf = {}
    for sublist in L:
        if sublist is not None:
            for p in sublist:
                n = tf.get(p[0])
                if not isinstance(n, tuple):

           # n doesn't exist in tf: add tuple from job sublist

                    tf[p[0]] = (p[1][0], p[1][1])
                else:

       # increment existing int/map for key p[0]
       # starting with the dict

                    positions = n[1]
                    for key in p[1][1]:
                        if not key in positions:
                            positions[key] = p[1][1][key]
                        else:
                            positions[key] += p[1][1][key]

           # Now, replace tf[p[0]] with new tuple values

                    tf[p[0]] = (p[1][0] + n[0], positions)

    return tf


def chunkjobs(infile, l, n):
    for i in xrange(0, len(l) - 2, n):
        yield (infile, i, i + n)


def Reduce(Mapping):

  # Reduce positions

    positions = {}
    for (index, value) in Mapping[1][1].items():
        if not index in positions:
            positions[index] = value
        else:
            positions[index] += value
  # Return new tuples for each token, ('key', count, {positions})
    return (Mapping[0], Mapping[1][0], positions)


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
    output = open('plaintext_wordlist_' + os.path.splitext(infile)[0]
                  + '.txt', 'wt')
    temp = ('', )
    for (idx, value) in enumerate(known_words):
        if idx % 2 == 0 and idx > 1:
            output.write('%35s %d:%d %35s %d:%d \n' % (temp[0],idx-1, temp[1],
                         value[0],idx, value[1]))
        else:
            temp = (value[0],value[1] )

    output.close()
    output = open(dict_file, 'wb')
    pickle.dump(known_words, output)
    output.close()
    print 'stored dictionary from:', len(known_words), 'unique tokens'


def humansize(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return '%3.1f %s' % (num, x)
        num /= 1024.0


def percentages(L):
    positions = L[2]
    percentages = []
    total = L[1]
    for n in positions:
        p = float(positions[n]) / total * 100
        if p > .5:
            percentages.append((n, p))

    return percentages


def process(infile):

  # prepare and load file

    print '\nStarted job', infile
    data_file = load(infile)
    manager = Manager()
    manager.file = data_file
    
  # Build a pool of num_cpus processes

    pool = Pool(processes=int(num_cpus), initializer=init,
                initargs=(data_file, ))

  # Partition job into appropriate slices, prepare argument tuple for Map

    offset = abs(len(data_file) / num_cpus)
    print 'partitioned job to', num_cpus, 'slices of', humansize(offset)
    partitioned_jobs = list(chunkjobs(infile, data_file, offset))
    data_file.close()
  # Generate count tuples for matched words from dictionary

    print 'Mapping job', infile
    single_count_tuples = pool.map(Map, partitioned_jobs)

  # Organize the count tuples; lists of tuples by token key

    token_to_tuples = Partition(single_count_tuples)

  # Collapse the lists of tuples into total term frequencies

    print 'Reducing job', infile
    term_frequencies = pool.map(Reduce, token_to_tuples.items())

  # Sort the term frequencies in nonincreasing order

    term_frequencies.sort(tuple_sort)

# dump dictionary from count, we want this sorted

    dumpdictionary(term_frequencies)

  # Output

    print 'top %d tokens by frequency' % int(args.top)
    for (index, pair) in enumerate(term_frequencies[:int(args.top)]):
        print index + 1, ':', pair[0], ':', pair[1]

    output = open('term_frequencies_' + os.path.splitext(infile)[0]
                  + '.txt', 'wt')
    for pair in term_frequencies:
        output.write(str(pair[0]) + ':  ' + str(pair[1]) + '\n')
        for n in percentages(pair):
            output.write(' %d: %3.2f%% ' % (n[0], n[1]))
        output.write('\n')

    output.close()


if __name__ == '__main__':
    files = args.files[0]
    for infile in files:
        process(infile)


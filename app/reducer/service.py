import re
from collections import defaultdict
from functools import partial

from .schemas import WordCount, ReduceRequest


def reduce(text: ReduceRequest):
    resp = map_reduce([text.text], word_count_mapper, word_count_reducer)
    return WordCount(wc=resp)


def tokenize(message):
    """return a set of words from a text"""

    message = message.lower()
    all_words = re.findall("[a-z0-9']+", message)
    return list(all_words)


def word_count_mapper(document):
    """return (word,1) for each word in the text"""

    for word in tokenize(document):
        yield word, 1


def word_count_reducer(word, counts):
    """return a sum up the counts for a word"""

    yield word, sum(counts)


def word_count(docs):
    """count the words in the input documents using MapReduce"""

    collector = defaultdict(list)

    for document in docs:
        for word, count in word_count_mapper(document):
            collector[word].append(count)

    return [output
            for word, counts in collector.items()
            for output in word_count_reducer(word, counts)]


def map_reduce(inputs, mr_mapper, mr_reducer):
    """return a list of MapReduce on the inputs using mapper and reducer"""

    collector = defaultdict(list)

    for i in inputs:
        for key, value in mr_mapper(i):
            collector[key].append(value)

    return [output
            for key, values in collector.items()
            for output in mr_reducer(key, values)]


def reduce_with(aggregation_fn, key, values):
    """return a key-values pair by applying aggregation_fn to the values"""
    yield key, aggregation_fn(values)


def values_reducer(aggregation_fn):
    """return function (values -> output) turned into a reducer"""
    return partial(reduce_with, aggregation_fn)

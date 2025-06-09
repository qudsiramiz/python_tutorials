# -*- coding: utf-8 -*-
"""
Created on Thu May 13 11:59:29 2021

@author: bgregor
"""

# Count letter frequency in 1M words


import itertools as its

# The multiprocessing library
import multiprocessing as mp
import pprint
import string
import tempfile

# A Counter is like a dictionary
# collections is a standard Python library
# https://docs.python.org/3.12/library/collections.html#collections.Counter
from collections import Counter
from zipfile import ZipFile

import requests
from get_n_cores import get_n_cores


def get_data():
    """Fetches a ZIP file, unpacks it to a file in the temp directory,
    returns the opened file. The data file is from SuperFastPython,
    an EXTENSIVE tutorial on using multiprocessing:
        https://superfastpython.com/category/multiprocessing/
    It contains 1M words.
    """
    url = "https://raw.githubusercontent.com/SuperFastPython/DataSets/main/bin/1m_words.txt.zip"
    with tempfile.NamedTemporaryFile() as zipf:
        req = requests.get(url)
        zipf.write(req.content)
        # Rewind the file
        zipf.seek(0)
        # Open the zip archive for reading
        myzip = ZipFile(zipf)
        # Read all lines from the file 1m_words.txt from the zip file
        # Treat it as UTF-8 Unicode text.
        all_lines = [word.decode("utf-8") for word in myzip.open("1m_words.txt").readlines()]
    # Strip to remove newline characters and lowercase the strings
    return [word.strip().lower() for word in all_lines]


def count_one_word(alphabet, word):
    # Filter the word so it only contains letters in
    # the alphabet.
    count = Counter()
    word = filter(lambda x: x in alphabet, word)
    # Update the counter with these letters.
    count.update(word)
    # Return the counter
    return count


def count_letters(words):
    """Return a dictionary of every lowercase letter
    in English found in a list of words"""
    # hint: here's a built-in Python string of the English alphabet
    alphabet = set(string.ascii_lowercase)
    # Initialize a Counter object for the whole alphabet
    letter_count = Counter()
    # and loop over all words
    for word in words:
        word_count = count_one_word(alphabet, word)
        letter_count.update(word_count)
    return letter_count


def par_count_letters(words, ncores):
    """A parallel version of count_letters"""
    with mp.Pool(processes=ncores) as pool:
        # Create a list of arguments for each word
        args = [(set(string.ascii_lowercase), word) for word in words]
        # Map the count_one_word function to the list of words
        n_in = pool.starmap(count_one_word, args)

        # Combine the results into a single Counter object
        letter_count = Counter()
        for count in n_in:
            letter_count.update(count)
    return letter_count


if __name__ == "__main__":
    # Fetch the data.
    words = get_data()
    print(f"number of words: {len(words)}")

    # Print out the dictionary of letter counts:
    letter_count = count_letters(words)
    pp = pprint.PrettyPrinter()
    # the Counter prints in order of counts.
    pp.pprint(letter_count)
    # Sort by the keys if you want the printout in
    # alphabetical order
    # pp.pprint({k: v for k, v in sorted(letter_count.items())})

    # Now do it again in parallel...
    ncores = get_n_cores()
    letter_count_par = par_count_letters(words, ncores)
    # and print it - the results should be the same!
    pp.pprint(letter_count_par)

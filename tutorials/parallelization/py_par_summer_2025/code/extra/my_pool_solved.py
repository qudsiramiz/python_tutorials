# -*- coding: utf-8 -*-
"""
Created on Thu May 13 11:59:29 2021

@author: bgregor
"""

# Count letter frequency in 1M words

 
# The multiprocessing library
import multiprocessing as mp
import string

from zipfile import ZipFile
import requests
import tempfile
import itertools as its
import pprint
import functools 



# A Counter is like a dictionary that automatically 
# stores key counts as the values. 
# collections is a standard Python library
# https://docs.python.org/3.12/library/collections.html#collections.Counter
from collections import Counter


from get_n_cores import get_n_cores


def get_data():
    ''' Fetches a ZIP file, unpacks it to a file in the temp directory,
        returns the opened file. The data file is from SuperFastPython,
        an EXTENSIVE tutorial on using multiprocessing:
            https://superfastpython.com/category/multiprocessing/
        It contains 1M words.
    '''
    url = 'https://raw.githubusercontent.com/SuperFastPython/DataSets/main/bin/1m_words.txt.zip'
    with tempfile.NamedTemporaryFile() as zipf:
        req = requests.get(url)
        zipf.write(req.content)
        # Rewind the file
        zipf.seek(0)
        # Open the zip archive for reading 
        myzip = ZipFile(zipf)
        # Read all lines from the file 1m_words.txt from the zip file 
        # Treat it as UTF-8 Unicode text. 
        all_lines = [word.decode('utf-8') for word in myzip.open('1m_words.txt').readlines()]
    # Strip to remove newline characters and lowercase the strings 
    return [word.strip().lower() for word in all_lines]
    
    
################ This is the original serial solution ################    
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
    ''' Return a dictionary of every lowercase letter
    in English found in a list of words'''
    # hint: here's a built-in Python string of the English alphabet
    alphabet = set(string.ascii_lowercase)
    # Initialize a Counter object
    letter_count = Counter()
    # and loop over all words 
    for word in words:
        word_count = count_one_word(alphabet, word)
        letter_count.update(word_count)
    return letter_count    
######################################################################


def par_count_one_word(word):
    ''' Modified count_one_word to only take 1 argument 
        This works, but there's a lot of repeated 
        calculations of the alphabet variable. Not
        a big deal, but it's inelegant.
    '''
    # Filter the word so it only contains letters in 
    # the alphabet.
    alphabet = set(string.ascii_lowercase)
    count = Counter()
    word = filter(lambda x: x in alphabet, word)
    # Update the counter with these letters.
    count.update(word)
    # Return the counter
    return count 

def par_count_letters(words, ncores):
    ''' A parallel version of count_letters '''
    alphabet = set(string.ascii_lowercase)
    # With just pool.map() there's an issue as the parallel function
    # can only take 1 argument. 
    # Solution 1: 
    #   compute the alphabet set in a variant function called 
    #       par_count_one_word()...see above.
    # Solution 2: 
    #   use pool.starmap() - not seen yet in the tutorial 
    # Solution 3:
    #   use the functools.partial function to partially
    #       call count_one_word()
    count_one_word_alpha = functools.partial(count_one_word,alphabet)
        
    with mp.Pool(processes=ncores) as pool:
        # Call pool.map to get a list of Counter objects
        # Solution 3:
        results = pool.map(count_one_word_alpha, words)
        # Solution 1 would look like this:
        # results = pool.map(par_count_one_word, words)
    # All the results are in memory, loop over and add them
    # to a Counter
    letter_count = Counter()
    for res in results:
        letter_count.update(res)
    return letter_count
    

if __name__ == '__main__':
    # Fetch the data.
    words = get_data()
    print(f'number of words: {len(words)}')

    # Print out the dictionary of letter counts:
    letter_count = count_letters(words)
    pp = pprint.PrettyPrinter()
    # the Counter prints in order of counts.
    pp.pprint(letter_count)    
    # Sort by the keys if you want the printout in 
    # alphabetical order
    #pp.pprint({k: v for k, v in sorted(letter_count.items())})   

    # Now do it again in parallel...
    ncores = get_n_cores()
    letter_count_par = par_count_letters(words, ncores)
    # and print it - the results should be the same!
    pp.pprint(letter_count_par)    

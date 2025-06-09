# -*- coding: utf-8 -*-
"""
Created on Thu May 13 11:59:29 2021

@author: bgregor
"""

# Count letter frequency in 1M words

 
# The Dask library
import dask
import dask.bag as db 

from collections import Counter

import string

# A function to download a file of 1M words
from zipfile import ZipFile
import pprint
import tempfile
import requests 
from get_n_cores import get_n_cores

from rcs_timer import timer

def get_data():
    ''' Fetches a ZIP file, unpacks it to a file in the temp directory,
        returns the opened file. The data file is from SuperFastPython,
        an extensive tutorial on using multiprocessing:
            https://superfastpython.com/category/multiprocessing/
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
    # Return as a Dask Bag. A better approach is to let the Dask Bag
    # read the file directly, but there's not too much data here. 
    return db.from_sequence([word.strip().lower() for word in all_lines])

def count_one_word(word):
    ''' Modified count_one_word to only take 1 argument 
        This works, but there's a lot of repeated 
        calculations of the alphabet variable. Not
        a big deal, but it's inelegant.
    '''
    # Filter the word so it only contains letters in 
    # the alphabet.
    alphabet = string.ascii_lowercase
    count = Counter()
    word = filter(lambda x: x in alphabet, word)
    # Update the counter with these letters.
    count.update(word)
    # Return the counter
    return count 


@timer
def dask_count_words():
    ''' This is in a separate function so the @timer
        can be applied '''
    # Fetch the data as a Dask Bag. 
    words = get_data()
    
    # Map count_letters() to each element of the Bag.
    words = words.map(count_one_word)
    
    # words is now a Bag of Counter objects. 
    # make a Counter for the final answer.
    # The Bag.fold() function applies an initial empty Counter() object,
    # and counter.update() is equivalent to counter1+counter2
    letter_count = words.fold(lambda x, y: x + y, initial=Counter())
    # Now actually run all of the Dask calculations. This will
    # execute in parallel.
    print("VISUA")
    letter_count.visualize(filename='count_words.svg', engine='cytoscape',optimize_graph=True)
    letter_count = letter_count.compute()
    return letter_count    

 
if __name__ == '__main__':
    ncores = get_n_cores()
    # Limit Dask to the desired number of cores. Use processes 
    # because the main parallel function (count_one_word) is a pure
    # Python function. 
    dask.config.set(scheduler='processes', num_workers=ncores )
    letter_count = dask_count_words()

    pp = pprint.PrettyPrinter()
    pp.pprint(letter_count)    


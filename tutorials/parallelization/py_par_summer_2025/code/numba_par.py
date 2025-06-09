# -*- coding: utf-8 -*-
"""


@author: bgregor
"""

import os

# NUMBA_NUM_THREADS sets the number of numba threads,
# but we want to make sure it's not set for this demo
# as it'll act as a maximum number of threads and
# on the SCC it defaults to a value of 1 when python3
# or miniconda modules are loaded. This must be done
# **BEFORE** importing the numba library.
if "NUMBA_NUM_THREADS" in os.environ:
    del os.environ["NUMBA_NUM_THREADS"]

import numba
import numpy as np
from get_n_cores import get_n_cores
from rcs_timer import timer

# Function "for_loop" will be turned into a parallel
# numba calculation.


# %%
@timer
@numba.njit(parallel=True, cache=True)
def for_loop(mat):
    """A double for loop over a 2D numpy ndarray"""
    rows, cols = mat.shape
    for i in range(rows):
        for j in range(cols):
            mat[i, j] = 2.0 * mat[i, j] - 1.0


@timer
def np_auto(mat):
    """Numpy element-by-element calculation"""
    # mat[:] means "do this in-place"
    mat[:] = 2.0 * mat - 1.0
    return mat


# %%
if __name__ == "__main__":

    SIZE = 10000
    rng = np.random.default_rng()
    mat = rng.uniform(-100, 100, [SIZE, SIZE])

    # Run the straightforward numpy
    # calculation
    mat2 = np_auto(mat)

    # Control numba threading
    # (or set NUMBA_NUM_THREADS before starting the program)
    n_cores = get_n_cores()
    numba.set_num_threads(n_cores)

    # Now do it element-by-element using double
    # for loops.
    mat2 = for_loop(mat)

    # What can we learn from what numba
    # did to the code?  Just type this into the console:
    # print_numba_info()

# %%

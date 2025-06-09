# -*- coding: utf-8 -*-
"""
BU RCS Parallel Python Tutorial

Calculate the value of pi 

@author: bgregor
"""

# Algorithm:  A unit circle has a radius of 1
#             and its area is A= pi R**2
#             Place that in a square of width & height of 2R
#             Its area is 4R**2
#             Divide the areas:   (pi R**2)/(4R**2) = pi/4
#             We can conclude:
#                   pi ~ 4 (# of pts in the circle / total number of points)
#               
#
#             Randomly choose a pair of numbers (x,y)
#             between -1 and 1 and increment the total point counter. 
#             Test to see if they're within the circle: 
#                  accept if x**2 + y**2 <= R
#                  and increment a counter for points in the circle.

import os 
if 'NUMBA_NUM_THREADS' in os.environ:
    del os.environ['NUMBA_NUM_THREADS']

# Start with plain ole Python and then convert to numpy

import numpy as np
import random
import numba

from rcs_timer import timer
from get_n_cores import get_n_cores

########################################
#%% Plain Numpy 
#%% Numpy version
@timer
def calc_pi_numpy(total):
    circle = 0
    # Vectorized numpy statements are much faster than using
    # Python for loops...but there's a lot of memory allocation
    # going on here.
    x = np.random.uniform(-1,1,total)
    y = np.random.uniform(-1,1,total)
    circle = np.sum(x**2 + y**2 <= 1)
    return 4.0 * (circle / total)
########################################

########################################


#%% Numba version.  Modify with nopython mode
# and parallel execution.

@numba.njit
def get_point_numba(cache=True,):
    # np.random.uniform is NOT directly supported in numba, it'll
    # call out to Python and Numpy if we use that.  Use np.random.random()
    # instead.  That produces numbers in the range 0->1 so subtract by 0.5 and
    # multiply by 2 to get to the right range
    # https://numba.pydata.org/numba-doc/dev/reference/numpysupported.html#random
    x = (np.random.random() - 0.5) * 2.0
    y = (np.random.random() - 0.5) * 2.0
    return x,y

# The fastmath=True option to numba.njit can sometimes produce faster
# code at a SLIGHT loss of numeric precision.
@timer
@numba.njit(cache=True, parallel=True)
def calc_pi_numba(total):
    circle = 0.0
    for i in numba.prange(total):
        # Just to show off calling another numba'd function.
        x,y = get_point_numba()  
        if x**2 + y**2 <= 1:
            circle +=1 
    return 4.0 * (circle / total)

#%%
if __name__=='__main__':
    # Plain numpy implementation 
    num_iters = int(5e6)
    print(calc_pi_numpy(num_iters))
    
    n_cores = get_n_cores()
    # First we'll run numba for 1 thread
    numba.set_num_threads(1)
    print(calc_pi_numba(num_iters))
    # and once more to show the numba performance after
    # compilation
    numba.set_num_threads(1)
    print(calc_pi_numba(num_iters))
    # Now turn on multiple cores
    print('Numba with %s threads' % n_cores)
    numba.set_num_threads(n_cores)
    print(calc_pi_numba(num_iters))


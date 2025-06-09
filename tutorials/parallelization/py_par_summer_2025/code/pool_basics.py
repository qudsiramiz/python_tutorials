#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BU RCS Parallel Python Tutorial

An example of using the multiprocessing Pool to compute the
value of pi.


@author: bgregor
"""

# The multiprocessing library
import multiprocessing as mp

# numpy for its random number generator
import numpy
import psutil

# The function to get the number of available cores
from get_n_cores import get_n_cores

# Import a function decorator to time function execution from timer.py
from rcs_timer import timer


def sample_points(n):
    """Picture a circle of radius 1 inside of a square with sides of length 1.
    Pick 2 random numbers between 0 and 1. In the upper right quadrant, test
    to see if those fall inside the circle.  Repeat, and make a count of the
    numbers that are inside the circle."""
    # Print my processor number. This will only work if this runs in a
    # process launched by multiprocessing, so use an "if" to avoid
    # an error in the serial case. This is just for demonstration purposes,
    # there's no real need to
    if len(mp.current_process()._identity) > 0:
        print(f"Running on processor {mp.current_process()._identity[0] - 1}")
    n_in = 0
    # Make a rng generator
    rng = numpy.random.default_rng()
    for i in range(n):
        x = rng.uniform()  # default range is 0-1
        y = rng.uniform()
        if x**2 + y**2 < 1.0:
            n_in += 1
    return n_in


# Serial version:
@timer
def serial_calc_pi(N):
    """Calculate the value of pi"""
    n_in = sample_points(N)
    pi = 4.0 * (n_in / N)
    return pi


# Now let's trying computing this in parallel with a
# multiprocessing.Pool
@timer
def par_calc_pi(N, nprocs=2):
    num_per_proc = N // nprocs  # integer division
    # The pool needs an iterable to do parallel calls.

    # Initialize a list of [num_per_proc, num_per_proc,...] etc
    nums = nprocs * [num_per_proc]
    # nums will get split up across the worker processes.
    # Each gets assigned a number N points to generate.
    total_N = sum(nums)  # account for rounding
    with mp.Pool(processes=nprocs) as pool:
        # n_in is the list of in-circle counts per process
        n_in = pool.map(sample_points, nums, 1)
    pi = 4.0 * (sum(n_in) / total_N)
    return pi


# Note - on Windows multiprocessing MUST use this
# convention
if __name__ == "__main__":
    max_N = 1_000_000
    # Run this in serial, line-by-line.
    pi = serial_calc_pi(max_N)
    print(f"serial calc pi={pi:1.8f}\n")

    # And in parallel.  The number of processes can
    # be specified.
    n_cores = get_n_cores()
    pi_par = par_calc_pi(max_N, nprocs=n_cores)
    print(f"parallel calc pi={pi_par:1.8f}")

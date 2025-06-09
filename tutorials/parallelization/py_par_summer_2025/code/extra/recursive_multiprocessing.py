# -*- coding: utf-8 -*-
"""
Created on Thu May 13 11:59:29 2021

@author: bgregor
"""

# The multiprocessing library
import multiprocessing as mp

import numpy as np

from get_n_cores import get_n_cores


# Sum all even numbers in a range of integers.

# #### The straightforward solution sums subsets of the numbers
#  in parallel, then summed the partial sums in serial.  What
#  if we wanted to sum up parts of the partial sums in parallel?
#  I.e. with 4 cores the 1000 rows are summed to 250 partial sums.
#  Let's then sum those in parallel down to ~62 partial sums, then
# those in parallel down to ~16 partial sums, etc...  This calls
# for recursion.
# The question is - can we recursively launch multiple Pools?
# Answer: Yes we can!


def sum_nums(nums):
    """
    nums: a numpy array
    """
    # Numpy-centric way: use np.where to find
    # all of the even numbers, then use that
    # index with np.sum
    if np.isscalar(nums):
        return nums
    even_indx = np.where(nums % 2 == 0)
    return np.sum(nums[even_indx])


def par_sum(nums, ncores):
    """Recursively sum where each recursive call opens its own
    parallel pool of workers.
    nums: a numpy array.
    ncores: size of the parallel pool
    """
    # When there's just a few values just return
    # their sum.
    if np.isscalar(nums) or len(nums) < ncores:
        return np.sum(nums)
    with mp.Pool(ncores) as pool:
        # Split the array of partial sums into smaller pieces
        nrows = len(nums)
        split_nums = np.array_split(nums, int(np.sqrt(nrows)))
        partial_sums = pool.map(sum_nums, split_nums)
        return par_sum(partial_sums, ncores)


if __name__ == "__main__":
    # the range to sum over.
    START_NUM = 0
    STOP_NUM = 100000000

    # Stick with 1-D.
    nums = np.arange(START_NUM, STOP_NUM, dtype=np.int64)

    final_sum = par_sum(nums, get_n_cores())

    print(f"The sum of the even numbers between {START_NUM} and {STOP_NUM} is: {final_sum}")

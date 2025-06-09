#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BU RCS Parallel Python Tutorial

A function decorator that prints the execution time
of a function.

@author: bgregor
"""

import time
import functools


# A decorator that does simple function timing.

def timer(func):
    ''' Takes a function as the argument.
        functools.wraps is a tool that helps properly
        handle doc strings and function names.'''
    @functools.wraps(func)
    def timing_wrapper(*args, **kwargs):
        # *args and **kwargs allow for variable numbers of
        # arguments
        start_t = time.perf_counter()
        # Call the function
        result = func(*args, **kwargs)
        end_t = time.perf_counter()
        # Compute & print the elapsed time. Return the wrapped
        # function's result.
        print(f'{func.__name__}: {end_t - start_t:.3f} sec')
        return result
    return timing_wrapper

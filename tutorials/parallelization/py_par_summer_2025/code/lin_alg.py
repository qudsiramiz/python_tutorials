# -*- coding: utf-8 -*-
"""
BU RCS Parallel Python Tutorial

A little benchmarking program showing the effect of multiple cores
on a matrix-matrix multiply.

@author: bgregor
"""


import sys
import time

import numpy as np

# The threadpoolctl library can be used for manipulating the
# number of threads for this demo.  This library is installed
# by default by Anaconda.
import threadpoolctl
import tqdm
from get_n_cores import get_n_cores
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator

if __name__ == "__main__":
    logical_cores = get_n_cores(use_physical_cores=False)
    real_cores = get_n_cores(use_physical_cores=True)

    print(
        "According to psutil this computer has %s logical cores and %s real cores."
        % (logical_cores, real_cores)
    )
    sys.stdout.flush()  # Force that to print.

    # A value of 3000 will use ~800 MB of RAM being used.
    # A value of 1000 will use ~150 MB of RAM.
    SIZE = 3000
    # make a pair of random square matrices.
    x = np.random.random([SIZE, SIZE])
    y = np.random.random([SIZE, SIZE])

    # And do a matrix-matrix multiplication to "warm up" the CPU and fill the
    # memory caches with these arrays.
    z = x @ y

    # Let's loop over the number of logical cores and set the number of
    # threads using the threadpoolctl lib.
    timer = {}
    # Run for a few iterations to get some averaging
    ITERS = 10
    for j in tqdm.tqdm(range(ITERS)):
        for i in range(1, logical_cores + 1):
            # Take care of storage:
            if i not in timer:
                timer[i] = np.zeros(ITERS)
            # And now time it:
            # Normally you'd set OMP_NUM_THREADS (or similar) variable to control
            # the number of threads before starting Python.
            # Here set it for the BLAS library with threadpoolctl.
            with threadpoolctl.threadpool_limits(limits=i, user_api="blas"):
                start = time.perf_counter()
                z[:] = x @ y  # matrix-matrix multiply.
                end = time.perf_counter()
                timer[i][j] = end - start  # store elapsed time

    # Compute median times.
    for key in timer:
        timer[key] = np.median(timer[key])

    # Get the time for 1 core
    t_one = timer[1]
    cores = sorted(list(timer.keys()))
    # Speedup ratio
    S = [t_one / timer[c] for c in cores]

    # The ideal speedup ratio is equal to the number of threads used
    S_ideal = cores

    ax = plt.figure().gca()
    ax.plot(cores, S_ideal, "b--")
    ax.plot(cores, S, "r-x")
    # Force integer horizontal axis
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    # set axis limits
    # ax.set_ylim([min(S) * 0.8, max(S) * 1.2])
    plt.title("Speedup Ratio.  Matrix size %s" % SIZE)
    plt.xlabel("N Cores")
    plt.ylabel("S")
    plt.show()

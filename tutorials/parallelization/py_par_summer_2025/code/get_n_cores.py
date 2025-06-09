# -*- coding: utf-8 -*-
"""
BU RCS Parallel Python Tutorial

A function that can be used to determine the maximum number of
cores available to Python on the BU SCC.  Can be adapted to and
used on other systems as well.

@author: bgregor
"""
import os
import platform
import subprocess

import psutil

# NOTE: This will not report the number of performance or efficiency
# cores on the latest x86_64 or ARM CPUs.


def get_n_cores(use_physical_cores=True, cores_var="NSLOTS"):
    """Get the number of cores that should be used.  This
    checks for an environment variable, NSLOTS, that is set
    using the SGE cluster queue software as used at BU's
    Shared Computing Cluster.  This should be modified to
    fit your own needs, or you can set the NSLOTS variable
    manually before calling this code.

    Optionally, set the use_physical_cores flag and this
    will just use all available physical cores.
    """
    # This checks for the existence of cores_var first,
    # and if it's found just returns that as the number
    # of cores to use. Convenient on the SCC.
    if cores_var in os.environ:
        return int(os.environ[cores_var])

    # Otherwise get the number of cores from psutil
    return psutil.cpu_count(logical=not use_physical_cores)


if __name__ == "__main__":
    # Return default on this platform
    print(f"Default num cores on this computer: {get_n_cores()}")
    # with physical cores
    print(f'Physical cores on this computer: {get_n_cores(use_physical_cores=True, cores_var="")}')
    # Logical cores
    print(f'Logical cores on this computer: {get_n_cores(use_physical_cores=False, cores_var="")}')
    # Set an environment variable to 3 and retrieve it
    os.environ["TEST_CORES"] = "3"
    print(
        f'Testing retrieving cores from an environment variable TEST_CORES. Should be 3: {get_n_cores(cores_var="TEST_CORES")}'
    )

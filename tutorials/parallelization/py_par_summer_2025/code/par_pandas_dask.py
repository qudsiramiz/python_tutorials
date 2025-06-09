#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BU RCS Parallel Python Tutorial

Pandas and Dask


@author: bgregor
"""

# An example of using Dask to auto-parallelize the processing
# of CSV data.



 

import pandas as pd
from matplotlib import pyplot as plt
# seaborn makes matplotlib look better
import seaborn as sns
sns.set_theme() 

from rcs_timer import timer
from get_n_cores import get_n_cores

import tempfile
import urllib.request
import os
import time

# Dask datatypes
import dask.dataframe as dd
import dask.array as da

# various ways to control Dask
import dask
import dask.multiprocessing 
from dask.distributed import Client,LocalCluster

#%%
# =============================================================================
#  Here I define functions 
# =============================================================================

def get_bike_data():  # xyz
    # Fetch the dataset.
    temp_dir = tempfile.gettempdir()
    csv_url = 'https://s3.amazonaws.com/hubway-data/hubway_Trips_2011.csv'
    csv_file = os.path.join(temp_dir,'bike.csv')
    if os.path.exists(csv_file):
        return csv_file
    # Not downloaded yet, fetch it.
    print('Downloading URL.')
    urllib.request.urlretrieve(csv_url, csv_file)
    print('Done.')
    return csv_file
#%%
def get_bike_data_big():
    # Get a bigger dataset. 398MB instead of 11.
    temp_dir = tempfile.gettempdir()
    csv_urls = ['https://s3.amazonaws.com/hubway-data/hubway_Trips_2011.csv',
                'https://s3.amazonaws.com/hubway-data/hubway_Trips_2012.csv',
                'https://s3.amazonaws.com/hubway-data/hubway_Trips_2013.csv',
                'https://s3.amazonaws.com/hubway-data/hubway_Trips_2014_1.csv',
                'https://s3.amazonaws.com/hubway-data/hubway_Trips_2014_2.csv']
    csv_files = [os.path.join(temp_dir,f'bike_{i}.csv') for i in range(len(csv_urls))]
    wildcard_csv_files = os.path.join(temp_dir,'bike_*.csv')
    if all([os.path.exists(csv) for csv in csv_files]):
        return wildcard_csv_files
    # Not downloaded yet, fetch it.
    print('Downloading URLs.')
    for i,csv_url in enumerate(csv_urls):
        urllib.request.urlretrieve(csv_url, csv_files[i])
    print('Done.')   
    return wildcard_csv_files

@timer
def proc_data(csv_file):
    df = dd.read_csv(csv_file, dtype={'Zip code': 'object'})
    # Make a filtered dataframe for members only
    df = df[df['Member type']=='Member']    
    
    # What was the longest ride?
    max_ride = df['Duration'].max()
    print(max_ride)
    # ? We have to ask for it to be computed...
    max_ride_val = max_ride.compute()
    print(f'Longest ride: {max_ride_val}')
    # When did it happen?
    res = df[df['Duration']==max_ride].compute()
    print(f'The longest ride row: \n{res}')
    
    # Other processing...
    # Get the unique station names - convert to a Pandas dataframe
    # for the assigning of unique numbers.
    station_names = dd.concat([df['Start station number'],df['End station number']]).unique().compute()
    station_names = pd.DataFrame(station_names,columns=['name']).reset_index(names='station_id')
    # Now use a merge to add new columns to the Dask dataframe.
    station_names = dd.from_pandas(station_names, npartitions=1)
    
    # And assign new columns that map the station number to an id number.
    df['start_id'] = df['Start station number'].replace(station_names.set_index('name')['station_id'])
    df['end_id'] = df['End station number'].replace(station_names.set_index('name')['station_id'])
    
    # Compute a histogram for the start and end id's to see which stations
    # are the most popular
    # Dask can compute a histogram using its Array datatype, so convert
    # those two columns.
    # oops, we need the number of unique station names to get the # of bins...
    # convert station_names back to a pandas DF and get the row count.
    nbins = station_names.compute().shape[0]
    start_hist = da.histogram(df['start_id'].to_dask_array(), bins=nbins, range=[0,nbins])
    end_hist = da.histogram(df['end_id'].to_dask_array(), bins=nbins, range=[0,nbins])
    # start_hist is a tuple: (dask.array, bins numpy array).
    # Force them to compute so they can be plotted.
    start_hist =  (start_hist[0].compute(), start_hist[1][0:-1])
    end_hist =  (end_hist[0].compute(), end_hist[1][0:-1])

    return start_hist, end_hist


def plot_histos(start_hist, end_hist):
    # A bar plot can be done with Seaborn but the data has to
    # be packed into a Pandas dataframe with the right format.
    nstations = start_hist[0].shape[0]
    s_df = pd.DataFrame({'id':start_hist[1], 'count':start_hist[0],'type':['start']*nstations})
    e_df = pd.DataFrame({'id':end_hist[1], 'count':end_hist[0],'type':['end']*nstations})
    df= pd.concat([s_df,e_df])
    g = sns.catplot(data=df, kind="bar", x="id", y="count", hue="type")
    plt.title("start/end station trip counts")
    g.set_xticklabels([""] * nstations)
    
if __name__=='__main__':
    # These MUST execute in a __main__ section.  Try varying threads_per_worker
    # and n_workers to see tradeoffs between using threaded, multi-process, and
    # mixed computations.
    cluster = LocalCluster(threads_per_worker=2, 
                           n_workers=2)
    client = Client(cluster)
    print(f'Open in your web browser: {client.dashboard_link}')
    # Get that link opened...the use of the dashboard is not at all
    # required for a "real" program
    input('Press Enter when you have the link opened to the dashboard')
    
    
    # These provide other ways to control Dask without the dask.distributed
    # library
    # Multiprocess parallelism
    #dask.config.set(scheduler='processes', num_workers=get_n_cores()) 
    # Multithreaded parallelism
    #dask.config.set(scheduler='threads', num_workers=1) 
    #dask.config.set(scheduler='threads', num_workers=get_n_cores()) 

    # Get a dataframe of just customers
    csv_file = get_bike_data()
    
    # Do some stuff with it.
    start_hist, end_hist = proc_data(csv_file)
    plot_histos(start_hist, end_hist)
     
    input('PAUSED')
    # Now get a bigger data set
    csv_file_many = get_bike_data_big()
    # Do some stuff with it.
    print('\n\n**** Processing many CSV ****\n\n')
    start_hist, end_hist = proc_data(csv_file_many)
    plot_histos(start_hist, end_hist)

    # Tell Dask it can close up shop
    client.shutdown()
    client.close()
    cluster.close()
    del client
    del cluster
    
    
 
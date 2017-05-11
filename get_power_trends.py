# AUTHOR: Ian Wright
# LAST EDITED: May 10, 2017

import geopandas as gpd
import shapely
from shapely.geometry import Point

import pandas as pd
import numpy as np
import csv
import pickle
from datetime import datetime, date, time

from pyspark import SparkContext
sc = SparkContext()

# set mode as 'TEST' or 'FULL'
MODE = 'TEST'

FULL_PATH = '/user/iw453/demand/anoms*.gz'
TEST_PATH = '/user/iw453/demand/anomstest.csv'
DICT_PATH = './data/anoms_dict.pickle'


def get_point(lng, lat):
    return Point(float(lng), float(lat))


def parse_events(events):
    """
    checks is a GDELT event is associated with a regional demand anomaly
    - first finds the EIA region for which the GDELT event date corresponded to a demand anom.
    - once the region is found, a spatial operation is performed to check if the GDELT
        event occurred within the given region's polygon
    - anomaly-associated events are passed through (filter functionality)
    """
    reader = csv.reader(events)
    # for each event record
    for event in reader:
        # continue to look for regional matches until one is found and yielded
        for region in anoms_dict.keys():
            # check if datestamp within the regional set of interesting dates
            try:
                if int(event[0]) in anoms_dict[region][0]:
                    # check if event location took place within region
                    point = get_point(event[8], event[7])
                    if point.within(anoms_dict[region][1]):
                        yield (region,          # region
                               (int(event[0]),   # date
                               str(event[1]),   # actorname
                               str(event[2]),   # actortype
                               str(event[3]),   # eventcode
                               int(event[4]),   # quadclass
                               float(event[5]))) #tone  
            except ValueError:
                pass


# filter: skip records with missing values
def skip_missing(line, col_index):
    return line[1][col_index] != ''

# map: combines original key with a field from V into a new concatenated key, in preparation for count
def concat_for_count(line, col_index):
    return (line[0] + '_' + str(line[1][col_index]), 1)

# map: re-divides the concatentated key
def resplit_regions(line):
    return (line[0].split('_')[0], (line[0].split('_')[1], line[1]))

# finds the most common 'n' values from a particular feature 
def get_top_n(v, n):
    return sorted(list(v), key=lambda x: x[1], reverse=True)[:n]


def get_top_values(col_index, n):
    """
    ARGS
    - col_index:
        1=actorname
        2=actortype
        3=eventcode
        4=quadclass
    - top n values
    """
    return reduced_anoms.filter(lambda event: skip_missing(event, col_index)) \
        .map(lambda event: concat_for_count(event, col_index)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(resplit_regions) \
        .groupByKey() \
        .mapValues(lambda counts: get_top_n(counts, n)) \
        .collect()


if __name__ == "__main__":

	# where will it sit?
	with open(DICT_PATH, 'rb') as handle:
	    anoms_dict = pickle.load(handle)

	# create single RDD from multiple zipped data files; correct string encoding, strip header row
    if MODE == 'TEST':
        CHOSEN_PATH = TEST_PATH
    else:
        CHOSEN_PATH = FULL_PATH

	anoms = sc.textFile(CHOSEN_PATH)
	anoms = anoms.map(lambda line: line.encode('utf-8'))
	anoms = anoms.filter(lambda line: not line.startswith('SQL'))

	# map by partition for initial data volume reduction (heaviest lifting is here)
	reduced_anoms = anoms.mapPartitions(parse_events)


	# top 10 actor names, by region
    actor_names = get_top_values(1, 10)
	print 'top 10 actor names, by region:\n', actor_names

	# top 10 actor types, by region
    actor_types = get_top_values(2, 10)
	print 'top 10 actor types, by region:\n', actor_types

	# top 10 event codes, by region
    event_codes = get_top_values(3, 10)
	print 'top 10 event codes, by region:\n', event_codes

	# see counts of the four quad classes, by region
    quad_classes = get_top_values(4, 4)
	print 'counts of the four quad classes, by region:\n', quad_classes

	# get average TONE, by region
    avg_tone = reduced_anoms.filter(lambda event: skip_missing(event, 5)) \
            .map(lambda event: (event[0], event[1][5])) \
            .mapValues(lambda v: (v, 1)) \
            .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
            .mapValues(lambda v:v[0] / v[1]) \
            .collect()
	print 'average tone, by region:\n', avg_tone


    # write results to a text file for later reference
    with open('anom_results.txt', 'wb') as f:

        f.write('top 10 actor names, by region:\n')
        for item in actor_names:
            f.write(str(item) + '\n')
        f.write('\n\n')

        f.write('top 10 actor types, by region:\n')
        for item in actor_types:
            f.write(str(item) + '\n')
        f.write('\n\n')
	
        f.write('top 10 event codes, by region:\n')
        for item in event_codes:
            f.write(str(item) + '\n')
        f.write('\n\n')

        f.write('counts of quad classes, by region:\n')
        for item in quad_classes:
            f.write(str(item) + '\n')
        f.write('\n\n')

        f.write('average tone, by region:\n')
        for item in avg_tone:
            f.write(str(item) + '\n')
        f.write('\n\n')


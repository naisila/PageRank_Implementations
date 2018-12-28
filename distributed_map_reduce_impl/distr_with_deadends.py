from __future__ import print_function

# DISTRIBUTED VERSION OF PAGERANK ALGORITHM USING MAP-REDUCE

# ORIGINAL NAIVE CODE IN https://github.com/apache/spark/blob/master/examples/src/main/python/pagerank.py
# MODIFIED BY NAISILA PUKA: Normalizes ranks, takes care of nodes with no in-links, dead-ends and PageRank 
# leakage by saving the whole sum in an accumulator.

# Invoke by spark-submit distr_with_deadends.py <file> <nodes> <iterno> 

import re
import sys
import time
from operator import add

from pyspark.sql import SparkSession

def computeContribs(mainUrl, urls, rank):
    # Calculates URL contributions to the rank of other URLs.
    # Yields zero for the contribution to the current URL to take care of URLs with no in-links.
    yield (mainUrl, 0)
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    # Parses a urls pair string into urls pair.
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def initialize(mainUrl, urls, size):
    # Initializes all the ranks to 1 / (total number of nodes)
    yield (mainUrl, 1 / float(size))
    for url in urls:
        yield (url, 1 / float(size))

# Start spark setup time 
start = time.time()

if len(sys.argv) != 4:
    print("Usage: pagerank <file> <nodesNo> <iterations>", file=sys.stderr)
    sys.exit(-1)

print("Starting PageRank. The results are correct for graphs with dead-ends and nodes with no in-links.",
        file=sys.stderr)

# Initialize the spark context.
spark = SparkSession\
    .builder\
    .appName("FireCoders_Python_PageRank_NoDeadEnds")\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("Error")

# Define an accumulator for PageRank leakage
accum = sc.accumulator(0.0)

def leakage(rank):
    # Sum all ranks to calculate PageRank leakage by 1 - accum.value
    global accum
    accum += rank[1]

# End spark setup time
end = time.time()
print("Spark setup time is: " + str( end - start) +" s.\n")

# Loads in input file. It should be in format of:
#     URL         neighbor URL
#     URL         neighbor URL
#     URL         neighbor URL
#     ...

# Start lines preprocessing time 
linesStart = time.time()

lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    
# Loads all URLs from input file and initialize their neighbors.
# Map function to parse neighbour
links = lines.map(lambda urls: parseNeighbors(urls)).distinct()

# Group by and reduce function(which does nothing), just yields the list of outlinks
links = links.groupByKey().cache()

# Gets number of nodes
nodes = int(sys.argv[2])

# Loads all URLs initializes ranks of them to 1 / nodes.
# Map function for initializing the rank vector, for each line of the link yields (url, 1/n) pair
init = links.flatMap(lambda url_urls: initialize(url_urls[0], url_urls[1], nodes))

# Group by and reduce function which chooses one of the values for the url (they are all 1/n)
ranks = init.groupByKey().cache().mapValues(list).map(lambda url_rank: (url_rank[0], url_rank[1][0]))
    
# A new variable to keep track of new and old ranks
newRanks = ranks

# Gets total number of iterations
iterNo = int(sys.argv[3])

# Initialize iteration number
curr = 1

# Lines preprocessing ends
linesEnd = time.time()

print ("Lines preprocessing time is: " + str(linesEnd - linesStart) + " s.\n")

# Start PageRank time
loopStartTime = time.time()

# Calculates and updates URL ranks continuously until all iterations specified in sys.args are done 
while curr <= iterNo:
    ranks = newRanks

    # Calculates URL contributions to the rank of other URLs.
    # Map function
    contribs = links.join(ranks).flatMap( lambda url_urls_rank: computeContribs(url_urls_rank[0], url_urls_rank[1][0], url_urls_rank[1][1]))

    # Reduce function on the part reduceByKey(add) to get new ranks
    # Then multiply by damping factor and then add teleportation
    newRanks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85)

    # Use accumulator for PageRank leakage
    newRanks.foreach(leakage)

    S = accum.value

    print("S is: " + str(S) + "\n")

    # Set rank to rank * damping_factor + (1 - S)/nodes
    newRanks = newRanks.mapValues(lambda rank: rank + (1 - S) / nodes)

    print("End of iteration #" + str(curr))
    print("\n")
    curr = curr + 1
    accum.add(-accum.value)
        
# End of PageRank time
loopEndTime = time.time()
print("PageRank Loop time is: " + str( loopEndTime - loopStartTime) + " s.\n")

# Print PageRank values
for (link, rank) in ranks.collect():
    print("%s has rank: %s." % (link, rank))

spark.stop()

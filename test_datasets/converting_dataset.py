from pyspark.sql import SparkSession
# By Endi Merkuri
# This code converts a given graph to one without dead ends
import random
def split(line):
	words = line.split("\t")
	return (words[0], words[1]), (words[1], words[0])

def node(line, i):
	words = line.split("\t")
	return words[i]

def gen(word):
	return (word, first[random.randint(0,len(first)-1)])

def concat(list):
	return list[0]+"\t"+list[1]+"\n"

name = "big"
spark = SparkSession\
        .builder\
        .appName("FireCoders_Python_PageRank")\
        .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("Error")
file = sc.textFile(name)
#Get the list of the nodes in the first column in the file
first = file.map(lambda line: node(line,0)).collect()
#Get the list of the nodes in the second column in the file
second = file.map(lambda line: node(line,1)).collect()
#Find the elements that are in the second column but not in the first column-dead ends
dead = list(set(second) - set(first))
#Generate random edges from the list of dead-ends
new = map(lambda a: gen(a), dead)
final = map(concat, new)
#Write the newly generated edges to the end of the file
f = open(name, "a+")
for i in final:
	f.write(i)
f.close()

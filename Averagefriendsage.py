from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)
#defining function parseline for splitting the data and storing data attributes like age and numfriends for further
#operation
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("C:/SparkCourse/fakefriends.csv")

#Defining all Rdd in queue for showing up the different stage of transformation.
#Below all transformations are having defined operation to be done once spark RDD action will be triggered
rdd1 = lines.map(parseLine)
rdd2 = rdd1.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd3 = rdd2.mapValues(lambda x: x[0] / x[1])
#Finally we are having spark action in place where we will use the last rdd created "rdd3" for collecting the result
#Here result is not rdd but its an object because we wre having action associated with it.
results = rdd3.collect()

# Here basic python concept getting applied. We are sorting the result and printing it. Here no spark theory applied
sortedResults = collections.OrderedDict(sorted(results))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

#below command can be used for printing simple result.
#for result in results:
    #print(result)

import matplotlib.pyplot as plt
age = sortedResults.keys()
friend = sortedResults.values()
plt.bar(age, friend,width=.8, color='r',align='center', alpha=0.5)
plt.xlabel('Age of the person')
plt.ylabel('Number of friend')
plt.title('Average friend numbers basing age')
plt.show()
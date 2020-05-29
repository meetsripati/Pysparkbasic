from pyspark import SparkConf, SparkContext
import collections
import matplotlib.pyplot as plt

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

#now plotting the sorted key values from above results.
x= sortedResults.keys()
y= sortedResults.values()
plt.bar(x,y,width=.5, color='r')
plt.show()


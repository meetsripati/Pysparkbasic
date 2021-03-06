from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("customerspent")
sc = SparkContext(conf = conf)

def parseline(line):
    fields = line.split(',')
    customerid = fields[0]
    spentmoney = fields[2]
    return (int(customerid),float(spentmoney))

lines = sc.textFile("c://sparkcourse/customer-orders.csv")
parsedline=lines.map(parseline)
spent = parsedline.reduceByKey(lambda x, y: (x + y))
finalspent = spent.collect()
for result in finalspent:
    print(result)
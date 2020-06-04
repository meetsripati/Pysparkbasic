#Here setting up an example for using broadcast variable in spark, it helps in gaining data from different file.


from pyspark import SparkConf, SparkContext


def loadMovieNames():
    movieNames = {}
    with open("c://SparkCourse/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
conf = SparkConf().setMaster('local').setAppName('broadcast testing')
sc = SparkContext(conf=conf)

broadcastVar = sc.broadcast(loadMovieNames())

print(broadcastVar.value)

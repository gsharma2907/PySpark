from pyspark import SparkContext
sc = SparkContext("local", "Collecting app")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "sparkhadoop", 
   "pyspark",
   "pysparkspark"]
)
coll = words.collect()
print "Elements in RDD -> %s" % (coll)

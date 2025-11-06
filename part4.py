from pyspark import SparkContext

sc = SparkContext('local', 'DataSkewDemo')

# create data set
skewed = sc.parallelize(['the'] * 1000000 + ['spark'] * 100 + ['hadoop'] * 100)

# map in to pairs
pairs = skewed.map(lambda w: (w, 1))

# count the words
result = pairs.reduceByKey(lambda a, b: a + b)

# collect the result
print(result.collect())

# Q: Observe the performance difference and discuss what causes it.
# A: The partition with the "the" will take longer to process because its substantially larger than the other ones.
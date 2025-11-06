from pyspark import SparkContext # donno if i can upload venv

sc = SparkContext('local', 'WordCountLab')

data = sc.textFile("sample_reviews_text.txt")

words = data.flatMap(lambda line: line.split())
pairs = words.map(lambda w: (w.lower(), 1))
result = pairs.reduceByKey(lambda a, b: a + b)

result.take(10)


def first_letter_partitioner(key):
    return ord(key[0].lower()) % 3  

partitioned = pairs.partitionBy(3, first_letter_partitioner) # pairs needs to be created in part 1.
result = partitioned.reduceByKey(lambda a,b: a+b)

partitioned = pairs.partitionBy(3, first_letter_partitioner)
partitioned_result = partitioned.reduceByKey(lambda a, b: a + b)

print(partitioned_result.take(10))

#Custom partitioning changes workload balance by letting us control how keys are assigned to partitions instead of relying on Spark’s default distribution.
#  When we choose a partitioning rule such as grouping words by their first letter, we can spread the data more evenly across partitions. 
# This prevents one partition from receiving too many records and becoming a bottleneck.
#  As a result, work is distributed more evenly, and overall job performance improves.

#Q2
#Custom partitioning lets you control where keys go, which can improve load balancing if the key distribution matches your partition logic.
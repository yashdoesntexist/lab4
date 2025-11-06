from pyspark import SparkContext

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

#Q2
#Custom partitioning lets you control where keys go, which can improve load balancing if the key distribution matches your partition logic.
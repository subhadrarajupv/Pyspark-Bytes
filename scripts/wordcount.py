from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[*]", "wordcount")
sc.setLogLevel("ERROR")

ip = sc.textFile("./datasets/search_data-201008-180523.txt")

words = ip.flatMap(lambda x: x.split(" "))

# With CountByValue - It is an action like Collect and executes immediately
# word_counts = words.map(lambda x: x.lower())  # We don't need a pair for CountByValue
# results = word_counts.countByValue()

# word_counts = words.map(lambda x: (x.lower(), 1))
# With SortByKey
# final_count = word_counts.reduceByKey(lambda x, y: x+y).map(lambda x: (x[1], x[0]))
# results = final_count.sortByKey(False).map(lambda x: (x[1], x[0])).collect()

word_counts = words.map(lambda x: (x.lower(), 1))
# With SortBy
final_count = word_counts.reduceByKey(lambda x, y: x+y)
results = final_count.sortBy(lambda x: x[1], False).collect()
# results = final_count.sortBy(lambda x: x[1], False).take(20) # To take only top 20 items

for result in results:
    print(result)


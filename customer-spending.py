from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf = conf)

def parseLine(line):
  fields = line.split(',')
  customerID = int(fields[0])
  spending = float(fields[2])
  return (customerID, spending)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)

customerSpending = rdd.reduceByKey(lambda x, y: x + y)
results = customerSpending.sortByKey(ascending=True).collect()

for result in results:
  print(f"{result[0]} : {result[1]:.2f}")
  
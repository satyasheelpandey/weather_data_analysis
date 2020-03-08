from pyspark import SparkConf, SparkContext

sc = SparkContext(master="local", appName='Demo')

print(sc.textFile("D:\\Workspace\\weather_analysis\\weather_files\\weather_1979.csv").first())
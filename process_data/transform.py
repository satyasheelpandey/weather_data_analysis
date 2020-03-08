from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from process_data.schema import schema


if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.driver.memory", "5g") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .appName("reading csv") \
        .getOrCreate()


    data_file = "D:\\Workspace\\weather_analysis\\weather_files\\weather_1989.csv"
    sdfData = scSpark.read.csv(data_file,header=False, sep=",", comment="#",schema=schema).cache()
    # sdfData = scSpark.read.csv(data_file, header=False, sep=",", comment="#",inferSchema=True).cache()
    # print(sdfData.printSchema())
    print('Total Records = {}'.format(sdfData.count()))
    sdfData.show()

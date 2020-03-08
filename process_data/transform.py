from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from process_data.schema import schema
from pyspark.sql import functions

if __name__ == '__main__':
    # setting up spark context
    scSpark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.driver.memory", "5g") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .appName("reading csv") \
        .getOrCreate()
    # reading the csv files ignoring the spaces and specifiying the date format
    data_file = "D:\\Workspace\\weather_analysis\\weather_files\\weather*.csv"
    dataframe = scSpark.read.csv(data_file, header=False, sep=",", comment="#", schema=schema,
                                 ignoreLeadingWhiteSpace=True, dateFormat='yyyyMMdd').cache()

    # sdfData = scSpark.read.csv(data_file, header=False, sep=",", comment="#", inferSchema=True).cache()
    # sfData_with_datatype = sdfData.withColumn('STN', sdfData['STN'].cast('int'))
    # print(dataframe.printSchema())
    # print('Total Records = {}'.format(dataframe.count()))
    # substituting null values with zeros
    # dataframe.explain()
    # dataframe.na.fill(0).show()
    dataframe.registerTempTable("weather_data")
    output=scSpark.sql('select distinct STN from weather_data')
    output.show()



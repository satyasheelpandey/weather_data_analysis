from process_data.schema import schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, format_number, when

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
    data_file = "../weather_files/weather*.csv"
    dataframe = scSpark.read.csv(data_file, header=False, sep=",", comment="#", schema=schema,
                                 ignoreLeadingWhiteSpace=True, dateFormat='yyyyMMdd').cache()

    # adding the column for year so that the aggregation can be done for each year
    # removing -1 from precipitation column to balance the skewness
    dataframe_with_date = dataframe.withColumn("YEAR", year(dataframe["DATE"])) \
        .withColumn("RH", when(dataframe["RH"] == -1, 0).otherwise(dataframe["RH"]))

    # aggregating the data based on station and year
    avg_years = dataframe_with_date \
        .groupBy("STN", "YEAR") \
        .avg("TG", "RH", "Q") \
        .orderBy("STN", "YEAR")

    # calculating average of the temp, rainfall and sunshine
    average_years_decimals = avg_years \
        .withColumn("AVG_TEMP (C)", format_number((avg_years["avg(TG)"] / 10), 2)) \
        .withColumn("AVG_PRECIPITATION (MM)", format_number((avg_years["avg(RH)"]), 2)) \
        .withColumn("AVG_SUNSHINE", format_number((avg_years["avg(Q)"]), 2)) \
        .drop("avg(TG)", "avg(RH)", "avg(Q)")

    # writing  data into a json file to be consumed further
    average_years_decimals.coalesce(1).write.format('json').save('../output_files/average_years_decimals',
                                                                 mode='overwrite')

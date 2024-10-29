package edu.ucr.cs.cs167.kwu116

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.beast.SparkSQLRegistration
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

/**
 * Scala example for Task 2 in Beast
 */
object BeastScalaSpatialAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val conf = new SparkConf().setAppName("Task 2 Example").setIfMissing("spark.master", "local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // Register Beast and GeoSpark functions
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(spark)
    GeoSparkSQLRegistrator.registerAll(spark)

    try {
      // Load the crime data Parquet file
      val crimeData = spark.read.parquet("Chicago_Crimes_ZIP.parquet")

      // Perform aggregation to find the number of crimes per ZIP code
      val crimesByZip = crimeData.groupBy("ZIPCode").count()

      // Load the ZIP Code dataset
      val zipCodeData = spark.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .option("header", "true")
        .load("path_to_your_zipcode_dataset/ZipCodes.csv")

      // Rename the ZCTA5CE10 column to ZIPCode to make the join condition straightforward
      val zipCodeDataRenamed = zipCodeData.withColumnRenamed("ZCTA5CE10", "ZIPCode")

      // Join crimesByZip with zipCodeData on ZIPCode to add the geometry
      val joinedData = crimesByZip.join(zipCodeDataRenamed, "ZIPCode")

      // Coalesce to ensure a single file output and save as a shapefile
      joinedData.coalesce(1)
        .write
        .format("com.databricks.spark.shape")
        .option("path", "ZIPCodeCrimeCount")
        .save()

    } finally {
      spark.stop()
    }
  }
}
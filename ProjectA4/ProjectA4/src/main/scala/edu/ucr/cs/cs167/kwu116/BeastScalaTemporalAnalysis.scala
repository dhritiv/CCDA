package edu.ucr.cs.cs167.kwu116

import org.apache.spark.SparkConf
import org.apache.spark.beast.{CRSServer, SparkSQLRegistration}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
 * Scala examples for Beast
 */
object BeastScalaTemporalAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    // Set the logging level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    // Start the CRSServer and store the information in SparkConf
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    CRSServer.startServer(sparkContext)
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    // Command line arguments
    val inputFile: String = args(0)
    val start_date: String = args(1)
    val end_date: String = args(2)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._

      // Open input parquet file
      if (!inputFile.endsWith(".parquet")) {
        Console.err.println(s"Unexpected input format. Expected file name to end with '.parquet'.")
      }
      var df_3 = sparkSession.read.parquet("Chicago_Crimes_ZIP.parquet")
      df_3.createOrReplaceTempView("crime_time")

      // DEBUG
      // df_3.printSchema()
      // df_3.show()

      /*
      Expected schema:
      root
       |-- x: double (nullable = true)
       |-- y: double (nullable = true)
       |-- ID: integer (nullable = true)
       |-- CaseNumber: string (nullable = true)
       |-- Date: string (nullable = true)
       |-- Block: string (nullable = true)
       |-- IUCR: string (nullable = true)
       |-- PrimaryType: string (nullable = true)
       |-- Description: string (nullable = true)
       |-- LocationDescription: string (nullable = true)
       |-- Arrest: string (nullable = true)
       |-- Domestic: string (nullable = true)
       |-- Beat: string (nullable = true)
       |-- District: string (nullable = true)
       |-- Ward: string (nullable = true)
       |-- CommunityArea: integer (nullable = true)
       |-- FBICode: string (nullable = true)
       |-- XCoordinate: integer (nullable = true)
       |-- YCoordinate: string (nullable = true)
       |-- Year: string (nullable = true)
       |-- UpdatedOn: string (nullable = true)
       |-- ZIPCode: string (nullable = true)
       */

      // Count records between start_date and end_date, grouped by PrimaryType
      var query = s"""
      SELECT PrimaryType, COUNT(*) AS Count
      FROM crime_time
      WHERE to_timestamp(Date, 'MM/dd/yyyy hh:mm:ss a') BETWEEN to_date('$start_date', 'MM/dd/yyyy') AND to_date('$end_date', 'MM/dd/yyyy')
      GROUP BY PrimaryType
      """

      // DEBUG
      // print(query)

      var result_df_3 = sparkSession.sql(query)

      // DEBUG
      result_df_3.show()

      // Write result to csv
      result_df_3 = result_df_3.coalesce(1)
      val csvPath = "CrimeTypeCount.csv"
      result_df_3.write
        .format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(csvPath)

    } finally {
      sparkSession.stop()
    }
  }
}
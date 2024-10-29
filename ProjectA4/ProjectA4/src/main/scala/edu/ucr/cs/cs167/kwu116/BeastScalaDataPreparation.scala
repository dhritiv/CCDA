package edu.ucr.cs.cs167.kwu116

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BeastScalaDataPreparation {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val inputfile: String = args(0)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()

      // Parse and load the CSV file using the DataFrame API
      val crimesDF: DataFrame = sparkSession.read.format("csv")
        .option("sep", "\t")
        .option("inferSchema", true)
        .option("header", true)
        .load(inputfile)

      // Introduce a geometry attribute using ST_CreatePoint function
      val crimesWithGeometryDF: DataFrame = crimesDF
        .withColumn("geometry", expr(s"ST_CreatePoint('x', 'y')"))

      // Rename attributes with spaces
      val renamedDF: DataFrame = crimesWithGeometryDF
        .withColumnRenamed("Case Number", "CaseNumber")
        .withColumnRenamed("Primary Type", "PrimaryType")
        .withColumnRenamed("Location Description", "LocationDescription")
        .withColumnRenamed("Community Area", "CommunityArea")
        .withColumnRenamed("FBI Code", "FBICode")
        .withColumnRenamed("X Coordinate", "XCoordinate")
        .withColumnRenamed("Y Coordinate", "YCoordinate")
        .withColumnRenamed("Updated On", "UpdatedOn")
        .withColumnRenamed("ZIP Code", "ZIPCode")

      // Convert DataFrame to SpatialRDD
      val spatialRDD: SpatialRDD = renamedDF.toSpatialRDD

      // Load ZIP Code dataset using Beast
      val zipCodeRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")

      // Spatial join to find ZIP code of each crime
      val joinedRDD: RDD[(IFeature, IFeature)] = zipCodeRDD.spatialJoin(spatialRDD)

      // Use the attribute ZCTA5CE10 from the ZIP code to introduce a new attribute ZIPCode in the crime
      val crimesWithZIPCodeDF: DataFrame = joinedRDD
        .map { case (crime, zipCode) =>
          val ZIPCODE = zipCode.getAs[String]("ZCTA5CE10")
          Feature.append(crime, ZIPCODE, "ZIPCode")}.toDataFrame(sparkSession)

      // Convert the result back to a Dataframe
      val convertedDF: DataFrame = crimesWithZIPCodeDF.toDF

      // Drop unnecessary columns
      val cleanedDF = crimesWithZIPCodeDF.drop("geometry")

      // Write output as Parquet file
      cleanedDF.write.parquet("Chicago_Crimes_ZIP")

      val t2 = System.nanoTime()
      println(s"Applied analysis algorithm on input $inputfile in ${(t2 - t1) * 1E-9} seconds")
    } finally {
      sparkSession.stop()
    }
  }
}
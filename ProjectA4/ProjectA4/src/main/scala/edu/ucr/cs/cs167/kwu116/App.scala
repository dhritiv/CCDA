package edu.ucr.cs.cs167.kwu116

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object App {

  def main(args : Array[String]) {
    if (args.length != 1) {
      println("Usage <input file>")
      println("  - <input file> path to a CSV file input")
      sys.exit(0)
    }
    val inputfile = args(0)
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("Arrest Prediction")
      .config(conf)
      .getOrCreate()

    val t1 = System.nanoTime
    try {
      import spark.implicits._
      // Read Parquet file as a DataFrame
      val ArrestDF: DataFrame = spark.read.parquet(inputfile)
        .filter("Arrest == 'true' OR Arrest == 'false'")

      val ArrestDFCombinedColumn = ArrestDF.withColumn("PrimaryType_Description", concat($"PrimaryType", lit(" "), $"Description"))

      ArrestDF.printSchema()
      ArrestDF.show()

      // Tokenzier that finds all the tokens (words) from the primary type and description
      val tokenizer = new Tokenizer()
        .setInputCol("PrimaryType_Description")
        .setOutputCol("tokens")

      // HashingTF transformer that converts the tokens into a set of numeric features
      val hashingTF = new HashingTF()
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")

      // StringIndexer that converts each arrest value to an index
      val stringIndexer = new StringIndexer()
        .setInputCol("Arrest")
        .setOutputCol("label")
        .setHandleInvalid("skip")
        .setStringOrderType("alphabetDesc")

      // Logistic Regression or another classifier that predicts the arrest value from the set of features
      val svc = new LinearSVC()
        .setLabelCol("label")
        .setFeaturesCol("features")

      // Pipeline
      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, hashingTF, stringIndexer, svc))

      // Parameter grid that cross validates the model on different hyper parameters
      val paramGrid: Array[ParamMap] = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures, Array(1024, 2048))
        .addGrid(svc.fitIntercept, Array(true, false))
        .addGrid(svc.regParam, Array(0.01, 0.0001))
        .addGrid(svc.maxIter, Array(10, 15))
        .addGrid(svc.threshold, Array(0, 0.25))
        .build()

      // Cross Validation job that processes the pipeline using all possible combinations in the parameter grid
      val cv = new TrainValidationSplit()
        .setEstimator(pipeline)
        .setEvaluator(new BinaryClassificationEvaluator())
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(0.8)
        .setParallelism(2)

      // Split data into 80% train and 20% test
      val Array(trainingData: Dataset[Row], testData: Dataset[Row]) = ArrestDFCombinedColumn.randomSplit(Array(0.8, 0.2))

      // Run cross-validation and choose the best set of parameters.
      val model: TrainValidationSplitModel = cv.fit(trainingData)

      // Get and print the parameters of the best model
      val numFeatures: Int = model.bestModel.asInstanceOf[PipelineModel].stages(1).asInstanceOf[HashingTF].getNumFeatures
      val fitIntercept: Boolean = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getFitIntercept
      val regParam: Double = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getRegParam
      val maxIter: Double = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getMaxIter
      val threshold: Double = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getThreshold
      val tol: Double = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getTol

      println(s"numFeatures: $numFeatures")
      println(s"fitIntercept: $fitIntercept")
      println(s"regParam: $regParam")
      println(s"maxIter: $maxIter")
      println(s"threshold: $threshold")
      println(s"tol: $tol")

      // Apply the model to the test set
      val predictions: DataFrame = model.transform(testData)
      predictions.select("PrimaryType", "Description", "Arrest", "label", "prediction").show()

      // Evaluate the test results
      val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("prediction")

      val accuracy: Double = binaryClassificationEvaluator.evaluate(predictions)
      println(s"Accuracy of the test set is $accuracy")

      val t2 = System.nanoTime
      println(s"Applied analysis algorithm on input $inputfile in ${(t2 - t1) * 1E-9} seconds")
    } finally {
      spark.stop
    }
  }
}
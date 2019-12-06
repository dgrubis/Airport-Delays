package project.SparkClassification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

object DTmodelMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nDTmodelMain <input dir> <output dir>")
      System.exit(1)
    }
    
    val conf = new SparkConf()
    val sparkSession = SparkSession.builder 
                       .appName("DecisionTreeModel")
                       .config(conf)
                       .getOrCreate() 
    
    val df = sparkSession.read.option("inferSchema","true")
                         .csv(args(0))
                         .toDF("DATE", "ORIGIN", "ORIGIN_LAT", "ORIGIN_LONG",	
                               "DEST", "DEST_LAT", "DEST_LON", "AIRLINE",	
                               "FLIGHT_NUMBER", "TAIL_NUMBER", "DISTANCE",
                               "WEATHER_CANCELLATION", "WEATHER_DELAY",
                               "WEATHER_DELAY_MINUTES", "DATE_O", "LATITUDE_O",
                               "LONGITUDE_O", "TEMP_O", "DEWP_O", "SLP_O", "STP_O", "VISIB_O",
                               "WDSP_O", "MXSPD_O", "GUST_O", "MAX_O", "MIN_O", "PRCP_O", "SNDP_O",
                               "FRSHTT_O", "DATE_D", "LATITUDE_D", "LONGITUDE_D", "TEMP_D",
                               "DEWP_D", "SLP_D", "STP_D", "VISIB_D",	"WDSP_D", "MXSPD_D", "GUST_D",
                               "MAX_D", "MIN_D", "PRCP_D", "SNDP_D", "FRSHTT_D") //read in the joined data as a dataframe
                                                                                
                           
    val myFeatures = Array("ORIGIN_LAT", "ORIGIN_LONG",
                         "DEST_LAT", "DEST_LON", "AIRLINE",
                         "TAIL_NUMBER",	"DISTANCE",
                         //Origin
                         "TEMP_O", "DEWP_O", "SLP_O", "STP_O", "VISIB_O",
                         "WDSP_O", "MXSPD_O", "GUST_O",	"MAX_O", "MIN_O",	"PRCP_O",
                         "SNDP_O", "FRSHTT_O",
                         //Destination
                         "TEMP_D", "DEWP_D", "SLP_D", "STP_D", "VISIB_D",
                         "WDSP_D", "MXSPD_D", "GUST_D",	"MAX_D", "MIN_D",	"PRCP_D",
                         "SNDP_D", "FRSHTT_D")
     
    val FeatureIndexer = new VectorAssembler()
                             .setInputCols(myFeatures)
                             .setOutputCol("features") //sets the feature vector as one column consisting of all specified features                   
     
    val modelData = FeatureIndexer.transform(df) //add the feature vector to the dataframe
    
    val Array(trainingData, testingData) = modelData.randomSplit(Array(0.75, 0.25)) //split data into training and testing
    
    val dtModel = new DecisionTreeClassifier()
                      .setFeaturesCol("features")
                      .setLabelCol("WEATHER_DELAY")
                      .setImpurity("entropy")
                      .setMaxDepth(10)
                      .fit(trainingData) //fits a decision tree classifier on the training data with set hyper-parameters
                     
    val predictionData = dtModel.transform(testingData) //append the predictions and probabilities to the dataframe
    
    val auc = new BinaryClassificationEvaluator()
                       .setLabelCol("WEATHER_DELAY")
                       .setMetricName("areaUnderROC") //measure accuracy of model with AUC
                       //only supported metric? 
                       .evaluate(predictionData)
                     
    println("AUC of Decision Tree Model is " + auc)
    
    //TODO: look at cross-validation methods to find optimal hyper-parameters
                       
  }
}
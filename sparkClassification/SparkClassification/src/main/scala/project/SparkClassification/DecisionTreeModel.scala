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
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

object DTmodelMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nRS_DMain <input dir> <output dir>")
      System.exit(1)
    }
    
    val conf = new SparkConf()
    val sparkSession = SparkSession.builder 
                       .appName("DecisionTreeModel")
                       .config(conf)
                       .getOrCreate() 
    
    val df = sparkSession.read.option("inferSchema","true").csv(args(0)).toDF() //read in the joined data as a dataframe
                                                                                //may need to name columns manually
    
    //val LabelIndexer = new StringIndexer()
                           //.setInputCol("WEATHER_DELAY")
                           //.setOutputCol("label")
                           //.fit(df)
    //WEATHER_DELAY is already a 0 or 1 flag
                           
    val myFeatures = Array("ORIGIN_LAT", "ORIGIN_LONG",
                         "DEST_LAT", "DEST_LON", "AIRLINE",
                         "TAIL_NUMBER",	"DISTANCE",
                         //Origin
                         "TEMP", "DEWP", "SLP", "STP", "VISIB",
                         "WDSP", "MXSPD", "GUST",	"MAX", "MIN",	"PRCP",
                         "SNDP", "FRSHTT",
                         //Destination
                         "TEMP", "DEWP", "SLP", "STP", "VISIB",
                         "WDSP", "MXSPD", "GUST",	"MAX", "MIN",	"PRCP",
                         "SNDP", "FRSHTT")
     
    val FeatureIndexer = new VectorAssembler()
                             .setInputCols(myFeatures)
                             .setOutputCol("features") //sets the feature vector as one column consisting of all specified features                   
     
                             
  }
}
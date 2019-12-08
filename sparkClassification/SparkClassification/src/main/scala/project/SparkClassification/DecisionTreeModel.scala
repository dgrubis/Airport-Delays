package project.SparkClassification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}



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
                               "FRSHTT_1_O","FRSHTT_2_O", "FRSHTT_3_O", "FRSHTT_4_O", "FRSHTT_5_O", "FRSHTT_6_O",
                               "NULL1", "DATE_D", "LATITUDE_D", "LONGITUDE_D", "TEMP_D",
                               "DEWP_D", "SLP_D", "STP_D", "VISIB_D",	"WDSP_D", "MXSPD_D", "GUST_D",
                               "MAX_D", "MIN_D", "PRCP_D", "SNDP_D", 
                               "FRSHTT_1_D","FRSHTT_2_D", "FRSHTT_3_D", "FRSHTT_4_D", "FRSHTT_5_D", "FRSHTT_6_D", "NULL2") //read in the joined data as a dataframe
                                                                                
                           
    val myFeatures = Array(
                         //Origin
                         "TEMP_O", "DEWP_O", "VISIB_O",
                         "WDSP_O",	"MAX_O", "MIN_O",
                         "SNDP_O", "FRSHTT_1_O","FRSHTT_2_O", "FRSHTT_3_O", "FRSHTT_4_O", "FRSHTT_5_O", "FRSHTT_6_O",
                         //Destination
                         "TEMP_D", "DEWP_D", "VISIB_D",
                         "WDSP_D",	"MAX_D", "MIN_D",
                         "SNDP_D", "FRSHTT_1_D","FRSHTT_2_D", "FRSHTT_3_D", "FRSHTT_4_D", "FRSHTT_5_D", "FRSHTT_6_D")
     
    val FeatureIndexer = new VectorAssembler()
                             .setInputCols(myFeatures)
                             .setOutputCol("features") //sets the feature vector as one column consisting of all specified features
    
    val LabelIndexer = new StringIndexer()
                           .setInputCol("WEATHER_DELAY")
                           .setOutputCol("label")    
                           //need to send WEATHER_DELAY through a string indexer to build a pipeline                         
    
    val Array(trainingData, testingData) = df.randomSplit(Array(0.75, 0.25)) //split data into training and testing
    
    val dtClassifier = new DecisionTreeClassifier()
                      .setFeaturesCol("features")
                      .setLabelCol("label")
                      .setImpurity("gini")
                      .setMaxDepth(5)
                      //creates a decision tree classifier on the training data with set hyper-parameters
          
    val pipeline = new Pipeline().setStages(Array(FeatureIndexer, LabelIndexer, dtClassifier)) //creates pipeline to chain the indexers (features, label) and classifier together              
   
    val dtModel = pipeline.fit(trainingData) //fit the model on the training data
                 
    val predictionData = dtModel.transform(testingData) //append the predictions and probabilities to the dataframe
    
    val metricEvaluator = new BinaryClassificationEvaluator()
                       .setLabelCol("label")
                       .setMetricName("areaUnderROC") //measure accuracy of model with AUC
                       //only supported metric? 
    
    val auc = metricEvaluator.evaluate(predictionData)
                     
    println("AUC of Decision Tree Model is " + auc)
    
    //Uses cross-validation to find optimal hyper-parameters
    //*Expensive* Compare to above with finding optimal hyperparameters vs. time complexity in parallel
    
                                  
    val gridSearch = new ParamGridBuilder()
                        .addGrid(dtClassifier.maxDepth, Array(5, 8, 12))
                        .addGrid(dtClassifier.maxBins, Array(20, 25, 32))
                        .addGrid(dtClassifier.impurity, Array("entropy", "gini"))
                        .build()
                        //specifies the parameters to be optimized in the model by building a grid (here is a 3x3x2) search grid
                     
    val cv = new CrossValidator()
                 .setEstimator(pipeline)
                 .setEvaluator(metricEvaluator)
                 .setEstimatorParamMaps(gridSearch)
                 .setNumFolds(5)
                 .setParallelism(5) //adds parallelism to training to achieve greater speedup
                 //creates cross validator object that will fit models as it searches for the optimal paramters based on the evaluator we specified 
    
    val CVmodel = cv.fit(trainingData) //fit this new model
    
    val cvPredictionData = CVmodel.transform(testingData)
    
    val cvAUC = metricEvaluator.evaluate(cvPredictionData)
    
    println("AUC of Optimized Decision Tree Model is " + cvAUC)
    
    
  }
}
package project.SparkClassification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

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
    
    
  }
}
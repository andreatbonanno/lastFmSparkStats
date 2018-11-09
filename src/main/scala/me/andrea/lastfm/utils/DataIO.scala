package me.andrea.lastfm.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}

object DataIO {

  def loadDf(resourceName: String, sep: String, colNames: String*)
            (implicit sqlContext: SQLContext): DataFrame = {
    sqlContext
      .read
      .option("delimiter", sep)
      .csv(getClass.getResource(resourceName).getPath)
      .toDF(colNames:_*)
  }

  def loadRdd(resourceName: String, sep: String = "\t")
            (implicit sparkContext: SparkContext): RDD[Vector[String]] = {
    sparkContext
      .textFile(getClass.getResource(resourceName).getPath)
      .map(line => line.split(sep).toVector)
  }

  def writeDf(df: Dataset[_ <: Serializable], outputFile: String, sep: String = ","): Unit = {
    df.coalesce(1)
      .write
      .format("csv")
      .option("delimiter","\t")
      .save(outputFile)
  }

  def writeRdd(rdd: RDD[_ <: Product], outputFile: String): Unit = {
    rdd.saveAsTextFile(outputFile)
  }

}

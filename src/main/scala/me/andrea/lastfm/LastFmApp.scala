package me.andrea.lastfm

import me.andrea.lastfm.services.df.DfStatsService
import me.andrea.lastfm.services.rdd.RddStatsService
import me.andrea.lastfm.utils.{ConfigLoader, DataIO}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}


import scala.util.Try

object LastFmApp {

  def main(args: Array[String]): Unit = {

    args match {
      case Array(dataStruct, questionNum)
        if dataStruct.matches("(RDD|DF)") && Try(questionNum.toInt).map(q => 1 to 3 contains q).getOrElse(false) =>

        val spark =
          SparkSession
            .builder()
            .appName("LastFmStats")
            .config("spark.master", ConfigLoader.sparkMaster)
            .config("spark.driver.host", ConfigLoader.sparkDriverHost)
            .config("spark.executor.memory", ConfigLoader.sparkExecutorMemory)
            .config("spark.driver.memory", ConfigLoader.sparkDriverMemory)
            .config("spark.hadoop.validateOutputSpecs", ConfigLoader.checkExistingOutput)
            .getOrCreate()

        dataStruct match {
          case "RDD" => {
            implicit val sc: SparkContext = spark.sparkContext

            val rdd = DataIO.loadRdd(ConfigLoader.dataFile)
            val service = new RddStatsService(rdd)

            val stats: RDD[_ <: Product] = questionNum.toInt match {
              case 1 => service.numSongsByUser
              case 2 => service.popularSongs()
              case _ => service.longestSessionsWithSongs()
            }

            DataIO.writeRdd(stats,s"${ConfigLoader.outputPath}$dataStruct$questionNum.txt")
          }

          case "DF" => {
            implicit val sqlc: SQLContext = spark.sqlContext

            val df = DataIO.loadDf(ConfigLoader.dataFile,"\t", ConfigLoader.sparkDfColNames:_*)
            val service = new DfStatsService(df)

            val resultDF: Dataset[_ <: Serializable] = questionNum.toInt match {
              case 1 => service.numSongsByUser
              case 2 => service.popularSongs()
              case _ => service.longestSessionsWithSongs()
            }

            DataIO.writeDf(resultDF,s"${ConfigLoader.outputPath}$dataStruct$questionNum.csv")
          }
        }

      case _ => println("Please enter a valid data format (RDD or DF) and a valid question number (i.e., 1 to 3)")
    }

  }

}

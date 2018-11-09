package me.andrea.lastfm.utils

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._

object ConfigLoader {

  lazy val conf: Config = ConfigFactory.load()

  lazy val sparkMaster = conf.getString("sparkMaster")
  lazy val sparkDriverHost = conf.getString("sparkDriverHost")
  lazy val sparkExecutorMemory = conf.getString("sparkExecutorMemory")
  lazy val sparkDriverMemory = conf.getString("sparkDriverMemory")
  lazy val sparkDfColNames = conf.getStringList("sparkDfColNames").toList

  lazy val checkExistingOutput = conf.getString("checkExistingOutput")

  lazy val dataFile = conf.getString("dataFile")
  lazy val testDataFile = conf.getString("testDataFile")
  lazy val testDataSampleFile = conf.getString("testDataSampleFile")
  lazy val outputPath = conf.getString("outputPath")

}

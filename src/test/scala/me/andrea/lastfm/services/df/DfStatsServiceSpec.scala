package me.andrea.lastfm.services.df

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase, SharedSparkContext}
import me.andrea.lastfm.utils.{ConfigLoader, DataIO}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class DfStatsServiceSpec extends FunSuite with SharedSparkContext with DataFrameSuiteBase with DatasetSuiteBase {

  import sqlContext.implicits._

  val colNames = ConfigLoader.sparkDfColNames

  test("DfStatsService numSongsByUser should return a DF listing user IDs " +
    "along with the number of distinct songs each user played") {

    val df = DataIO.loadDf(ConfigLoader.testDataFile, "\t", colNames:_*)
    val service = new DfStatsService(df)

    val expectedDF = sc.parallelize(
      Seq(
        ("user_000377",1L),
        ("user_000702",3L),
        ("user_000544",2L),
        ("user_000344",1L),
        ("user_000427",3L),
        ("user_000210",1L),
        ("user_000384",4L),
        ("user_000445",3L),
        ("user_000199",3L),
        ("user_000889",1L),
        ("user_000249",2L),
        ("user_000569",1L),
        ("user_000400",2L),
        ("user_000701",3L),
        ("user_000026",2L),
        ("user_000691",2L),
        ("user_000033",2L),
        ("user_000729",2L),
        ("user_000709",3L),
        ("user_000482",2L)
      )
    ).toDF("userId","count").orderBy("userId")

    val resultDF = service.numSongsByUser.orderBy("userId")

    assertDataFrameEquals(expectedDF, resultDF)
  }

  test("DfStatsService popularSongs should return a DF of the n most popular songs (artist and title) " +
    "in the dataset, with the number of times each was played") {

    val df = DataIO.loadDf(ConfigLoader.testDataFile, "\t", colNames:_*)
    val service = new DfStatsService(df)

    val expectedDF = sc.parallelize(
      Seq(
        ("Sunz Of Man","Champagne Room",4),
        ("Arctic Monkeys","Cigarette Smoker Fiona",2),
        ("Pantera","Fucking Hostile",2)
      )
    ).toDF().as[(String,String,Int)]

    val resultDF = service.popularSongs(3)

    assertDatasetEquals(expectedDF, resultDF)
  }

  test("DfStatsService longestSessionsWithSongs should return the n longest " +
    "sessions (defined in minutes, default to 20)") {

    val df = DataIO.loadDf(ConfigLoader.testDataFile, "\t", colNames:_*)
    val service = new DfStatsService(df)

    val schema = StructType(
      Array(
        StructField("userId",StringType,true),
        StructField("sessionId",StringType,false),
        StructField("sessionDuration",LongType,true),
        StructField("sessionFirstSongTs",StringType,true),
        StructField("sessionLastSongTs",StringType,true),
        StructField("artistName",StringType,true),
        StructField("trackName",StringType,true)
      )
    )

    val expectedDF = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("user_000384","user_000384_2",1105L,"2007-01-08T17:22:35Z","2007-01-08T17:41:00Z","The Hold Steady","Cattle And The Creeping Things"),
          Row("user_000384","user_000384_2",1105L,"2007-01-08T17:22:35Z","2007-01-08T17:41:00Z","Pixies","Velouria"),
          Row("user_000427","user_000427_1",854L,"2006-02-18T17:27:42Z","2006-02-18T17:41:56Z","Capleton","Behold (Feat. Morgan Heritage)"),
          Row("user_000427","user_000427_1",854L,"2006-02-18T17:27:42Z","2006-02-18T17:41:56Z","Mikey Dread","Enjoy Yourself"),
          Row("user_000701","user_000701_1",420L,"2007-05-22T19:35:41Z","2007-05-22T19:42:41Z","Arctic Monkeys","Cigarette Smoker Fiona"),
          Row("user_000701","user_000701_1",420L,"2007-05-22T19:35:41Z","2007-05-22T19:42:41Z","Ratatat","Kennedy"),
          Row("user_000701","user_000701_1",420L,"2007-05-22T19:35:41Z","2007-05-22T19:42:41Z","Gorillaz","Every Planet We Reach Is Dead")
        )
      ),
      schema
    ).orderBy($"sessionDuration".desc, $"trackName")

    val resultDF = service.longestSessionsWithSongs(limit = 3)

    assertDataFrameEquals(expectedDF, resultDF)

  }
}

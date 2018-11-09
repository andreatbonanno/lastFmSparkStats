package me.andrea.lastfm.services.df

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

case class Listening(userId: String,
                     timestamp: String,
                     artistId: String,
                     artistName: String,
                     trackId: String,
                     trackName: String)


class DfStatsService(listeningsDF: DataFrame)(implicit spark: SQLContext) {

  import spark.implicits._

  val tsFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"

  lazy val numSongsByUser: DataFrame =
    listeningsDF
      .groupBy("userId")
      .agg(countDistinct("trackId").as("count"))


  def popularSongs(n: Int = 100) =
    listeningsDF
      .as[Listening]
      .groupByKey(_.trackId)
      .mapGroups { (_, listenings) => listenings.toList }
      .map(listenings => (listenings.head.artistName, listenings.head.trackName, listenings.size))
      .sort($"_3".desc, $"_1")
      .limit(n)


  private def dfWithSession(minutes: Long) = {

    val w = Window.partitionBy("userId").orderBy("timestamp")

    listeningsDF.withColumn("timestamp", to_timestamp($"timestamp", tsFormat).cast(TimestampType))
      .orderBy("timestamp")
      .withColumn("prevTs", lag($"timestamp".cast("long"), 1, 0).over(w))
      .withColumn("diffMins", ($"timestamp".cast("long") - $"prevTs") / 60D)
      .withColumn("isNewSession", ($"diffMins".cast(LongType) > minutes).cast(IntegerType))
      .withColumn("sessionId", concat_ws("_", $"userId", sum("isNewSession").over(w)))

  }

  private def dfWithSessionDuration(minutes: Long = 20, limit: Int = 10) =
    dfWithSession(minutes)
      .groupBy($"sessionId")
      .agg(
        min("timestamp").as("sessionFirstSongTs"),
        max("timestamp").as("sessionLastSongTs")
      )
      .withColumn("sessionDuration", $"sessionLastSongTs".cast(LongType) - $"sessionFirstSongTs".cast(LongType))
      .withColumn("sessionFirstSongTs",date_format($"sessionFirstSongTs", tsFormat))
      .withColumn("sessionLastSongTs",date_format($"sessionLastSongTs", tsFormat))
      .orderBy($"sessionDuration".desc)
      .limit(limit)

  def longestSessionsWithSongs(minutes: Long = 20, limit: Int = 10) =
    dfWithSessionDuration(minutes, limit)
      .join(dfWithSession(minutes), "sessionId")
      .select("userId",
        "sessionId",
        "sessionDuration",
        "sessionFirstSongTs",
        "sessionLastSongTs",
        "artistName",
        "trackName")
      .orderBy($"sessionDuration".desc, $"trackName")
}

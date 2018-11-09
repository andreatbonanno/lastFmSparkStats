package me.andrea.lastfm.services.rdd

import java.time.Instant

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case class SessionStats(firstTs: Long, lastTs: Long, duration: Long)
case class ListeningWithSessionNum(listenings: Seq[String], ts: Long, sessionId: Long)
case class SessionStatsWithSongs(userId: String,
                                 sessionId: String,
                                 sessionDuration: Long,
                                 firstTs: String,
                                 lastTs: String,
                                 tracks: Seq[(String, String)])


class RddStatsService(listeningsRDD: RDD[Vector[String]])
                     (implicit sparkContext: SparkContext) extends Serializable {

  lazy val numSongsByUser: RDD[(String, Int)] =
    listeningsRDD
      .groupBy(_.head)
      .mapValues(l => l.map(_(4)).toSet.size)
      .zipWithIndex
      .keys


  def popularSongs(n: Int = 100): RDD[((String, String), Int)] =
    listeningsRDD
      .groupBy(_(4))
      .map { case (_, rows) => {
          val artist: String = rows.head(3)
          val title: String = rows.head(5)
          (artist, title) -> rows.size
        }
      }
      .sortBy(- _._2)
      .zipWithIndex
      .filter{case (_, idx) => idx < n}
      .keys
      .coalesce(1)(Ordering[Long].on(- _._2))


 def longestSessionsWithSongs(n: Int = 10): RDD[SessionStatsWithSongs] =
   rddWithSessionsStats()
     .sortBy(- _.sessionDuration)
     .zipWithIndex
     .filter{case (_, idx) => idx < n}
     .keys
     .coalesce(1)(Ordering[Long].on(- _.sessionDuration))


  def rddWithSessionsStats(sessionDuration: Long = 20L): RDD[SessionStatsWithSongs] =
    listeningsRDD
      .groupBy(_.head)
      .flatMap { case (_, records) => {
          val recordsWithTs =
            records
              .map { row => ListeningWithSessionNum(row, Instant.parse(row(1)).getEpochSecond, 1L) }
              .toSeq
              .sortBy(_.ts)

          groupSessions(recordsWithTs, duration = sessionDuration)
            .groupBy(_.sessionId)
            .map { case (sessionNum, session) => {
                val sessionStats = sessionFirstLastDuration(session)
                SessionStatsWithSongs(
                  session.head.listenings.head,
                  s"${session.head.listenings.head}_$sessionNum",
                  sessionStats.duration,
                  new DateTime(sessionStats.firstTs*1000).toString,
                  new DateTime(sessionStats.lastTs*1000).toString,
                  session.map(l => (l.listenings(3), l.listenings(5)))
                )
              }
            }
        }
      }


  def groupSessions(listenings: Seq[ListeningWithSessionNum], accum: Seq[ListeningWithSessionNum] = Seq(), duration: Long): Seq[ListeningWithSessionNum] =
    listenings match {
      case head :: next :: tail =>
        groupSessions(
          ListeningWithSessionNum(next.listenings, next.ts, head.sessionId + (if ((next.ts - head.ts) / 60 > duration) 1 else 0)) +: tail,
          accum :+ head,
          duration
        )
      case head :: Nil => accum :+ head
      case _ => accum
    }


  def sessionFirstLastDuration(a: Seq[ListeningWithSessionNum]): SessionStats = {
    SessionStats(a.map(_.ts).min, a.map(_.ts).max, a.map(_.ts).max - a.map(_.ts).min)
  }
}

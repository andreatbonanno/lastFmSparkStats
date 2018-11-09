package me.andrea.lastfm.services.rdd

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import me.andrea.lastfm.utils.{ConfigLoader, DataIO}
import org.scalatest.FunSuite

class RddStatsServiceSpec extends FunSuite with SharedSparkContext with RDDComparisons {

  test("RddStatsService numSongsByUser should return an RDD listing user IDs " +
    "along with the number of distinct songs each user played") {

    val rdd = DataIO.loadRdd(ConfigLoader.testDataFile)(sc)
    val service = new RddStatsService(rdd)(sc)

    val expectedRDD = sc.parallelize(
      Seq(
        ("user_000377",1),
        ("user_000702",3),
        ("user_000544",2),
        ("user_000344",1),
        ("user_000427",3),
        ("user_000210",1),
        ("user_000384",4),
        ("user_000445",3),
        ("user_000199",3),
        ("user_000889",1),
        ("user_000249",2),
        ("user_000569",1),
        ("user_000400",2),
        ("user_000701",3),
        ("user_000026",2),
        ("user_000691",2),
        ("user_000033",2),
        ("user_000729",2),
        ("user_000709",3),
        ("user_000482",2)
      )
    )
    val resultRDD = service.numSongsByUser

    assert(None === compareRDD(expectedRDD, resultRDD))
    assertRDDEquals(expectedRDD, resultRDD)
  }

  test("RddStatsService popularSongs should return a RDD of the n most popular songs (artist and title) " +
    "in the dataset, with the number of times each was played") {

    val rdd = DataIO.loadRdd(ConfigLoader.testDataFile)(sc)
    val service = new RddStatsService(rdd)(sc)

    val expectedRDD = sc.parallelize(
      Seq(
        (("Sunz Of Man", "Champagne Room"), 4),
        (("Pantera", "Fucking Hostile"), 2),
        (("Arctic Monkeys", "Cigarette Smoker Fiona"), 2)
      )
    )

    val resultRDD = service.popularSongs(3)

    assert(None === compareRDD(expectedRDD, resultRDD))
    assertRDDEquals(expectedRDD, resultRDD)
  }

  test("RddStatsService groupSessions should group listenings in sessions " +
    "according to interval length (i.e., duration)") {

    val rdd = DataIO.loadRdd(ConfigLoader.testDataFile)(sc)
    val service = new RddStatsService(rdd)(sc)

    val listenings = Seq(
      ListeningWithSessionNum(Seq(""), 1237623183L, 1L),
      ListeningWithSessionNum(Seq(""), 1237623193L, 1L),
      ListeningWithSessionNum(Seq(""), 1237623283L, 1L),
      ListeningWithSessionNum(Seq(""), 1257623183L, 1L),
      ListeningWithSessionNum(Seq(""), 1267623183L, 1L),
      ListeningWithSessionNum(Seq(""), 1267623184L, 1L))

    val expected = Seq(
      ListeningWithSessionNum(Seq(""), 1237623183L, 1L),
      ListeningWithSessionNum(Seq(""), 1237623193L, 1L),
      ListeningWithSessionNum(Seq(""), 1237623283L, 1L),
      ListeningWithSessionNum(Seq(""), 1257623183L, 2L),
      ListeningWithSessionNum(Seq(""), 1267623183L, 3L),
      ListeningWithSessionNum(Seq(""), 1267623184L, 3L)
    )

    val result = service.groupSessions(listenings, duration = 20L)

    assert(result === expected)

  }

  test("RddStatsService rddWithSessionsStats should return RDD of user sessions along with start time, " +
    "duration and songs played for each session") {

    val rdd = DataIO.loadRdd(ConfigLoader.testDataSampleFile)(sc)
    val service = new RddStatsService(rdd)(sc)

    val expectedRDD = sc.parallelize(
      Seq(
        SessionStatsWithSongs("user_000701","user_000701_1",420,"2007-05-22T20:35:41.000+01:00","2007-05-22T20:42:41.000+01:00",List(("Arctic Monkeys","Cigarette Smoker Fiona"), ("Ratatat","Kennedy"), ("Gorillaz","Every Planet We Reach Is Dead"))),
        SessionStatsWithSongs("user_000427","user_000427_2",0,"2008-05-08T17:05:17.000+01:00","2008-05-08T17:05:17.000+01:00",List(("Sunz Of Man","Champagne Room"))),
        SessionStatsWithSongs("user_000427","user_000427_1",854,"2006-02-18T17:27:42.000Z","2006-02-18T17:41:56.000Z",List(("Capleton","Behold (Feat. Morgan Heritage)"), ("Mikey Dread","Enjoy Yourself"))),
        SessionStatsWithSongs("user_000427","user_000427_3",0,"2008-05-21T02:42:51.000+01:00","2008-05-21T02:42:51.000+01:00",List(("Sunz Of Man","Champagne Room"))),
        SessionStatsWithSongs("user_000384","user_000384_2",1105,"2007-01-08T17:22:35.000Z","2007-01-08T17:41:00.000Z",List(("The Hold Steady","Cattle And The Creeping Things"), ("Pixies","Velouria"))),
        SessionStatsWithSongs("user_000384","user_000384_1",0,"2006-03-22T14:48:38.000Z","2006-03-22T14:48:38.000Z",List(("The Stranglers","No More Heroes"))),
        SessionStatsWithSongs("user_000384","user_000384_3",0,"2008-01-08T17:13:56.000Z","2008-01-08T17:13:56.000Z",List(("Nirvana","Heart-Shaped Box"))),
        SessionStatsWithSongs("user_000889","user_000889_1",0,"2007-02-24T19:15:55.000Z","2007-02-24T19:15:55.000Z",List(("Disturbed","Sons Of Plunder")))
      )
    )

    val resultRDD = service.rddWithSessionsStats()

    assert(None === compareRDD(expectedRDD, resultRDD))
    assertRDDEquals(expectedRDD, resultRDD)
  }

  test("RddStatsService longestSessionsWithSongs should return the n longest" +
    "sessions (defined in minutes, default to 20)") {

    val rdd = DataIO.loadRdd(ConfigLoader.testDataFile)(sc)
    val service = new RddStatsService(rdd)(sc)

    val expectedRDD = sc.parallelize(
      Seq(
        SessionStatsWithSongs("user_000384","user_000384_2",1105,"2007-01-08T17:22:35.000Z","2007-01-08T17:41:00.000Z",List(("The Hold Steady","Cattle And The Creeping Things"), ("Pixies","Velouria"))),
        SessionStatsWithSongs("user_000427","user_000427_1",854,"2006-02-18T17:27:42.000Z","2006-02-18T17:41:56.000Z",List(("Capleton","Behold (Feat. Morgan Heritage)"), ("Mikey Dread","Enjoy Yourself"))),
        SessionStatsWithSongs("user_000701","user_000701_1",420,"2007-05-22T20:35:41.000+01:00","2007-05-22T20:42:41.000+01:00",List(("Arctic Monkeys","Cigarette Smoker Fiona"), ("Ratatat","Kennedy"), ("Gorillaz","Every Planet We Reach Is Dead")))
      )
    )
    val resultRDD = service.longestSessionsWithSongs(n = 3)

    assert(None === compareRDD(expectedRDD, resultRDD))
    assertRDDEquals(expectedRDD, resultRDD)

  }
}

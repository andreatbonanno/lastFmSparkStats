name := "LastFmStats"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "com.typesafe" % "config" % "1.3.3",
  "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.10.0" % "test",
  "org.apache.spark" %% "spark-hive" % "2.3.1" % "test"
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

lazy val root = project.in(file("."))
  .settings(
    name := "bgoctransformer",
    version := "0.1.0-rc1",
    organization := "com.snowplowanalytics",
    scalaVersion := "2.11.11",
    initialCommands := "import com.snowplowanalytics.bgoctransformer._"
  )
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
      // For Snowplow libs
      "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
      "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
      // For user-agent-utils
      "Clojars Maven Repo" at "http://clojars.org/repo",
      // For hadoop-lzo
      "Twitter" at "https://maven.twttr.com/"
    ),
    libraryDependencies ++= Seq(
      Dependencies.spark,
      Dependencies.sparkSql,
      Dependencies.hadoop,
      Dependencies.geoip2,

      Dependencies.scopt,
      Dependencies.commonEnrich,

      Dependencies.specs2,
      Dependencies.scalaCheck
    )
  )
  .settings(BuildSettings.helpersSettings)


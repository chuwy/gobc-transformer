/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
import sbt._

object Dependencies {

  object V {
    // Spark
    val hadoop           = "2.7.3"
    val spark            = "2.2.0"
    // Scala
    val scopt            = "3.6.0"
    val scalaz7          = "7.0.9"
    val json4sJackson    = "3.2.11"
    val analyticsSdk     = "0.2.0"
    val commonEnrich     = "0.31.0"
    val geoip2           = "2.5.0"
    // Scala (test only)
    val specs2           = "2.3.13"
    val scalazSpecs2     = "0.2"
    val scalaCheck       = "1.12.2"
  }

  // Spark
  val hadoop           = "org.apache.hadoop"          % "hadoop-aws"                    % V.hadoop          % "provided"
  val spark            = "org.apache.spark"           %% "spark-core"                   % V.spark           % "provided"
  val sparkSql         = "org.apache.spark"           %% "spark-sql"                    % V.spark           % "provided"

  // Scala
  val scopt            = "com.github.scopt"           %% "scopt"                        % V.scopt
  val json4sJackson    = "org.json4s"                 %% "json4s-jackson"               % V.json4sJackson
  val analyticsSdk     = "com.snowplowanalytics"      %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val commonEnrich     = ("com.snowplowanalytics"    %% "snowplow-common-enrich"   % V.commonEnrich)
    .exclude("com.maxmind.geoip2", "geoip2")
  val geoip2            = "com.maxmind.geoip2"        %  "geoip2"                   % V.geoip2
  // Scala (test only)
  val specs2           = "org.specs2"                 %% "specs2"                    % V.specs2         % "test"
  val scalaCheck       = "org.scalacheck"             %% "scalacheck"                % V.scalaCheck     % "test"
}

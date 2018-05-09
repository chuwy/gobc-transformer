package com.snowplowanalytics.bgoctransformer

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import com.snowplowanalytics.bgoctransformer.Main.AppConfig


object TransformerJob {
  def process(appConfig: AppConfig): Unit = {
    val config = new SparkConf()
      .setAppName("spark-example")
      .setIfMissing("spark.master", "local[*]")

    val sc = new SparkContext(config)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    // Always assuming correct enriched TSV input
    val common = sc
      .textFile(appConfig.input)
      .filter(!_.startsWith("#"))
      .map(Transformer.transform(appConfig.ipLookupsEnrichment))

    val good = common
      .filter(_.isRight)
      .map(_.right.get)
      .map(Row.fromSeq)

    val bad = common
      .filter(_.isLeft)
      .map(_.left.get)

    sqlContext.createDataFrame(good, Transformer.getSchema)
      .write
      .csv(appConfig.output)

    appConfig.outputBad match {
      case Some(path) => bad.saveAsTextFile(path)
      case None => ()
    }
  }
}

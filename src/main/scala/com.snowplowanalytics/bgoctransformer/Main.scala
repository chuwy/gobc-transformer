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

package com.snowplowanalytics.bgoctransformer

import java.util.Base64

import scalaz._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.IpLookupsEnrichment


object Main {

  /**
   * Raw CLI configuration used to extract options from command line
   * Created solely for private `rawCliConfig` value and can contain
   * incorrect state that should be handled by `transform` function
   */
  private case class CliConfig(input: String, output: String, badOutput: Option[String], resolver: String, geoIpEnrichment: String)

  private val resolverConfig = """{"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1","data":{"cacheSize":500,"repositories":[{"name":"Iglu Central","priority": 0,"vendorPrefixes":["com.snowplowanalytics"],"connection":{"http":{"uri": "http://iglucentral.com"}}}]}}"""
  private val defaultResolver = Base64.getEncoder.encodeToString(resolverConfig.getBytes())
  private val enrichmentConfig = """{"schema": "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0","data":{"name":"ip_lookups","vendor": "com.snowplowanalytics.snowplow","enabled": true,"parameters": {"geo": {"database": "GeoLite2-City.mmdb","uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"}}}}"""
  private val defaultEnrichment = Base64.getEncoder.encodeToString(enrichmentConfig.getBytes())

  /**
   * Starting raw value, required by `parser`
   */
  private val rawCliConfig = CliConfig("", "", None, defaultResolver, defaultEnrichment)

  /**
   * End application config parsed from CLI. Unlike `CliConfig`
   */
  case class AppConfig(input: String, output: String, outputBad: Option[String], ipLookupsEnrichment: IpLookupsEnrichment)

  /**
   * Check that raw config contains valid stat
   */
  def transform(raw: CliConfig): Either[String, AppConfig] = {
    val ipEnrichment = for {
      r <- Utils.getResolver(raw.resolver)
      e <- Utils.getIpLookupsEnrichment(r, raw.geoIpEnrichment)
    } yield e

    ipEnrichment match {
      case Failure(errors) => Left(errors.list.mkString(", "))
      case Success(enrichment) => Right(AppConfig(raw.input, raw.output, raw.badOutput, enrichment))
    }
  }

  /**
   * Scopt parser providing necessary argument annotations and basic validation
   */
  private val parser = new scopt.OptionParser[CliConfig](generated.ProjectMetadata.name) {
    head(generated.ProjectMetadata.name, generated.ProjectMetadata.version)

    opt[String]('i', "input").required()
      .action((x, c) => c.copy(input = x))
      .text("Input S3 directory")

    opt[String]('o', "output").required()
      .action((x, c) => c.copy(output = x))
      .text("Output directory")

    opt[String]('o', "badOutput")
      .action((x, c) => c.copy(badOutput = Some(x)))
      .text("Output directory for problematic rows")

    opt[String]("resolver")
      .action((x, c) => c.copy(resolver = x))
      .text("Base64-encoded Iglu Resolver")

    opt[String]("geoIpEnrichmentConfig")
      .action((x, c) => c.copy(geoIpEnrichment = x))
      .text("Base64-encoded IP Lookup Enrichment config")

    help("help").text("prints this usage text")
  }


  def main(args: Array[String]): Unit = {
    parser.parse(args, rawCliConfig).map(transform) match {
      case Some(Right(appConfig)) =>
        TransformerJob.process(appConfig)

      case Some(Left(error)) =>
        // Failed transformation
        println(error)
        sys.exit(1)

      case None =>
        // Invalid arguments
        sys.exit(1)
    }
  }
}

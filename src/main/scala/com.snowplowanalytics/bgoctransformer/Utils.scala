package com.snowplowanalytics.bgoctransformer

import java.util.Base64

import scalaz._
import Scalaz._

import com.fasterxml.jackson.core.JsonParseException

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJValue._
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._

import org.json4s.jackson.JsonMethods.parse

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.IpLookupsEnrichment

object Utils {
  def getResolver(b64Resolver: String) = {
    try {
      val resolverConfig = new String(Base64.getDecoder.decode(b64Resolver))
      val json = parse(resolverConfig)
      Resolver.parse(json)
    } catch {
      case _: IllegalArgumentException => "Cannot decode resolver".toProcessingMessageNel.failure
      case e: JsonParseException => s"Cannot parse resolver JSON ${e.getMessage}".toProcessingMessageNel.failure
    }
  }

  def getIpLookupsEnrichment(resolver: Resolver, b64Config: String) = {
    try {
      val enrichmentConfig = new String(Base64.getDecoder.decode(b64Config))
      for {
        dataPair <- parse(enrichmentConfig).validateAndIdentifySchema(dataOnly = true)(resolver)
        (key, data) = dataPair
        enrichment <- IpLookupsEnrichment.parse(data, key, false)
      } yield enrichment
    } catch {
      case _: IllegalArgumentException => "Cannot decode IP Lookups config".toProcessingMessageNel.failure
      case e: JsonParseException => s"Cannot parse IP Lookups JSON ${e.getMessage}".toProcessingMessageNel.failure
    }
  }
}
